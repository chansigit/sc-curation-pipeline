"""A minimal Dagster Pipes client that runs external work as a Slurm batch job.

Dagster has no built-in Slurm client. This one bridges via **file-based Pipes
channels on the shared filesystem** so the context + messages survive the
``sbatch`` -> compute-node boundary: the orchestration side writes the Pipes
context to a file and reads the external job's messages from another file; the
generated sbatch script exports the Pipes bootstrap env vars and runs the external
Python, which uses ``open_dagster_pipes()``. The client submits via ``sbatch``,
polls ``sacct`` until a terminal state, and raises ``dagster.Failure`` if the job
did not COMPLETE.

The bug-prone bits — sbatch rendering and ``sacct`` state parsing — are pure
functions so they can be unit-tested without a scheduler.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import time
import uuid

import dagster as dg

# Slurm states that mean the job is over.
_OK = {"COMPLETED"}
_NOT_DONE = {"PENDING", "RUNNING", "REQUEUED", "RESIZING", "SUSPENDED", "COMPLETING", "CONFIGURING"}


def render_sbatch_script(
    *, job_name: str, partition: str, gpus: int, time_limit: str, mem: str,
    cpus: int, gpu_constraint: str, env: dict, python: str, script: str, log_path: str,
) -> str:
    """Render an sbatch script: #SBATCH directives + exported Pipes env + the run.

    ``env`` (the Pipes bootstrap vars) is exported INSIDE the script so it reaches
    the job regardless of sbatch env-propagation quirks. ``-G {gpus}`` is always
    emitted (Sherlock rejects GPU jobs without it).
    """
    lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={job_name}",
        f"#SBATCH -p {partition}",
        f"#SBATCH -G {gpus}",
        f"#SBATCH --time={time_limit}",
        f"#SBATCH --mem={mem}",
        f"#SBATCH --cpus-per-task={cpus}",
        f"#SBATCH --output={log_path}",
    ]
    if gpu_constraint:
        lines.append(f"#SBATCH -C {gpu_constraint}")
    lines.append("set -euo pipefail")
    for key, val in env.items():
        lines.append(f"export {key}={shlex.quote(str(val))}")
    lines.append(f"{shlex.quote(python)} {shlex.quote(script)}")
    return "\n".join(lines) + "\n"


def parse_sacct_state(output: str) -> str | None:
    """Primary job state from ``sacct -j <id> --format=State --noheader -P``.

    sacct prints the job then its steps; the first non-empty line is the job. A
    trailing ``+`` (e.g. ``CANCELLED+``) is stripped. Returns None if absent yet.
    """
    for line in output.splitlines():
        tok = line.strip()
        if tok:
            return tok.split()[0].rstrip("+")
    return None


class PipesSlurmClient:
    """Run external code as a Slurm batch job, bridged by file-based Dagster Pipes.

    Not a ConfigurableResource — instantiated from CurationSettings inside the asset.
    """

    def __init__(
        self, *, python: str, script: str, pipes_dir: str, partition: str,
        time_limit: str, mem: str, cpus: int, gpu_constraint: str = "",
        gpus: int = 1, poll_interval_sec: float = 30.0,
    ):
        self.python = python
        self.script = script
        self.pipes_dir = pipes_dir
        self.partition = partition
        self.time_limit = time_limit
        self.mem = mem
        self.cpus = cpus
        self.gpu_constraint = gpu_constraint
        self.gpus = gpus
        self.poll_interval_sec = poll_interval_sec

    def run(self, *, context, extras: dict | None = None) -> dg.PipesClientCompletedInvocation:
        os.makedirs(self.pipes_dir, exist_ok=True)
        tag = f"{context.partition_key or 'run'}-{uuid.uuid4().hex[:8]}"
        ctx_path = os.path.join(self.pipes_dir, f"ctx-{tag}.json")
        msg_path = os.path.join(self.pipes_dir, f"msg-{tag}.jsonl")
        log_path = os.path.join(self.pipes_dir, f"slurm-{tag}.out")
        sbatch_path = os.path.join(self.pipes_dir, f"job-{tag}.sbatch")

        with dg.open_pipes_session(
            context=context,
            context_injector=dg.PipesFileContextInjector(path=ctx_path),
            message_reader=dg.PipesFileMessageReader(path=msg_path),
            extras=extras or {},
        ) as session:
            script_text = render_sbatch_script(
                job_name=f"mrvi-{context.partition_key or 'run'}",
                partition=self.partition, gpus=self.gpus, time_limit=self.time_limit,
                mem=self.mem, cpus=self.cpus, gpu_constraint=self.gpu_constraint,
                env=session.get_bootstrap_env_vars(), python=self.python,
                script=self.script, log_path=log_path,
            )
            with open(sbatch_path, "w") as fh:
                fh.write(script_text)
            job_id = self._submit(sbatch_path)
            context.log.info(f"submitted Slurm job {job_id} on '{self.partition}'; polling sacct")
            self._wait(job_id, context, log_path)
        return dg.PipesClientCompletedInvocation(session)

    def _submit(self, sbatch_path: str) -> str:
        res = subprocess.run(["sbatch", "--parsable", sbatch_path], capture_output=True, text=True)
        if res.returncode != 0:
            raise dg.Failure(
                description=f"sbatch submission failed: {res.stderr.strip()}",
                metadata={"sbatch_stderr": dg.MetadataValue.text(res.stderr[-2000:])},
                allow_retries=False,
            )
        return res.stdout.strip().split(";")[0]  # --parsable -> "<jobid>[;cluster]"

    def _wait(self, job_id: str, context, log_path: str) -> None:
        while True:
            time.sleep(self.poll_interval_sec)
            out = subprocess.run(
                ["sacct", "-j", job_id, "--format=State", "--noheader", "-P"],
                capture_output=True, text=True,
            ).stdout
            state = parse_sacct_state(out)
            if state is None or state in _NOT_DONE:
                continue
            if state in _OK:
                return
            tail = ""
            try:
                with open(log_path) as fh:
                    tail = fh.read()[-2000:]
            except OSError:
                pass
            raise dg.Failure(
                description=f"Slurm job {job_id} ended in state {state}",
                metadata={"job_id": dg.MetadataValue.text(job_id),
                          "state": dg.MetadataValue.text(state),
                          "slurm_log_tail": dg.MetadataValue.text(tail)},
            )

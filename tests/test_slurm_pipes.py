"""Tests for the pure pieces of the Slurm Pipes client (no scheduler needed)."""

from sc_curation_pipeline.defs.slurm_pipes import parse_sacct_state, render_sbatch_script


def test_render_sbatch_script_directives_env_and_run():
    s = render_sbatch_script(
        job_name="mrvi-test2", partition="gpu", gpus=1, time_limit="01:00:00",
        mem="32GB", cpus=4, gpu_constraint="",
        env={"DAGSTER_PIPES_CONTEXT": "/x/ctx.json", "DAGSTER_PIPES_MESSAGES": "/x/msg.jsonl"},
        python="/venv/bin/python", script="/repo/scripts/job.py", log_path="/x/log.out",
    )
    assert s.startswith("#!/bin/bash")
    for directive in ("#SBATCH --job-name=mrvi-test2", "#SBATCH -p gpu", "#SBATCH -G 1",
                      "#SBATCH --time=01:00:00", "#SBATCH --mem=32GB",
                      "#SBATCH --cpus-per-task=4", "#SBATCH --output=/x/log.out"):
        assert directive in s
    assert "export DAGSTER_PIPES_CONTEXT=/x/ctx.json" in s
    assert "export DAGSTER_PIPES_MESSAGES=/x/msg.jsonl" in s
    assert s.rstrip().endswith("/venv/bin/python /repo/scripts/job.py")
    assert "#SBATCH -C" not in s          # no constraint emitted when empty


def test_render_sbatch_script_gpu_constraint():
    s = render_sbatch_script(
        job_name="j", partition="dev", gpus=1, time_limit="00:30:00", mem="16GB",
        cpus=2, gpu_constraint="GPU_MEM:24GB", env={}, python="py", script="s", log_path="l",
    )
    assert "#SBATCH -p dev" in s
    assert "#SBATCH -C GPU_MEM:24GB" in s


def test_parse_sacct_state():
    assert parse_sacct_state("RUNNING\nRUNNING\n") == "RUNNING"
    assert parse_sacct_state("COMPLETED\nCOMPLETED\nCOMPLETED\n") == "COMPLETED"
    assert parse_sacct_state("CANCELLED+\n") == "CANCELLED"      # trailing + stripped
    assert parse_sacct_state("") is None
    assert parse_sacct_state("\n  \n") is None

"""Terminal asset: MrVI (torch) + Leiden clustering, run as a Slurm GPU job via Pipes.

Orchestration only — the heavy GPU training lives in scripts/mrvi_leiden_job.py and
is launched on a GPU node by PipesSlurmClient. The external job rewrites the
*_filtered.h5ad in place (adds obsm["X_mrvi_u"] + obs["mrvi_leiden"]) and reports
its metadata back through Pipes, which becomes this asset's materialization.
"""

import os
import sys

import dagster as dg

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.settings import CurationSettings
from sc_curation_pipeline.defs.qc import resolve_h5ad_path, output_path_for
from sc_curation_pipeline.defs.filter_cells import filtered_path_for
from sc_curation_pipeline.defs.slurm_pipes import PipesSlurmClient

# scripts/mrvi_leiden_job.py at the repo root (src/sc_curation_pipeline/defs/ -> ../../../).
_JOB_SCRIPT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "scripts", "mrvi_leiden_job.py")
)


@dg.asset(
    partitions_def=h5ad_partitions,
    group_name="curation",
    deps=["doublet_scored_h5ad"],
    retry_policy=dg.RetryPolicy(max_retries=2),
)
def mrvi_leiden_h5ad(context: dg.AssetExecutionContext, curation: CurationSettings):
    """Train MrVI + Leiden on the filtered .h5ad via a Slurm GPU job (Dagster Pipes).

    Submits scripts/mrvi_leiden_job.py to Slurm (`-p {mrvi_partition} -G 1 ...`),
    polls until it finishes, and surfaces the job's reported metadata. The external
    job writes obsm["X_mrvi_u"] + obs["mrvi_leiden"] back into the *_filtered.h5ad.
    """
    src = resolve_h5ad_path(context, curation)
    standardized = output_path_for(curation.output_dir, context.partition_key, src)
    filtered = filtered_path_for(standardized)
    if not os.path.isfile(filtered):
        raise dg.Failure(
            description=f"filtered h5ad not found (run initially_filtered_h5ad first): {filtered!r}",
            metadata={"filtered": dg.MetadataValue.path(filtered)},
            allow_retries=False,
        )

    client = PipesSlurmClient(
        python=sys.executable,                 # the dl2025 venv python (shared on scratch)
        script=_JOB_SCRIPT,
        pipes_dir=os.path.join(curation.output_dir, ".pipes"),
        partition=curation.mrvi_partition,
        time_limit=curation.mrvi_time,
        mem=curation.mrvi_mem,
        cpus=curation.mrvi_cpus,
        gpu_constraint=curation.mrvi_gpu_constraint,
    )
    return client.run(
        context=context,
        extras={
            "filtered_path": filtered,
            "leiden_resolution": curation.leiden_resolution,
            "max_epochs": curation.mrvi_max_epochs,
            "n_hvg": curation.mrvi_n_hvg,
        },
    ).get_materialize_result()

import dagster as dg

from sc_curation_pipeline.defs.qc import h5ad_qc, h5ad_qc_job
from sc_curation_pipeline.defs.sensors import watch_h5ad_dir
from sc_curation_pipeline.defs.settings import build_curation_settings


@dg.definitions
def defs() -> dg.Definitions:
    """Bundle the curation asset, job, sensor, and the env-driven resource.

    This is the single place the `curation` resource is registered; a bare
    module-scope resource instance would be silently ignored by the loader.
    load_from_defs_folder discovers this @definitions function and merges it.
    """
    return dg.Definitions(
        assets=[h5ad_qc],
        jobs=[h5ad_qc_job],
        sensors=[watch_h5ad_dir],
        resources={"curation": build_curation_settings()},
    )

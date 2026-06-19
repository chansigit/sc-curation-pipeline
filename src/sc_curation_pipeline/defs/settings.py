import os

import dagster as dg


class CurationSettings(dg.ConfigurableResource):
    """Env-driven configuration for the h5ad QC pipeline."""

    watch_dir: str
    done_marker: str = ".done"
    h5ad_glob: str = "*.h5ad"
    scan_interval_sec: int = 30
    min_cells: int = 100
    max_mito_pct: float = 20.0


def build_curation_settings() -> CurationSettings:
    """Construct CurationSettings from environment variables.

    SC_CURATION_WATCH_DIR is required: read via os.environ so direct attribute
    access returns the resolved string AND a missing value raises KeyError
    loudly (spec §5.1 "missing -> clear error"). Optional fields fall back to
    defaults via os.getenv with explicit casts.
    """
    return CurationSettings(
        watch_dir=os.environ["SC_CURATION_WATCH_DIR"],
        done_marker=os.getenv("SC_CURATION_DONE_MARKER", ".done"),
        h5ad_glob=os.getenv("SC_CURATION_H5AD_GLOB", "*.h5ad"),
        scan_interval_sec=int(os.getenv("SC_CURATION_SCAN_INTERVAL_SEC", "30")),
        min_cells=int(os.getenv("SC_CURATION_MIN_CELLS", "100")),
        max_mito_pct=float(os.getenv("SC_CURATION_MAX_MITO_PCT", "20")),
    )


def sanitize_key(rel_path: str) -> str:
    """Encode a relative POSIX folder path into a Dagster-legal partition key.

    Reversible and injective: the escape char is "_" (literal "_" -> "_U"),
    and the path separator "/" -> "_S". Because every literal underscore is
    escaped before slashes are encoded, distinct paths can never collide
    (e.g. nested "GSE1/sampleB" -> "GSE1_SsampleB" vs a flat folder literally
    named "GSE1__sampleB" -> "GSE1_U_UsampleB").
    """
    return rel_path.strip("/").replace("_", "_U").replace("/", "_S")


def path_for_partition_key(key: str) -> str:
    """Reverse sanitize_key back to a relative POSIX path (decode _S then _U)."""
    return key.replace("_S", "/").replace("_U", "_")


def partition_key_for(watch_dir: str, folder: str) -> str:
    """Partition key for a sample folder relative to the watch dir."""
    rel = os.path.relpath(folder, watch_dir).replace(os.sep, "/")
    return sanitize_key(rel)

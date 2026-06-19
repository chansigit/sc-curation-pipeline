import os

import dagster as dg


def _env_int(name: str, default: int) -> int:
    """Int from env, robust to empty/invalid values (-> default)."""
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    """Float from env, robust to empty/invalid values (-> default)."""
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


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

    SC_CURATION_WATCH_DIR is required and must be a non-empty path: a missing,
    empty, or whitespace-only value raises a clear ValueError at load time
    (spec §5.1 "missing -> clear error"), rather than silently producing a
    sensor that never finds anything. Optional fields fall back to defaults.
    """
    watch_dir = os.environ.get("SC_CURATION_WATCH_DIR", "").strip()
    if not watch_dir:
        raise ValueError(
            "SC_CURATION_WATCH_DIR is required and must be a non-empty path "
            "(set it in the environment or the project .env)."
        )
    return CurationSettings(
        watch_dir=watch_dir,
        done_marker=os.getenv("SC_CURATION_DONE_MARKER", ".done"),
        h5ad_glob=os.getenv("SC_CURATION_H5AD_GLOB", "*.h5ad"),
        scan_interval_sec=_env_int("SC_CURATION_SCAN_INTERVAL_SEC", 30),
        min_cells=_env_int("SC_CURATION_MIN_CELLS", 100),
        max_mito_pct=_env_float("SC_CURATION_MAX_MITO_PCT", 20.0),
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

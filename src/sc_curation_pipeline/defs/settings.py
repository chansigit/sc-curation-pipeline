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


def _env_bool(name: str, default: bool) -> bool:
    """Bool from env: 1/true/yes/on -> True, 0/false/no/off -> False, else default."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    """Float from env, robust to empty/invalid values (-> default)."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


class CurationSettings(dg.ConfigurableResource):
    """Env-driven configuration for the h5ad curation + QC pipeline."""

    watch_dir: str
    output_dir: str
    done_marker: str = ".done"
    h5ad_glob: str = "*.h5ad"
    scan_interval_sec: int = 30
    min_cells: int = 100
    min_genes: int = 5000
    min_genes_per_cell: int = 400
    # Metadata-role identification (stanmetacols). True = LLM-first with heuristic
    # fallback; False = offline heuristic only.
    metacols_use_llm: bool = True
    # LLM backend for metacols (used only when metacols_use_llm=True). provider
    # "openai" targets any OpenAI-compatible endpoint (e.g. Volcengine ARK / Doubao)
    # via metacols_base_url, with the key read from the env var named by
    # metacols_api_key_env. Defaults are stanmetacols' Anthropic defaults; override
    # in .env to use ARK (provider=openai, base_url=ark…, model=doubao…, key=ARK_API_KEY).
    metacols_provider: str = "anthropic"
    metacols_model: str = "claude-opus-4-8"
    metacols_base_url: str = ""        # empty -> backend default
    metacols_api_key_env: str = ""     # env var name holding the key (empty -> SDK default)
    # MrVI + Leiden clustering (mrvi_leiden_h5ad), trained on a Slurm GPU job via
    # Dagster Pipes. sbatch resources are env-configurable; partition accepts "dev"
    # (small/fast-queue) or "gpu" (large). The client always adds -G 1.
    mrvi_partition: str = "gpu"
    mrvi_time: str = "01:00:00"        # short -> faster scheduling; bump for big data
    mrvi_cpus: int = 4
    mrvi_mem: str = "32GB"
    mrvi_gpu_constraint: str = ""      # optional sbatch -C (e.g. "GPU_MEM:24GB"); empty -> none
    mrvi_max_epochs: int = 0           # 0 -> let scvi pick its default
    leiden_resolution: float = 1.0


def build_curation_settings() -> CurationSettings:
    """Construct CurationSettings from environment variables.

    SC_CURATION_WATCH_DIR is required and must be a non-empty path: a missing,
    empty, or whitespace-only value raises a clear ValueError at load time
    (spec §5.1 "missing -> clear error"), rather than silently producing a
    sensor that never finds anything. SC_CURATION_OUTPUT_DIR is also required
    (the standardized .h5ad output directory). Optional fields fall back to
    defaults.
    """
    watch_dir = os.environ.get("SC_CURATION_WATCH_DIR", "").strip()
    if not watch_dir:
        raise ValueError(
            "SC_CURATION_WATCH_DIR is required and must be a non-empty path "
            "(set it in the environment or the project .env)."
        )
    output_dir = os.environ.get("SC_CURATION_OUTPUT_DIR", "").strip()
    if not output_dir:
        raise ValueError(
            "SC_CURATION_OUTPUT_DIR is required and must be a non-empty path "
            "(the standardized .h5ad output directory; never the source dir)."
        )
    return CurationSettings(
        watch_dir=watch_dir,
        output_dir=output_dir,
        done_marker=os.getenv("SC_CURATION_DONE_MARKER", ".done"),
        h5ad_glob=os.getenv("SC_CURATION_H5AD_GLOB", "*.h5ad"),
        scan_interval_sec=_env_int("SC_CURATION_SCAN_INTERVAL_SEC", 30),
        min_cells=_env_int("SC_CURATION_MIN_CELLS", 100),
        min_genes=_env_int("SC_CURATION_MIN_GENES", 5000),
        min_genes_per_cell=_env_int("SC_CURATION_MIN_GENES_PER_CELL", 400),
        metacols_use_llm=_env_bool("SC_CURATION_METACOLS_USE_LLM", True),
        metacols_provider=os.getenv("SC_CURATION_METACOLS_PROVIDER", "anthropic"),
        metacols_model=os.getenv("SC_CURATION_METACOLS_MODEL", "claude-opus-4-8"),
        metacols_base_url=os.getenv("SC_CURATION_METACOLS_BASE_URL", ""),
        metacols_api_key_env=os.getenv("SC_CURATION_METACOLS_API_KEY_ENV", ""),
        mrvi_partition=os.getenv("SC_CURATION_MRVI_PARTITION", "gpu"),
        mrvi_time=os.getenv("SC_CURATION_MRVI_TIME", "01:00:00"),
        mrvi_cpus=_env_int("SC_CURATION_MRVI_CPUS", 4),
        mrvi_mem=os.getenv("SC_CURATION_MRVI_MEM", "32GB"),
        mrvi_gpu_constraint=os.getenv("SC_CURATION_MRVI_GPU_CONSTRAINT", ""),
        mrvi_max_epochs=_env_int("SC_CURATION_MRVI_MAX_EPOCHS", 0),
        leiden_resolution=_env_float("SC_CURATION_LEIDEN_RESOLUTION", 1.0),
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

import glob
import os

import dagster as dg

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.qc import (
    H5AD_PATH_TAG,
    SPECIES_MARKER_PREFIX,
    SPECIES_TAG,
    h5ad_qc_job,
)
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for

# Fallback tick interval used when SC_CURATION_SCAN_INTERVAL_SEC is unset or invalid.
# The decorator's minimum_interval_seconds is read from the env at import time via
# _interval_seconds(); the resource's scan_interval_sec field is purely informational.
_DEFAULT_INTERVAL_SEC = 30


def _interval_seconds() -> int:
    """Tick interval from env, robust to empty/invalid values (-> default)."""
    raw = os.getenv("SC_CURATION_SCAN_INTERVAL_SEC")
    if raw is None or raw == "":
        return _DEFAULT_INTERVAL_SEC
    try:
        return int(raw)
    except ValueError:
        return _DEFAULT_INTERVAL_SEC


def _find_species_code(files: list[str]) -> str | None:
    """Species code from a single `.species.<code>` marker, else None.

    Zero or multiple species markers (or an empty code) -> None, so the sample is
    still discovered but the asset fast-fails with a clear reason.
    """
    codes = [
        f[len(SPECIES_MARKER_PREFIX):]
        for f in files
        if f.startswith(SPECIES_MARKER_PREFIX) and f[len(SPECIES_MARKER_PREFIX):]
    ]
    return codes[0] if len(codes) == 1 else None


def discover_samples(
    watch_dir: str, done_marker: str, h5ad_glob: str
) -> list[tuple[str, str, str | None]]:
    """Find completed sample folders under watch_dir.

    A folder qualifies if it contains the done marker AND exactly one file
    matching h5ad_glob. Returns sorted
    [(partition_key, abs_h5ad_path, species_code_or_None), ...]. Folders with
    the marker but zero or multiple h5ads are skipped. The species code comes
    from a `.species.<code>` marker; it is NOT required for discovery (a missing
    or ambiguous one yields None and the asset fast-fails).
    """
    if not os.path.isdir(watch_dir):
        return []
    found: list[tuple[str, str, str | None]] = []
    for root, _dirs, files in os.walk(watch_dir):
        if done_marker not in files:
            continue
        matches = sorted(glob.glob(os.path.join(root, h5ad_glob)))
        if len(matches) != 1:
            continue
        key = partition_key_for(watch_dir, root)
        found.append((key, os.path.abspath(matches[0]), _find_species_code(files)))
    found.sort(key=lambda kv: kv[0])
    return found


@dg.sensor(
    job=h5ad_qc_job,
    minimum_interval_seconds=_interval_seconds(),
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def watch_h5ad_dir(
    context: dg.SensorEvaluationContext, curation: CurationSettings
):
    """Marker-driven discovery sensor: register new samples + request one run each."""
    if not os.path.isdir(curation.watch_dir):
        return dg.SkipReason(f"watch dir not found: {curation.watch_dir}")

    discovered = discover_samples(
        curation.watch_dir, curation.done_marker, curation.h5ad_glob
    )
    if not discovered:
        return dg.SkipReason(f"no completed samples under {curation.watch_dir}")

    # Dedup on partition existence (not submitted-run state). Trade-off: if the
    # daemon registers a partition then crashes before submitting its RunRequest,
    # that sample is skipped on later ticks (visible-but-unmaterialized in the UI;
    # recoverable via manual re-materialize). Acceptable for dev; revisit for the
    # production daemon (spec §10).
    new = [
        (key, path, species)
        for key, path, species in discovered
        if not context.instance.has_dynamic_partition(h5ad_partitions.name, key)
    ]
    if not new:
        return dg.SkipReason("no new samples since last tick")

    new_keys = [key for key, _, _ in new]
    return dg.SensorResult(
        dynamic_partitions_requests=[h5ad_partitions.build_add_request(new_keys)],
        run_requests=[
            dg.RunRequest(
                partition_key=key,
                run_key=key,
                tags={H5AD_PATH_TAG: path, SPECIES_TAG: species or ""},
            )
            for key, path, species in new
        ],
    )

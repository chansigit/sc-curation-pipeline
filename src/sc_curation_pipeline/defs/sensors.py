import glob
import os

import dagster as dg

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.qc import H5AD_PATH_TAG, h5ad_qc_job
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for

# Fallback tick interval used only when SC_CURATION_SCAN_INTERVAL_SEC is unset.
# The decorator's minimum_interval_seconds is read from the env at import time;
# the resource's scan_interval_sec field is then purely informational.
_DEFAULT_INTERVAL_SEC = 30


def discover_samples(
    watch_dir: str, done_marker: str, h5ad_glob: str
) -> list[tuple[str, str]]:
    """Find completed sample folders under watch_dir.

    A folder qualifies if it contains the done marker AND exactly one file
    matching h5ad_glob. Returns sorted [(partition_key, abs_h5ad_path), ...].
    Folders with the marker but zero or multiple h5ads are skipped.
    """
    if not os.path.isdir(watch_dir):
        return []
    found: list[tuple[str, str]] = []
    for root, _dirs, files in os.walk(watch_dir):
        if done_marker not in files:
            continue
        matches = sorted(glob.glob(os.path.join(root, h5ad_glob)))
        if len(matches) != 1:
            continue
        key = partition_key_for(watch_dir, root)
        found.append((key, os.path.abspath(matches[0])))
    found.sort(key=lambda kv: kv[0])
    return found


@dg.sensor(
    job=h5ad_qc_job,
    minimum_interval_seconds=int(
        os.getenv("SC_CURATION_SCAN_INTERVAL_SEC", str(_DEFAULT_INTERVAL_SEC))
    ),
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

    new = [
        (key, path)
        for key, path in discovered
        if not context.instance.has_dynamic_partition(h5ad_partitions.name, key)
    ]
    if not new:
        return dg.SkipReason("no new samples since last tick")

    new_keys = [key for key, _ in new]
    return dg.SensorResult(
        dynamic_partitions_requests=[h5ad_partitions.build_add_request(new_keys)],
        run_requests=[
            dg.RunRequest(
                partition_key=key,
                run_key=key,
                tags={H5AD_PATH_TAG: path},
            )
            for key, path in new
        ],
    )

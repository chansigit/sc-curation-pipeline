import os

import dagster as dg

from sc_curation_pipeline.defs.sensors import discover_samples, watch_h5ad_dir
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for
from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.qc import H5AD_PATH_TAG


def _make_sample(watch, name, *, with_done=True, n_h5ad=1):
    folder = os.path.join(watch, name)
    os.makedirs(folder, exist_ok=True)
    for i in range(n_h5ad):
        with open(os.path.join(folder, f"f{i}.h5ad"), "wb") as fh:
            fh.write(b"x")
    if with_done:
        open(os.path.join(folder, ".done"), "w").close()
    return folder


def test_discover_samples_only_done_with_one_h5ad(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    good = _make_sample(watch, "good", with_done=True, n_h5ad=1)
    _make_sample(watch, "no_done", with_done=False, n_h5ad=1)   # skipped: no marker
    _make_sample(watch, "two_h5ad", with_done=True, n_h5ad=2)   # skipped: ambiguous
    nested = _make_sample(watch, "GSE1/sampleA", with_done=True, n_h5ad=1)  # nested OK

    found = discover_samples(watch, ".done", "*.h5ad")
    keys = {k for k, _ in found}
    assert keys == {
        partition_key_for(watch, good),
        partition_key_for(watch, nested),
    }
    paths = dict(found)
    assert os.path.isfile(paths[partition_key_for(watch, good)])


def test_sensor_registers_and_requests_new(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    good = _make_sample(watch, "sampleA", with_done=True, n_h5ad=1)
    key = partition_key_for(watch, good)
    settings = CurationSettings(watch_dir=watch, scan_interval_sec=30)

    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )
    result = watch_h5ad_dir(ctx)
    assert isinstance(result, dg.SensorResult)
    assert [r.partition_key for r in result.run_requests] == [key]
    assert result.run_requests[0].run_key == key
    assert result.run_requests[0].tags[H5AD_PATH_TAG].endswith("f0.h5ad")
    dpr = result.dynamic_partitions_requests[0]
    assert dpr.partitions_def_name == "h5ad_samples"
    assert dpr.partition_keys == [key]


def test_sensor_dedups_already_registered(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    good = _make_sample(watch, "sampleA", with_done=True, n_h5ad=1)
    key = partition_key_for(watch, good)
    settings = CurationSettings(watch_dir=watch)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])  # pre-registered
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )
    result = watch_h5ad_dir(ctx)
    assert isinstance(result, dg.SkipReason)
    assert result.skip_message


def test_sensor_skips_when_no_done(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    _make_sample(watch, "sampleA", with_done=False, n_h5ad=1)
    settings = CurationSettings(watch_dir=watch)

    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )
    result = watch_h5ad_dir(ctx)
    assert isinstance(result, dg.SkipReason)


def test_sensor_skips_missing_watch_dir(tmp_path):
    watch = str(tmp_path / "does_not_exist")
    settings = CurationSettings(watch_dir=watch)
    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )
    result = watch_h5ad_dir(ctx)
    assert isinstance(result, dg.SkipReason)


def test_sensor_write_once_no_rerun_on_change(tmp_path):
    # §5.3 write-once: once a sample is registered, mutating its h5ad bytes/mtime
    # (without changing identity) must NOT yield a new RunRequest on the next tick.
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    folder = _make_sample(watch, "sampleA", with_done=True, n_h5ad=1)
    key = partition_key_for(watch, folder)
    h5ad_path = os.path.join(folder, "f0.h5ad")
    settings = CurationSettings(watch_dir=watch)

    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )

    # First tick: registers the partition and requests one run.
    first = watch_h5ad_dir(ctx)
    assert isinstance(first, dg.SensorResult)
    assert [r.partition_key for r in first.run_requests] == [key]
    # Apply the dynamic-partition registration as the daemon would.
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])

    # Mutate the already-registered sample's h5ad content + mtime; identity (the
    # folder/partition key) is unchanged.
    with open(h5ad_path, "wb") as fh:
        fh.write(b"yy")
    future = os.stat(h5ad_path).st_mtime + 10_000
    os.utime(h5ad_path, (future, future))

    # Second tick on the same instance: no new RunRequest for the registered key.
    second = watch_h5ad_dir(ctx)
    if isinstance(second, dg.SensorResult):
        assert key not in [r.partition_key for r in second.run_requests]
    else:
        assert isinstance(second, dg.SkipReason)

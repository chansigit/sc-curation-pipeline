import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.qc import compute_qc


def test_compute_qc_sparse_counts(tmp_path, h5ad_writer, make_sparse_counts):
    X, var_names = make_sparse_counts(n_obs=120, n_vars=10, seed=1)
    path = h5ad_writer(str(tmp_path / "s" / "a.h5ad"), X,
                       var_names=var_names, add_raw=True, add_extras=True)
    out = compute_qc(path)
    assert out["n_cells"] == 120
    assert out["n_genes"] == 10
    assert out["is_sparse"] is True
    assert out["has_raw"] is True
    assert "counts" in out["layers"]
    assert "X_pca" in out["obsm"]
    assert "batch" in out["obs_columns"]
    assert "gene_ids" in out["var_columns"]
    assert out["is_raw_counts"] is True
    assert out["total_counts"] > 0
    assert out["mito_pct"] >= 0.0
    assert out["ribo_pct"] >= 0.0
    assert out["file_size_bytes"] > 0


def test_compute_qc_dense(tmp_path, h5ad_writer):
    import os
    from datetime import datetime

    X = np.array([[1.0, 0.0, 2.0], [0.0, 3.0, 0.0]], dtype=np.float32)
    path = h5ad_writer(str(tmp_path / "d" / "x.h5ad"), X,
                       var_names=["GENE0", "MT-CO1", "RPS3"])
    out = compute_qc(path)
    assert out["n_cells"] == 2
    assert out["n_genes"] == 3
    assert out["is_sparse"] is False
    assert out["is_raw_counts"] is True
    assert out["total_counts"] == 6.0
    # mito = MT-CO1 column sum (0+3=3) over total 6 -> 50%
    assert abs(out["mito_pct"] - 50.0) < 1e-6
    # ribo = RPS3 column sum (2+0=2) over total 6 -> ~33.33%
    assert abs(out["ribo_pct"] - (100.0 * 2.0 / 6.0)) < 1e-6
    # file metadata: absolute path + ISO-8601 mtime string
    assert out["path"] == os.path.abspath(path)
    assert isinstance(out["mtime"], str)
    datetime.fromisoformat(out["mtime"])  # parses as ISO-8601


def test_compute_qc_normalized_is_not_raw(tmp_path, h5ad_writer):
    X = np.log1p(np.array([[1.0, 4.0], [2.0, 8.0]], dtype=np.float32))
    path = h5ad_writer(str(tmp_path / "n" / "n.h5ad"), X, var_names=["GENE0", "GENE1"])
    out = compute_qc(path)
    assert out["is_raw_counts"] is False


def test_compute_qc_empty_matrix(tmp_path, h5ad_writer):
    import os
    from datetime import datetime

    X = sp.csr_matrix(np.zeros((5, 4), dtype=np.float32))
    path = h5ad_writer(str(tmp_path / "e" / "e.h5ad"), X,
                       var_names=["GENE0", "GENE1", "MT-CO1", "RPS3"])
    out = compute_qc(path)
    assert out["n_cells"] == 5
    assert out["total_counts"] == 0.0
    assert out["mito_pct"] == 0.0
    assert out["ribo_pct"] == 0.0
    assert out["density"] == 0.0
    assert out["sparsity"] == 1.0
    # file metadata: absolute path + ISO-8601 mtime string
    assert out["path"] == os.path.abspath(path)
    assert isinstance(out["mtime"], str)
    datetime.fromisoformat(out["mtime"])  # parses as ISO-8601


def test_compute_qc_streaming_matches_inmemory(tmp_path, h5ad_writer, make_sparse_counts):
    X, var_names = make_sparse_counts(n_obs=200, n_vars=8, seed=2)
    path = h5ad_writer(str(tmp_path / "str" / "s.h5ad"), X, var_names=var_names)
    full = compute_qc(path, memory_cap=10_000_000)
    streamed = compute_qc(path, memory_cap=16)  # tiny cap forces row chunking
    assert streamed["total_counts"] == full["total_counts"]
    assert streamed["median_counts_per_cell"] == full["median_counts_per_cell"]
    assert streamed["median_genes_per_cell"] == full["median_genes_per_cell"]
    assert abs(streamed["mito_pct"] - full["mito_pct"]) < 1e-9
    assert streamed["density"] == full["density"]
    assert streamed["sparsity"] == full["sparsity"]


def test_partitions_def_name():
    from sc_curation_pipeline.defs.partitions import h5ad_partitions

    assert h5ad_partitions.name == "h5ad_samples"


import os

import dagster as dg

from sc_curation_pipeline.defs.qc import h5ad_qc, h5ad_qc_job  # noqa: E402
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for  # noqa: E402
from sc_curation_pipeline.defs.partitions import h5ad_partitions  # noqa: E402


def _materialize(path_to_h5ad, watch_dir, key, settings, instance):
    return dg.materialize(
        [h5ad_qc],
        partition_key=key,
        instance=instance,
        resources={"curation": settings},
        tags={"sc/h5ad_path": path_to_h5ad},
        raise_on_error=False,
    )


def test_h5ad_qc_materialize_pass(tmp_path, h5ad_writer, make_sparse_counts):
    watch = str(tmp_path / "watch")
    folder = os.path.join(watch, "GSE1_sampleA")
    X, var_names = make_sparse_counts(n_obs=300, n_vars=12, seed=3)
    path = h5ad_writer(os.path.join(folder, "a.h5ad"), X, var_names=var_names)
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch, min_cells=100, max_mito_pct=90.0)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])
    result = _materialize(path, watch, key, settings, instance)
    assert result.success

    mats = result.asset_materializations_for_node("h5ad_qc")
    md = mats[0].metadata
    assert md["n_cells"].value == 300
    assert md["n_genes"].value == 12
    assert mats[0].partition == key
    assert md["h5ad_path"].value == os.path.abspath(path)

    evals = {e.check_name: e for e in result.get_asset_check_evaluations()}
    assert evals["min_cells"].passed is True
    assert evals["is_raw_counts"].passed is True
    assert evals["max_mito_pct"].severity == dg.AssetCheckSeverity.ERROR
    # ERROR-severity checks render red but do NOT fail the run by default.
    assert result.success is True
    assert not result.is_node_failed("h5ad_qc")


def test_h5ad_qc_soft_gate_fails_check_not_run(tmp_path, h5ad_writer, make_sparse_counts):
    watch = str(tmp_path / "watch")
    folder = os.path.join(watch, "tiny")
    X, var_names = make_sparse_counts(n_obs=10, n_vars=6, seed=4)
    path = h5ad_writer(os.path.join(folder, "t.h5ad"), X, var_names=var_names)
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch, min_cells=100, max_mito_pct=20.0)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])
    result = _materialize(path, watch, key, settings, instance)
    assert result.success  # soft gate -> green run

    evals = {e.check_name: e for e in result.get_asset_check_evaluations()}
    assert evals["min_cells"].passed is False  # 10 < 100 -> red check


def test_h5ad_qc_corrupt_raises_failure(tmp_path):
    watch = str(tmp_path / "watch")
    folder = os.path.join(watch, "bad")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "broken.h5ad")
    with open(path, "wb") as fh:
        fh.write(b"this is not an hdf5 file")
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])
    result = _materialize(path, watch, key, settings, instance)
    assert result.success is False
    assert result.is_node_failed("h5ad_qc")


def test_h5ad_qc_missing_watch_dir_raises(tmp_path):
    # Partition whose run carries NO sc/h5ad_path tag, and a watch_dir that does
    # not exist on disk -> resolve_h5ad_path must raise dg.Failure (spec §5.1).
    missing = str(tmp_path / "does_not_exist")
    key = "ghost_sample"
    settings = CurationSettings(watch_dir=missing)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])
    result = dg.materialize(
        [h5ad_qc],
        partition_key=key,
        instance=instance,
        resources={"curation": settings},
        raise_on_error=False,  # no sc/h5ad_path tag -> falls back to watch dir
    )
    assert result.success is False
    assert result.is_node_failed("h5ad_qc")
    failures = [
        e
        for e in result.get_step_failure_events()
        if e.step_key == "h5ad_qc"
    ]
    assert failures
    msg = failures[0].event_specific_data.error.message
    assert "SC_CURATION_WATCH_DIR" in msg
    assert missing in msg


def test_h5ad_qc_job_targets_asset():
    assert h5ad_qc_job.name == "h5ad_qc_job"


def test_compute_qc_large_noninteger_not_raw(tmp_path, h5ad_writer):
    import numpy as np
    X = np.array([[60274.35, 159721.49], [80000.5, 99999.99]], dtype=np.float64)
    path = h5ad_writer(str(tmp_path / "big" / "b.h5ad"), X, var_names=["GENE0", "GENE1"])
    assert compute_qc(path)["is_raw_counts"] is False


def test_compute_qc_large_integers_are_raw(tmp_path, h5ad_writer):
    import numpy as np
    X = np.array([[1_000_000.0, 2_500_000.0], [3_000_000.0, 0.0]], dtype=np.float64)
    path = h5ad_writer(str(tmp_path / "bigint" / "b.h5ad"), X, var_names=["GENE0", "GENE1"])
    assert compute_qc(path)["is_raw_counts"] is True


def test_compute_qc_inf_not_raw(tmp_path, h5ad_writer):
    import numpy as np
    X = np.array([[1.0, 2.0], [3.0, np.inf]], dtype=np.float64)
    path = h5ad_writer(str(tmp_path / "inf" / "b.h5ad"), X, var_names=["GENE0", "GENE1"])
    assert compute_qc(path)["is_raw_counts"] is False


def test_h5ad_qc_transient_error_is_retried(tmp_path, monkeypatch):
    # Regression for BUG 2: a transient (non-Failure) read error must be retried
    # by RetryPolicy(max_retries=2); previously allow_retries=False suppressed it.
    from sc_curation_pipeline.defs import qc as qcmod
    watch = str(tmp_path / "watch")
    folder = os.path.join(watch, "flaky")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "f.h5ad")
    open(path, "wb").close()
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch)
    calls = []
    def boom(p, *a, **k):
        calls.append(p)
        raise OSError("transient lustre hiccup")
    monkeypatch.setattr(qcmod, "compute_qc", boom)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])
    result = dg.materialize(
        [qcmod.h5ad_qc], partition_key=key, instance=instance,
        resources={"curation": settings},
        tags={"sc/h5ad_path": path}, raise_on_error=False,
    )
    assert result.success is False
    assert len(calls) == 3  # initial + 2 retries

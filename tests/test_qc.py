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

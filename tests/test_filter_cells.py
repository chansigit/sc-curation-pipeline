import anndata
import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.filter_cells import (
    filter_cells_by_genes,
    filtered_path_for,
)


def _adata(counts):
    a = anndata.AnnData(X=np.asarray(counts, dtype="float32"))
    a.layers["counts"] = sp.csr_matrix(np.asarray(counts))
    a.obs_names = [f"c{i}" for i in range(a.n_obs)]
    return a


def test_threshold_boundary_keep_and_drop():
    # 3 cells with 3, 2, 1 detected genes; threshold 2 keeps the first two.
    counts = np.array([
        [1, 1, 1],   # 3 detected
        [1, 1, 0],   # 2 detected -> kept (>=2)
        [1, 0, 0],   # 1 detected -> dropped
    ])
    out, nb, na = filter_cells_by_genes(_adata(counts), 2)
    assert (nb, na) == (3, 2)
    assert list(out.obs_names) == ["c0", "c1"]


def test_layers_and_X_subset_together():
    counts = np.array([[1, 1, 1], [1, 0, 0]])  # cell1 has 1 gene -> dropped at thr 2
    a = _adata(counts)
    a.layers["extra"] = sp.csr_matrix(np.array([[5, 5, 5], [9, 9, 9]]))
    out, nb, na = filter_cells_by_genes(a, 2)
    assert (nb, na) == (2, 1)
    assert out.n_obs == 1
    assert out.X.shape == (1, 3)
    assert out.layers["extra"].shape == (1, 3)
    np.testing.assert_array_equal(np.asarray(out.layers["counts"].todense()), [[1, 1, 1]])


def test_all_dropped_returns_zero():
    counts = np.array([[1, 0, 0], [0, 1, 0]])  # each has 1 gene
    out, nb, na = filter_cells_by_genes(_adata(counts), 5)
    assert (nb, na) == (2, 0)
    assert out.n_obs == 0


def test_does_not_mutate_input():
    a = _adata(np.array([[1, 1, 1], [1, 0, 0]]))
    filter_cells_by_genes(a, 2)
    assert a.n_obs == 2  # original untouched


def test_filtered_path_for():
    assert filtered_path_for("/out/s/a.h5ad") == "/out/s/a_filtered.h5ad"
    assert filtered_path_for("x.h5ad") == "x_filtered.h5ad"

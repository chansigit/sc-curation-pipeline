import os

import anndata
import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.standardize import build_standardized_adata, write_standardized


def _counts(n=40, g=20, seed=0):
    return np.random.RandomState(seed).poisson(0.7, size=(n, g)).astype(np.float64)


def test_build_sets_counts_layer_and_lognorm_X():
    counts = _counts()
    ad = anndata.AnnData(X=np.zeros_like(counts))
    ad.layers["spliced"] = sp.csr_matrix(_counts(seed=9))  # velocity layer preserved
    out = build_standardized_adata(ad, sp.csr_matrix(counts), target_sum=1e4)

    cl = out.layers["counts"]
    np.testing.assert_array_equal(np.asarray(cl.todense()), counts)
    # X == log1p(normalize_total(counts, 1e4))
    lib = counts.sum(axis=1, keepdims=True); lib[lib == 0] = 1
    expected = np.log1p(counts / lib * 1e4)
    X = out.X.todense() if sp.issparse(out.X) else out.X
    np.testing.assert_allclose(np.asarray(X), expected, rtol=1e-4, atol=1e-4)
    assert "spliced" in out.layers  # velocity preserved


def test_build_renames_source_layer():
    # counts coming from a non-"counts" layer (e.g. raw_counts) must be RENAMED to
    # "counts" — the old name is dropped so counts is not stored twice; velocity
    # and other layers are left untouched.
    counts = _counts()
    ad = anndata.AnnData(X=np.zeros_like(counts))
    ad.layers["raw_counts"] = sp.csr_matrix(counts)
    ad.layers["spliced"] = sp.csr_matrix(_counts(seed=3))  # velocity layer untouched
    out = build_standardized_adata(ad, ad.layers["raw_counts"], source="layer:raw_counts")
    assert "counts" in out.layers
    assert "raw_counts" not in out.layers   # old name dropped (renamed -> counts)
    assert "spliced" in out.layers          # velocity preserved
    np.testing.assert_array_equal(np.asarray(out.layers["counts"].todense()), counts)


def test_write_standardized_creates_file(tmp_path):
    counts = _counts()
    ad = anndata.AnnData(X=np.zeros_like(counts))
    out = build_standardized_adata(ad, sp.csr_matrix(counts))
    path = str(tmp_path / "nested" / "sample.h5ad")
    write_standardized(out, path)
    assert os.path.isfile(path)
    back = anndata.read_h5ad(path)
    assert "counts" in back.layers
    np.testing.assert_array_equal(np.asarray(back.layers["counts"].todense()), counts)

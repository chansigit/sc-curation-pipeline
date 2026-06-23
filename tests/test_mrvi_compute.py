"""Tests for the MrVI/Leiden compute helpers.

leiden_on_rep is fast and always run. train_mrvi_u_latent does a real (tiny, 1-epoch,
CPU) MrVI smoke — slower, but verifies the model trains and yields the u latent.
"""

import anndata as ad
import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.mrvi_compute import (
    LATENT_KEY,
    LEIDEN_KEY,
    effective_hvg_batch_key,
    leiden_on_rep,
    select_hvg_mask,
    train_mrvi_u_latent,
)


def _structured_counts(rng, n_obs, n_var, n_hot):
    """Counts with `n_hot` clearly high-variance genes (so seurat_v3 has signal)."""
    counts = rng.poisson(0.5, size=(n_obs, n_var))
    counts[:, :n_hot] = rng.poisson(5.0, size=(n_obs, n_hot))
    return counts


def test_select_hvg_mask_subsets_to_n_top():
    rng = np.random.default_rng(0)
    counts = _structured_counts(rng, 200, 300, n_hot=20)
    a = ad.AnnData(X=counts.astype("float32"))
    a.layers["counts"] = sp.csr_matrix(counts)
    mask = select_hvg_mask(a, n_top_genes=50, batch_key=None)
    assert mask.dtype == bool and mask.shape == (300,)
    assert mask.sum() == 50                       # exactly top-N selected
    assert a.n_vars == 300 and "highly_variable" not in a.var  # adata not mutated (inplace=False)


def test_select_hvg_mask_all_when_fewer_genes():
    a = ad.AnnData(X=np.zeros((10, 30), dtype="float32"))
    a.layers["counts"] = sp.csr_matrix(np.ones((10, 30)))
    mask = select_hvg_mask(a, n_top_genes=2000, batch_key=None)   # 30 < 2000 -> all
    assert mask.all() and mask.shape == (30,)


def test_effective_hvg_batch_key():
    a = ad.AnnData(X=np.zeros((300, 5), dtype="float32"))
    a.obs["big"] = ["A"] * 150 + ["B"] * 150        # both batches >= 100
    a.obs["small"] = ["A"] * 250 + ["B"] * 50        # one batch < 100
    assert effective_hvg_batch_key(a, batch_key="big", min_batch_cells=100) == "big"
    assert effective_hvg_batch_key(a, batch_key="small", min_batch_cells=100) is None
    assert effective_hvg_batch_key(a, batch_key="missing") is None
    assert effective_hvg_batch_key(a, batch_key=None) is None


def test_select_hvg_mask_falls_back_when_small_batch():
    # a tiny sample would make seurat_v3's per-batch loess singular -> must fall back
    # to global selection (no crash) and still return exactly top-N.
    rng = np.random.default_rng(2)
    counts = _structured_counts(rng, 260, 300, n_hot=20)
    a = ad.AnnData(X=counts.astype("float32"))
    a.layers["counts"] = sp.csr_matrix(counts)
    a.obs["sample"] = ["A"] * 230 + ["B"] * 30       # B too small -> global fallback
    mask = select_hvg_mask(a, n_top_genes=50, batch_key="sample")
    assert mask.sum() == 50


def test_leiden_on_rep_clusters_separated_embedding():
    rng = np.random.default_rng(0)
    # two well-separated blobs in a 10-d "latent" -> Leiden should find >= 2 clusters
    emb = np.r_[rng.normal(0.0, 0.3, size=(120, 10)), rng.normal(8.0, 0.3, size=(120, 10))]
    a = ad.AnnData(X=np.zeros((240, 5), dtype="float32"))
    a.obs_names = [f"c{i}" for i in range(240)]
    a.obsm[LATENT_KEY] = emb
    n_clusters = leiden_on_rep(a, resolution=1.0)
    assert LEIDEN_KEY in a.obs.columns
    assert a.obs[LEIDEN_KEY].shape[0] == 240
    assert n_clusters >= 2


def test_train_mrvi_u_latent_smoke_with_hvg():
    # real MrVI (torch), 1 epoch on CPU, trained on a HVG subset (n_hvg < n_vars):
    # verifies the select-HVG -> subset-copy -> train path and that the u latent maps
    # back onto all cells of the full-gene adata.
    rng = np.random.default_rng(0)
    counts = _structured_counts(rng, 200, 300, n_hot=30)
    a = ad.AnnData(X=np.asarray(counts, dtype="float32"))
    a.layers["counts"] = sp.csr_matrix(counts)
    a.obs_names = [f"c{i}" for i in range(200)]
    a.obs["sample"] = ["A"] * 100 + ["B"] * 100
    u = train_mrvi_u_latent(a, n_hvg=50, max_epochs=1, accelerator="cpu")
    assert u.ndim == 2 and u.shape[0] == 200      # latent aligned to all 200 cells
    assert a.n_vars == 300                         # full-gene adata not subset in place


def test_train_mrvi_u_latent_single_sample_fallback():
    # no 'sample' column -> a constant sample is added; still produces a u latent
    rng = np.random.default_rng(1)
    counts = rng.poisson(0.5, size=(150, 80))
    a = ad.AnnData(X=np.asarray(counts, dtype="float32"))
    a.layers["counts"] = sp.csr_matrix(counts)
    a.obs_names = [f"c{i}" for i in range(150)]
    u = train_mrvi_u_latent(a, max_epochs=1, accelerator="cpu")
    assert u.shape[0] == 150
    assert "sample" in a.obs.columns           # fallback column added

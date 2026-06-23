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
    leiden_on_rep,
    train_mrvi_u_latent,
)


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


def test_train_mrvi_u_latent_smoke():
    # real MrVI (torch), 1 epoch on CPU — just verify it trains and returns the u latent
    rng = np.random.default_rng(0)
    counts = rng.poisson(0.5, size=(200, 100))
    a = ad.AnnData(X=np.asarray(counts, dtype="float32"))
    a.layers["counts"] = sp.csr_matrix(counts)
    a.obs_names = [f"c{i}" for i in range(200)]
    a.obs["sample"] = ["A"] * 100 + ["B"] * 100
    u = train_mrvi_u_latent(a, max_epochs=1, accelerator="cpu")
    assert u.ndim == 2 and u.shape[0] == 200


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

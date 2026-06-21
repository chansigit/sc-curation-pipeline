from pathlib import Path

import numpy as np
import pytest
import scipy.sparse as sp
import anndata as ad

from sc_curation_pipeline.defs.settings import CurationSettings


@pytest.fixture
def settings_factory(tmp_path):
    """Return a callable producing CurationSettings rooted at a temp watch dir."""

    def _make(**overrides):
        watch = overrides.pop("watch_dir", str(tmp_path / "watch"))
        Path(watch).mkdir(parents=True, exist_ok=True)
        out = overrides.pop("output_dir", str(tmp_path / "out"))
        kwargs = dict(
            watch_dir=watch,
            output_dir=out,
            done_marker=".done",
            h5ad_glob="*.h5ad",
            scan_interval_sec=30,
            min_cells=100,
            min_genes=5000,
            min_genes_per_cell=400,
        )
        kwargs.update(overrides)
        return CurationSettings(**kwargs)

    return _make


def _write_h5ad(path, X, var_names=None, add_raw=False, add_extras=False):
    n_obs, n_vars = X.shape
    if var_names is None:
        var_names = [f"GENE{i}" for i in range(n_vars)]
    adata = ad.AnnData(X=X)
    adata.var_names = list(var_names)
    adata.obs_names = [f"cell{i}" for i in range(n_obs)]
    adata.obs["batch"] = ["b"] * n_obs
    adata.var["gene_ids"] = list(var_names)
    if add_raw:
        adata.raw = adata
    if add_extras:
        adata.layers["counts"] = X.copy()
        adata.obsm["X_pca"] = np.zeros((n_obs, 2), dtype=np.float32)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    adata.write_h5ad(path)
    return path


@pytest.fixture
def h5ad_writer():
    """Return the _write_h5ad helper for building synthetic h5ad files."""
    return _write_h5ad


@pytest.fixture
def make_sparse_counts():
    """Return a callable building an integer CSR count matrix with MT-/RPS genes."""

    def _make(n_obs=120, n_vars=10, seed=0):
        rng = np.random.default_rng(seed)
        dense = rng.integers(0, 5, size=(n_obs, n_vars)).astype(np.float32)
        var_names = [f"GENE{i}" for i in range(n_vars - 2)] + ["MT-CO1", "RPS3"]
        return sp.csr_matrix(dense), var_names

    return _make


@pytest.fixture
def write_adata():
    """Write an h5ad with given X and optional layers/raw. Returns the path."""
    def _make(path, X, *, var_names=None, layers=None, raw_X=None, raw_var_names=None):
        n_obs, n_vars = X.shape
        if var_names is None:
            var_names = [f"GENE{i}" for i in range(n_vars)]
        adata = ad.AnnData(X=X)
        adata.var_names = list(var_names)
        adata.obs_names = [f"cell{i}" for i in range(n_obs)]
        if layers:
            for k, v in layers.items():
                adata.layers[k] = v
        if raw_X is not None:
            rv = raw_var_names or [f"GENE{i}" for i in range(raw_X.shape[1])]
            raw = ad.AnnData(X=raw_X)
            raw.var_names = list(rv)
            adata.raw = raw
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        adata.write_h5ad(path)
        return path
    return _make

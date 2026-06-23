"""Pure compute for the MrVI + Leiden step (no Dagster / no Slurm).

Split out so both the external Slurm job script (scripts/mrvi_leiden_job.py) and the
unit tests can import it. ``train_mrvi_u_latent`` imports scvi lazily, so importing
this module for the (light) Leiden helper does not pull in scvi/torch.
"""

import numpy as np
import scanpy as sc

SAMPLE_KEY = "sample"
DUMMY_SAMPLE = "_all"
COUNTS_LAYER = "counts"
LATENT_KEY = "X_mrvi_u"
LEIDEN_KEY = "mrvi_leiden"
DEFAULT_N_HVG = 2000
# seurat_v3 fits a loess (variance ~ mean) per batch; on a small batch that fit goes
# numerically singular ("near singularities"). Only batch when every batch clears this.
MIN_HVG_BATCH_CELLS = 100


def effective_hvg_batch_key(adata, *, batch_key, min_batch_cells: int = MIN_HVG_BATCH_CELLS):
    """The batch key seurat_v3 can safely use, else None (-> global HVG).

    Returns ``batch_key`` only if it exists and **every** batch has at least
    ``min_batch_cells`` cells; otherwise None, because a small batch makes the
    per-batch loess fit singular."""
    if batch_key is None or batch_key not in adata.obs:
        return None
    if int(adata.obs[batch_key].value_counts().min()) < min_batch_cells:
        return None
    return batch_key


def select_hvg_mask(adata, *, n_top_genes: int, batch_key=None,
                    layer: str = COUNTS_LAYER) -> np.ndarray:
    """Boolean mask (over adata.var) of the top-N highly variable genes.

    Uses the ``seurat_v3`` flavour, which expects **raw counts** (read from ``layer``).
    ``batch_key`` selects HVGs per batch then ranks across batches (matches MrVI's
    multi-sample setup) — but only when every batch is large enough
    (:func:`effective_hvg_batch_key`); small samples fall back to global selection,
    and a loess failure triggers a final global retry. Computed with ``inplace=False``
    so it does NOT mutate ``adata`` (the written-back object keeps a clean ``.var``).
    Returns an all-True mask when ``n_top_genes`` is falsy or >= n_vars."""
    if not n_top_genes or adata.n_vars <= n_top_genes:
        return np.ones(adata.n_vars, dtype=bool)
    eff_batch = effective_hvg_batch_key(adata, batch_key=batch_key)
    try:
        hvg = sc.pp.highly_variable_genes(
            adata, n_top_genes=n_top_genes, flavor="seurat_v3",
            layer=layer, batch_key=eff_batch, inplace=False,
        )
    except Exception:
        if eff_batch is None:
            raise
        hvg = sc.pp.highly_variable_genes(  # last-resort global retry
            adata, n_top_genes=n_top_genes, flavor="seurat_v3",
            layer=layer, batch_key=None, inplace=False,
        )
    assert hvg is not None  # inplace=False always returns a DataFrame
    return hvg["highly_variable"].to_numpy()


def train_mrvi_u_latent(adata, *, sample_key: str = SAMPLE_KEY, n_hvg: int = DEFAULT_N_HVG,
                        max_epochs=None, accelerator: str = "auto", seed: int = 0) -> np.ndarray:
    """Train MrVI (torch backend) on the top-``n_hvg`` HVGs and return its **u**
    latent (n_obs x d), aligned to ``adata``'s cells.

    HVGs (seurat_v3, batch_key=sample) are selected on the raw counts and MrVI is
    trained on a gene-subset *copy* — so ``adata`` itself is not subset and the
    per-cell latent maps back onto the full-gene object unchanged. ``n_hvg=0`` (or
    fewer genes than ``n_hvg``) trains on all genes. Single-sample fallback: if
    ``sample_key`` is absent, a constant sample column is added (MrVI degenerates to
    scVI-like). ``accelerator="auto"`` uses the GPU when available. Mutates
    ``adata.obs`` only (adds the dummy sample column when needed); writes no files.
    """
    import scvi
    from scvi.external import MRVI

    scvi.settings.seed = seed
    if sample_key not in adata.obs.columns:
        adata.obs[sample_key] = DUMMY_SAMPLE
    mask = select_hvg_mask(adata, n_top_genes=n_hvg, batch_key=sample_key)
    train_adata = adata if mask.all() else adata[:, mask].copy()
    MRVI.setup_anndata(train_adata, layer=COUNTS_LAYER, sample_key=sample_key, backend="torch")
    model = MRVI(train_adata)
    model.train(accelerator=accelerator, max_epochs=max_epochs)
    return np.asarray(model.get_latent_representation(give_z=False))  # give_z=False -> u latent


def leiden_on_rep(adata, *, rep_key: str = LATENT_KEY, resolution: float = 1.0,
                  key_added: str = LEIDEN_KEY) -> int:
    """Build a neighbor graph on ``obsm[rep_key]`` and Leiden-cluster into
    ``obs[key_added]``. Uses the igraph flavour (avoids the leidenalg deprecation).
    Returns the number of clusters. Mutates ``adata``."""
    sc.pp.neighbors(adata, use_rep=rep_key)
    sc.tl.leiden(adata, resolution=resolution, key_added=key_added,
                 flavor="igraph", n_iterations=2, directed=False)
    return int(adata.obs[key_added].nunique())

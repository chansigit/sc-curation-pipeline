"""Pure compute for the MrVI + Leiden step (no Dagster / no Slurm).

Split out so both the external Slurm job script (scripts/mrvi_leiden_job.py) and the
unit tests can import it. ``train_mrvi_u_latent`` imports scvi lazily, so importing
this module for the (light) Leiden helper does not pull in scvi/torch.
"""

import numpy as np
import scanpy as sc

SAMPLE_KEY = "sample"
DUMMY_SAMPLE = "_all"
LATENT_KEY = "X_mrvi_u"
LEIDEN_KEY = "mrvi_leiden"


def train_mrvi_u_latent(adata, *, sample_key: str = SAMPLE_KEY, max_epochs=None,
                        accelerator: str = "auto", seed: int = 0) -> np.ndarray:
    """Train MrVI (torch backend) and return its **u** latent (n_obs x d).

    Single-sample fallback: if ``sample_key`` is absent, a constant sample column is
    added (MrVI degenerates to scVI-like) so a clustering result always exists.
    ``accelerator="auto"`` uses the GPU when available, else CPU. Mutates
    ``adata.obs`` (adds the dummy sample column when needed); does not write files.
    """
    import scvi
    from scvi.external import MRVI

    scvi.settings.seed = seed
    if sample_key not in adata.obs.columns:
        adata.obs[sample_key] = DUMMY_SAMPLE
    MRVI.setup_anndata(adata, layer="counts", sample_key=sample_key, backend="torch")
    model = MRVI(adata)
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

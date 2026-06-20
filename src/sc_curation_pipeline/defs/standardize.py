"""Build & write the standardized counts-bearing AnnData for one sample."""

import os

import numpy as np
import scanpy as sc


def build_standardized_adata(adata, counts, *, target_sum: float = 1e4):
    """Set layers['counts']=counts (integer) and X=normalize_total+log1p(counts).

    Mutates `adata` in place and returns it. Other layers (e.g. spliced/unspliced)
    and obs/var/obsm/obsp/uns are preserved. The counts layer keeps integer dtype;
    normalization runs on a float copy in X.
    """
    adata.layers["counts"] = counts
    adata.X = counts.astype(np.float32)  # float copy for normalization; counts layer untouched
    sc.pp.normalize_total(adata, target_sum=target_sum)
    sc.pp.log1p(adata)
    return adata


def write_standardized(adata, out_path: str) -> None:
    """Write the standardized AnnData to out_path (creating parent dirs)."""
    dirname = os.path.dirname(out_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    adata.write_h5ad(out_path)

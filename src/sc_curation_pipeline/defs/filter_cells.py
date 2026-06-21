"""Cell-level filtering by detected genes per cell (on the counts layer)."""

import os

import numpy as np
import scipy.sparse as sp


def _genes_per_cell(counts) -> np.ndarray:
    """Detected genes per cell (number of genes with counts>0), 1-D length n_obs."""
    if sp.issparse(counts):
        return np.asarray(counts.tocsr().getnnz(axis=1)).ravel()
    C = np.asarray(counts)
    return (C != 0).sum(axis=1).ravel()


def filter_cells_by_genes(adata, min_genes_per_cell: int):
    """Keep cells whose counts layer has >= min_genes_per_cell detected genes.

    Filters on ``adata.layers["counts"]``; subsets the AnnData by row so X and
    every layer/obs/obsm follow. Returns (adata_filtered, n_before, n_after).
    Does not mutate the input (returns an AnnData slice copy).
    """
    counts = adata.layers["counts"]
    gpc = _genes_per_cell(counts)
    keep = gpc >= min_genes_per_cell
    n_before = int(adata.n_obs)
    n_after = int(keep.sum())
    return adata[keep].copy(), n_before, n_after


def filtered_path_for(standardized_path: str) -> str:
    """Derive the filtered output path by inserting '_filtered' before the ext.

    e.g. '/out/s/a.h5ad' -> '/out/s/a_filtered.h5ad'.
    """
    root, ext = os.path.splitext(standardized_path)
    return f"{root}_filtered{ext}"

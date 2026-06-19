import os
from datetime import datetime, timezone

import numpy as np
import scipy.sparse as sp
import anndata as ad


def compute_qc(path: str, memory_cap: int = 50_000_000) -> dict:
    """Memory-aware QC on an .h5ad file.

    Structural metrics come from backed='r' (cheap). Count metrics are computed
    in memory if n_obs*n_vars <= memory_cap, else X is streamed in row chunks.
    Handles scipy sparse (CSR/CSC) and dense X.
    """
    st = os.stat(path)
    out = {
        "path": os.path.abspath(path),
        "file_size_bytes": int(st.st_size),
        "mtime": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
    }

    A = ad.read_h5ad(path, backed="r")
    try:
        n_cells, n_genes = int(A.n_obs), int(A.n_vars)
        X = A.X
        is_sparse = sp.issparse(X) or getattr(X, "format", None) in ("csr", "csc")

        out.update({
            "n_cells": n_cells,
            "n_genes": n_genes,
            "X_dtype": str(X.dtype),
            "is_sparse": bool(is_sparse),
            "has_raw": A.raw is not None,
            "layers": list(A.layers.keys()),
            "obsm": list(A.obsm.keys()),
            "obsp": list(A.obsp.keys()),
            "obs_columns": list(A.obs.columns),
            "n_obs_columns": len(A.obs.columns),
            "var_columns": list(A.var.columns),
            "n_var_columns": len(A.var.columns),
        })

        up = A.var_names.astype(str).str.upper()
        mito_mask = np.asarray(up.str.startswith("MT-"))
        ribo_mask = np.asarray(up.str.startswith("RPS") | up.str.startswith("RPL"))

        total_cells = n_cells * n_genes
        counts_per_cell = np.zeros(n_cells, dtype=np.float64)
        genes_per_cell = np.zeros(n_cells, dtype=np.float64)
        mito_per_cell = np.zeros(n_cells, dtype=np.float64)
        ribo_per_cell = np.zeros(n_cells, dtype=np.float64)
        nnz_total = 0
        is_integer = True

        def _accumulate(block, start):
            nonlocal nnz_total, is_integer
            end = start + block.shape[0]
            if sp.issparse(block):
                block = block.tocsr()
                counts_per_cell[start:end] = np.asarray(block.sum(axis=1)).ravel()
                genes_per_cell[start:end] = block.getnnz(axis=1)
                nnz_total += block.nnz
                if mito_mask.any():
                    mito_per_cell[start:end] = np.asarray(block[:, mito_mask].sum(axis=1)).ravel()
                if ribo_mask.any():
                    ribo_per_cell[start:end] = np.asarray(block[:, ribo_mask].sum(axis=1)).ravel()
                if is_integer and block.nnz:
                    d = block.data
                    if not np.allclose(d, np.round(d), atol=1e-6):
                        is_integer = False
            else:
                block = np.asarray(block)
                counts_per_cell[start:end] = block.sum(axis=1)
                genes_per_cell[start:end] = (block != 0).sum(axis=1)
                nnz_total += int((block != 0).sum())
                if mito_mask.any():
                    mito_per_cell[start:end] = block[:, mito_mask].sum(axis=1)
                if ribo_mask.any():
                    ribo_per_cell[start:end] = block[:, ribo_mask].sum(axis=1)
                if is_integer and block.size:
                    if not np.allclose(block, np.round(block), atol=1e-6):
                        is_integer = False

        if total_cells <= memory_cap:
            _accumulate(A.to_memory().X, 0)
        else:
            chunk = max(1, memory_cap // max(1, n_genes))
            for start in range(0, n_cells, chunk):
                stop = min(start + chunk, n_cells)
                _accumulate(X[start:stop], start)

        total_counts = float(counts_per_cell.sum())
        out.update({
            "total_counts": total_counts,
            "median_counts_per_cell": float(np.median(counts_per_cell)) if n_cells else 0.0,
            "median_genes_per_cell": float(np.median(genes_per_cell)) if n_cells else 0.0,
            "density": (nnz_total / total_cells) if total_cells else 0.0,
            "sparsity": (1.0 - nnz_total / total_cells) if total_cells else 0.0,
            "mito_pct": (100.0 * mito_per_cell.sum() / total_counts) if total_counts else 0.0,
            "ribo_pct": (100.0 * ribo_per_cell.sum() / total_counts) if total_counts else 0.0,
            "is_raw_counts": bool(is_integer),
        })
    finally:
        if A.isbacked and A.file is not None:
            A.file.close()
    return out

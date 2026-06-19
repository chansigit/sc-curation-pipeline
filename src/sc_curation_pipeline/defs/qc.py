import glob
import os
from datetime import datetime, timezone

import dagster as dg
import numpy as np
import scipy.sparse as sp
import anndata as ad

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.settings import CurationSettings, path_for_partition_key

H5AD_PATH_TAG = "sc/h5ad_path"


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


def resolve_h5ad_path(
    context: dg.AssetExecutionContext, settings: CurationSettings
) -> str:
    """Resolve the absolute .h5ad path for the current partition.

    Prefers the sc/h5ad_path run tag (set by the sensor). Falls back to
    reconstructing the folder from watch_dir + the partition key and globbing.
    Raises dagster.Failure if the file is missing or ambiguous.
    """
    key = context.partition_key
    tag_path = context.run.tags.get(H5AD_PATH_TAG)
    if tag_path and os.path.isfile(tag_path):
        return os.path.abspath(tag_path)

    # No usable tag -> we must fall back to the watch dir. Guard it first so a
    # missing/invalid SC_CURATION_WATCH_DIR fails loudly (spec §5.1) instead of
    # silently globbing nothing.
    if not settings.watch_dir or not os.path.isdir(settings.watch_dir):
        raise dg.Failure(
            description=(
                f"SC_CURATION_WATCH_DIR missing or not a directory: "
                f"{settings.watch_dir!r}"
            ),
            metadata={
                "partition": dg.MetadataValue.text(key),
                "watch_dir": dg.MetadataValue.text(str(settings.watch_dir)),
            },
            allow_retries=False,
        )

    folder = os.path.join(settings.watch_dir, path_for_partition_key(key))
    matches = sorted(glob.glob(os.path.join(folder, settings.h5ad_glob)))
    if len(matches) != 1:
        raise dg.Failure(
            description=(
                f"expected exactly one h5ad in {folder!r} matching "
                f"{settings.h5ad_glob!r}, found {len(matches)}"
            ),
            metadata={
                "partition": dg.MetadataValue.text(key),
                "folder": dg.MetadataValue.path(folder),
                "matches": dg.MetadataValue.json(matches),
            },
            allow_retries=False,
        )
    return os.path.abspath(matches[0])


@dg.asset(
    partitions_def=h5ad_partitions,
    group_name="curation",
    check_specs=[
        dg.AssetCheckSpec(name="min_cells", asset="h5ad_qc"),
        dg.AssetCheckSpec(name="max_mito_pct", asset="h5ad_qc"),
        dg.AssetCheckSpec(name="is_raw_counts", asset="h5ad_qc"),
    ],
    retry_policy=dg.RetryPolicy(max_retries=2),
)
def h5ad_qc(context: dg.AssetExecutionContext, curation: CurationSettings):
    """Lightweight QC on one sample's h5ad; results as metadata + checks."""
    path = resolve_h5ad_path(context, curation)
    try:
        qc = compute_qc(path)
    except dg.Failure:
        raise
    except Exception as exc:  # corrupt / unreadable h5ad -> hard error (red run)
        raise dg.Failure(
            description=f"failed to read/QC h5ad at {path!r}: {exc}",
            metadata={
                "partition": dg.MetadataValue.text(context.partition_key),
                "h5ad_path": dg.MetadataValue.path(path),
                "error": dg.MetadataValue.text(repr(exc)),
            },
            allow_retries=False,
        )

    yield dg.MaterializeResult(
        metadata={
            "h5ad_path": dg.MetadataValue.path(str(qc["path"])),
            "file_size_bytes": dg.MetadataValue.int(int(qc["file_size_bytes"])),
            "mtime": dg.MetadataValue.text(str(qc["mtime"])),
            "n_cells": dg.MetadataValue.int(int(qc["n_cells"])),
            "n_genes": dg.MetadataValue.int(int(qc["n_genes"])),
            "X_dtype": dg.MetadataValue.text(str(qc["X_dtype"])),
            "is_sparse": dg.MetadataValue.bool(bool(qc["is_sparse"])),
            "sparsity": dg.MetadataValue.float(float(qc["sparsity"])),
            "density": dg.MetadataValue.float(float(qc["density"])),
            "has_raw": dg.MetadataValue.bool(bool(qc["has_raw"])),
            "layers": dg.MetadataValue.json(list(qc["layers"])),
            "obsm": dg.MetadataValue.json(list(qc["obsm"])),
            "obsp": dg.MetadataValue.json(list(qc["obsp"])),
            "obs_columns": dg.MetadataValue.json(list(qc["obs_columns"])),
            "n_obs_columns": dg.MetadataValue.int(int(qc["n_obs_columns"])),
            "var_columns": dg.MetadataValue.json(list(qc["var_columns"])),
            "n_var_columns": dg.MetadataValue.int(int(qc["n_var_columns"])),
            "total_counts": dg.MetadataValue.float(float(qc["total_counts"])),
            "median_counts_per_cell": dg.MetadataValue.float(float(qc["median_counts_per_cell"])),
            "median_genes_per_cell": dg.MetadataValue.float(float(qc["median_genes_per_cell"])),
            "mito_pct": dg.MetadataValue.float(float(qc["mito_pct"])),
            "ribo_pct": dg.MetadataValue.float(float(qc["ribo_pct"])),
            "is_raw_counts": dg.MetadataValue.bool(bool(qc["is_raw_counts"])),
        }
    )

    yield dg.AssetCheckResult(
        passed=bool(qc["n_cells"] >= curation.min_cells),
        check_name="min_cells",
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "n_cells": dg.MetadataValue.int(int(qc["n_cells"])),
            "min_cells": dg.MetadataValue.int(int(curation.min_cells)),
        },
    )
    yield dg.AssetCheckResult(
        passed=bool(qc["mito_pct"] <= curation.max_mito_pct),
        check_name="max_mito_pct",
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "mito_pct": dg.MetadataValue.float(float(qc["mito_pct"])),
            "max_mito_pct": dg.MetadataValue.float(float(curation.max_mito_pct)),
        },
    )
    yield dg.AssetCheckResult(
        passed=bool(qc["is_raw_counts"]),
        check_name="is_raw_counts",
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"is_raw_counts": dg.MetadataValue.bool(bool(qc["is_raw_counts"]))},
    )


h5ad_qc_job = dg.define_asset_job(
    name="h5ad_qc_job",
    selection=dg.AssetSelection.assets("h5ad_qc"),
)

import glob
import os

import dagster as dg
import numpy as np
import scipy.sparse as sp
import anndata as ad
import h5py

import stancounts

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.settings import CurationSettings, path_for_partition_key
from sc_curation_pipeline.defs.standardize import build_standardized_adata, write_standardized

H5AD_PATH_TAG = "sc/h5ad_path"


def compute_count_qc(counts, var_names) -> dict:
    """QC metrics computed on an in-memory counts matrix (sparse or dense)."""
    is_sparse = sp.issparse(counts)
    C = counts.tocsr() if is_sparse else np.asarray(counts)
    n_cells, n_vars = int(C.shape[0]), int(C.shape[1])

    up = np.char.upper(np.asarray([str(v) for v in var_names], dtype=str))
    mito_mask = np.char.startswith(up, "MT-")
    ribo_mask = np.char.startswith(up, "RPS") | np.char.startswith(up, "RPL")

    if is_sparse:
        counts_per_cell = np.asarray(C.sum(axis=1)).ravel().astype(np.float64)
        genes_per_cell = C.getnnz(axis=1).astype(np.float64)
        detected_per_gene = np.asarray(C.getnnz(axis=0)).ravel()
        nnz_total = int(C.nnz)
        mito_per_cell = (np.asarray(C[:, mito_mask].sum(axis=1)).ravel()
                         if mito_mask.any() else np.zeros(n_cells))
        ribo_per_cell = (np.asarray(C[:, ribo_mask].sum(axis=1)).ravel()
                         if ribo_mask.any() else np.zeros(n_cells))
    else:
        counts_per_cell = C.sum(axis=1).astype(np.float64)
        genes_per_cell = (C != 0).sum(axis=1).astype(np.float64)
        detected_per_gene = (C != 0).sum(axis=0)
        nnz_total = int((C != 0).sum())
        mito_per_cell = C[:, mito_mask].sum(axis=1) if mito_mask.any() else np.zeros(n_cells)
        ribo_per_cell = C[:, ribo_mask].sum(axis=1) if ribo_mask.any() else np.zeros(n_cells)

    total = n_cells * n_vars
    total_counts = float(counts_per_cell.sum())
    n_genes_detected = int((np.asarray(detected_per_gene).ravel() > 0).sum())
    with np.errstate(divide="ignore", invalid="ignore"):
        mito_pct_per_cell = np.where(counts_per_cell > 0,
                                     100.0 * mito_per_cell / counts_per_cell, 0.0)
    return {
        "n_cells": n_cells,
        "n_vars": n_vars,
        "n_genes_detected": n_genes_detected,
        "total_counts": total_counts,
        "median_counts_per_cell": float(np.median(counts_per_cell)) if n_cells else 0.0,
        "median_genes_per_cell": float(np.median(genes_per_cell)) if n_cells else 0.0,
        "density": (nnz_total / total) if total else 0.0,
        "sparsity": (1.0 - nnz_total / total) if total else 0.0,
        "mito_pct": float(100.0 * mito_per_cell.sum() / total_counts) if total_counts else 0.0,
        "ribo_pct": float(100.0 * ribo_per_cell.sum() / total_counts) if total_counts else 0.0,
        "per_cell": {"counts": counts_per_cell, "genes": genes_per_cell, "mito_pct": mito_pct_per_cell},
    }


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


def output_path_for(output_dir, partition_key, src_path) -> str:
    """Mirror the sample's relative folder under output_dir (never the source)."""
    rel_folder = path_for_partition_key(partition_key)
    return os.path.join(output_dir, rel_folder, os.path.basename(src_path))


@dg.asset(
    partitions_def=h5ad_partitions,
    group_name="curation",
    retry_policy=dg.RetryPolicy(max_retries=2),
)
def h5ad_qc(context: dg.AssetExecutionContext, curation: CurationSettings):
    """Standardize one sample (counts layer + lognorm X), write it out, QC on counts."""
    path = resolve_h5ad_path(context, curation)
    if not h5py.is_hdf5(path):
        raise dg.Failure(
            description=f"not a valid HDF5/h5ad file: {path!r} (file signature not found)",
            metadata={"partition": dg.MetadataValue.text(context.partition_key),
                      "h5ad_path": dg.MetadataValue.path(path)},
            allow_retries=False,
        )
    try:
        adata = ad.read_h5ad(path)
    except Exception as exc:  # transient I/O on a valid HDF5 -> retriable
        raise dg.Failure(
            description=f"failed to read h5ad at {path!r}: {exc}",
            metadata={"error": dg.MetadataValue.text(repr(exc))},
        )

    try:
        res = stancounts.get_counts(adata)
    except stancounts.CountsUnavailable as exc:
        raise dg.Failure(
            description=f"no recoverable counts for {path!r}: {exc}",
            metadata={"h5ad_path": dg.MetadataValue.path(path)},
            allow_retries=False,
        )
    counts, counts_source = res["counts"], res["source"]

    qc = compute_count_qc(counts, adata.var_names)

    if qc["n_cells"] < curation.min_cells:
        raise dg.Failure(
            description=f"rejected: n_cells {qc['n_cells']} < min_cells {curation.min_cells}",
            metadata={"n_cells": dg.MetadataValue.int(qc["n_cells"]),
                      "min_cells": dg.MetadataValue.int(curation.min_cells)},
            allow_retries=False,
        )
    if qc["n_genes_detected"] < curation.min_genes:
        raise dg.Failure(
            description=f"rejected: n_genes_detected {qc['n_genes_detected']} < min_genes {curation.min_genes}",
            metadata={"n_genes_detected": dg.MetadataValue.int(qc["n_genes_detected"]),
                      "min_genes": dg.MetadataValue.int(curation.min_genes)},
            allow_retries=False,
        )

    out_path = output_path_for(curation.output_dir, context.partition_key, path)
    try:
        std = build_standardized_adata(adata, counts, source=counts_source)
        write_standardized(std, out_path)
    except Exception as exc:  # disk/write hiccup -> retriable
        raise dg.Failure(
            description=f"failed to write standardized h5ad to {out_path!r}: {exc}",
            metadata={"error": dg.MetadataValue.text(repr(exc))},
        )

    try:
        from sc_curation_pipeline.defs.plots import render_qc_panel
        pc = qc["per_cell"]
        qc_plots = dg.MetadataValue.md(render_qc_panel(
            pc["counts"], pc["genes"], pc["mito_pct"], sample_label=context.partition_key))
    except Exception as exc:  # noqa: BLE001 - plotting is non-fatal
        context.log.warning(f"QC plot rendering failed: {exc!r}")
        qc_plots = dg.MetadataValue.md(f"⚠️ 图未生成: {exc}")

    yield dg.MaterializeResult(
        metadata={
            "output_path": dg.MetadataValue.path(out_path),
            "counts_source": dg.MetadataValue.text(counts_source),
            "source_h5ad": dg.MetadataValue.path(path),
            "qc_plots": qc_plots,
            "n_cells": dg.MetadataValue.int(qc["n_cells"]),
            "n_genes_detected": dg.MetadataValue.int(qc["n_genes_detected"]),
            "n_vars": dg.MetadataValue.int(qc["n_vars"]),
            "total_counts": dg.MetadataValue.float(qc["total_counts"]),
            "median_counts_per_cell": dg.MetadataValue.float(qc["median_counts_per_cell"]),
            "median_genes_per_cell": dg.MetadataValue.float(qc["median_genes_per_cell"]),
            "density": dg.MetadataValue.float(qc["density"]),
            "sparsity": dg.MetadataValue.float(qc["sparsity"]),
            "mito_pct": dg.MetadataValue.float(qc["mito_pct"]),
            "ribo_pct": dg.MetadataValue.float(qc["ribo_pct"]),
            "layers": dg.MetadataValue.json(list(std.layers.keys())),
            "obsm": dg.MetadataValue.json(list(std.obsm.keys())),
        }
    )


h5ad_qc_job = dg.define_asset_job(
    name="h5ad_qc_job",
    selection=dg.AssetSelection.assets("h5ad_qc"),
)

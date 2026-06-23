"""Downstream cell-level filtering asset: keep cells with enough detected genes."""

import dagster as dg
import anndata as ad

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.settings import CurationSettings
from sc_curation_pipeline.defs.qc import resolve_h5ad_path, output_path_for
from sc_curation_pipeline.defs.standardize import write_standardized
from sc_curation_pipeline.defs.filter_cells import filter_cells_by_genes, filtered_path_for


@dg.asset(
    partitions_def=h5ad_partitions,
    group_name="curation",
    deps=["standardized_h5ad"],
    retry_policy=dg.RetryPolicy(max_retries=2),
)
def initially_filtered_h5ad(context: dg.AssetExecutionContext, curation: CurationSettings):
    """Filter cells by detected genes on the standardized .h5ad; write *_filtered.h5ad.

    Reads the upstream standardized file (written by standardized_h5ad), keeps cells whose
    counts layer has >= min_genes_per_cell detected genes, and writes a separate
    *_filtered.h5ad to the same OUTPUT_DIR. The upstream full-cell file is kept.
    """
    src = resolve_h5ad_path(context, curation)
    standardized = output_path_for(curation.output_dir, context.partition_key, src)
    try:
        adata = ad.read_h5ad(standardized)
    except Exception as exc:  # missing/transient -> retriable
        raise dg.Failure(
            description=f"failed to read standardized h5ad at {standardized!r}: {exc}",
            metadata={"standardized": dg.MetadataValue.path(standardized),
                      "error": dg.MetadataValue.text(repr(exc))},
        )

    if "counts" not in adata.layers:
        raise dg.Failure(
            description=f"standardized h5ad has no 'counts' layer: {standardized!r}",
            metadata={"standardized": dg.MetadataValue.path(standardized)},
            allow_retries=False,
        )

    filtered, n_before, n_after = filter_cells_by_genes(adata, curation.min_genes_per_cell)

    if n_after < curation.min_cells:
        raise dg.Failure(
            description=(
                f"rejected: cells after filter {n_after} < min_cells {curation.min_cells} "
                f"(min_genes_per_cell={curation.min_genes_per_cell})"
            ),
            metadata={"n_cells_before": dg.MetadataValue.int(n_before),
                      "n_cells_after": dg.MetadataValue.int(n_after),
                      "min_cells": dg.MetadataValue.int(curation.min_cells)},
            allow_retries=False,
        )

    out_path = filtered_path_for(standardized)
    try:
        write_standardized(filtered, out_path)
    except Exception as exc:  # disk hiccup -> retriable
        raise dg.Failure(
            description=f"failed to write filtered h5ad to {out_path!r}: {exc}",
            metadata={"error": dg.MetadataValue.text(repr(exc))},
        )

    yield dg.MaterializeResult(
        metadata={
            "filtered_output_path": dg.MetadataValue.path(out_path),
            "source_standardized": dg.MetadataValue.path(standardized),
            "min_genes_per_cell": dg.MetadataValue.int(curation.min_genes_per_cell),
            "n_cells_before": dg.MetadataValue.int(n_before),
            "n_cells_after": dg.MetadataValue.int(n_after),
            "n_cells_removed": dg.MetadataValue.int(n_before - n_after),
            # AnnData's own repr (what print(adata) shows) — shape + obs/var/uns/obsm/layers.
            "adata_info": dg.MetadataValue.md(f"```\n{filtered}\n```"),
        }
    )

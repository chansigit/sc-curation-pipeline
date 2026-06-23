"""Terminal asset: per-sample Scrublet doublet scores, written back onto the
filtered .h5ad's obs.

Scrublet simulates doublets by combining random pairs of observed transcriptomes,
so it must run *within* a sample (mixing samples invents cross-sample "doublets").
When the metacols-identified ``sample`` column is present we run Scrublet once per
sample; otherwise on the whole dataset. A failure in one group (too few cells, a
degenerate matrix) is non-fatal — that group's cells get ``doublet_score = NaN``
and ``predicted_doublet = False`` — so one bad sample never sinks the rest.
"""

import dagster as dg
import anndata as ad
import numpy as np
import scanpy as sc

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.settings import CurationSettings
from sc_curation_pipeline.defs.qc import resolve_h5ad_path, output_path_for
from sc_curation_pipeline.defs.standardize import write_standardized
from sc_curation_pipeline.defs.filter_cells import filtered_path_for

SAMPLE_KEY = "sample"


def compute_doublet_scores(adata, *, sample_key: str = SAMPLE_KEY, random_state: int = 0):
    """Per-sample Scrublet doublet scores computed on ``adata.layers["counts"]``.

    Returns ``(scores, predicted, failed_groups)``: ``scores`` is float64[n_obs]
    (NaN where Scrublet could not score), ``predicted`` is bool[n_obs] (False where
    unavailable), ``failed_groups`` lists the sample labels Scrublet raised on.
    Runs once per ``sample_key`` group when that obs column exists, else on the whole
    dataset. Never raises; never mutates ``adata``.
    """
    counts = adata.layers["counts"]
    n = int(adata.n_obs)
    scores = np.full(n, np.nan, dtype=np.float64)
    predicted = np.zeros(n, dtype=bool)

    if sample_key in adata.obs.columns:
        groups = adata.obs.groupby(sample_key, observed=True).indices  # label -> positions
    else:
        groups = {None: np.arange(n)}

    failed = []
    for label, idx in groups.items():
        idx = np.asarray(idx)
        try:
            sub = ad.AnnData(X=counts[idx].copy())
            sc.pp.scrublet(sub, random_state=random_state)
            scores[idx] = np.asarray(sub.obs["doublet_score"], dtype=np.float64)
            # predicted_doublet may be NaN when Scrublet can't auto-set a threshold.
            p = np.asarray(sub.obs["predicted_doublet"], dtype=np.float64)
            predicted[idx] = np.nan_to_num(p, nan=0.0).astype(bool)
        except Exception:  # noqa: BLE001 - per-group failure is non-fatal (NaN/False)
            failed.append(label)
    return scores, predicted, failed


@dg.asset(
    partitions_def=h5ad_partitions,
    group_name="curation",
    deps=["initially_filtered_h5ad"],
    retry_policy=dg.RetryPolicy(max_retries=2),
)
def doublet_scored_h5ad(context: dg.AssetExecutionContext, curation: CurationSettings):
    """Add per-sample Scrublet doublet scores to the filtered .h5ad (rewritten in place).

    Reads ``*_filtered.h5ad`` (from initially_filtered_h5ad), runs Scrublet on the
    counts layer per ``sample`` (when identified) else whole-dataset, writes
    ``obs["doublet_score"]`` / ``obs["predicted_doublet"]``, and overwrites the file.
    """
    src = resolve_h5ad_path(context, curation)
    standardized = output_path_for(curation.output_dir, context.partition_key, src)
    filtered = filtered_path_for(standardized)
    try:
        adata = ad.read_h5ad(filtered)
    except Exception as exc:  # missing/transient -> retriable
        raise dg.Failure(
            description=f"failed to read filtered h5ad at {filtered!r}: {exc}",
            metadata={"filtered": dg.MetadataValue.path(filtered),
                      "error": dg.MetadataValue.text(repr(exc))},
        )
    if "counts" not in adata.layers:
        raise dg.Failure(
            description=f"filtered h5ad has no 'counts' layer: {filtered!r}",
            metadata={"filtered": dg.MetadataValue.path(filtered)},
            allow_retries=False,
        )

    has_sample = SAMPLE_KEY in adata.obs.columns
    scores, predicted, failed = compute_doublet_scores(adata, sample_key=SAMPLE_KEY)
    adata.obs["doublet_score"] = scores
    adata.obs["predicted_doublet"] = predicted
    if failed:
        context.log.warning(f"Scrublet failed (NaN) for {len(failed)} sample(s): {failed}")

    try:
        write_standardized(adata, filtered)  # overwrite in place
    except Exception as exc:  # disk hiccup -> retriable
        raise dg.Failure(
            description=f"failed to rewrite filtered h5ad with doublet scores to {filtered!r}: {exc}",
            metadata={"error": dg.MetadataValue.text(repr(exc))},
        )

    n_scored = int(np.isfinite(scores).sum())
    n_pred = int(predicted.sum())
    yield dg.MaterializeResult(
        metadata={
            "filtered_output_path": dg.MetadataValue.path(filtered),
            "batch_key": dg.MetadataValue.text(SAMPLE_KEY if has_sample else "—"),
            "n_cells": dg.MetadataValue.int(int(adata.n_obs)),
            "n_scored": dg.MetadataValue.int(n_scored),
            "n_predicted_doublets": dg.MetadataValue.int(n_pred),
            "doublet_rate": dg.MetadataValue.float(n_pred / n_scored if n_scored else 0.0),
            "n_failed_samples": dg.MetadataValue.int(len(failed)),
            "adata_info": dg.MetadataValue.md(f"```\n{adata}\n```"),
        }
    )

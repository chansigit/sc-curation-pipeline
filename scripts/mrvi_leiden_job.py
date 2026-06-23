"""External Dagster Pipes job: MrVI (torch) + Leiden on a *_filtered.h5ad, in place.

Launched by PipesSlurmClient via sbatch on a GPU node. Reads the filtered path and
params from Pipes extras, trains MrVI, takes the u latent, Leiden-clusters it,
writes obsm["X_mrvi_u"] + obs["mrvi_leiden"] back into the same file, and reports
metadata via Pipes. Runs under the dl2025 venv python (which has the editable
sc_curation_pipeline + scvi/scanpy installed).
"""

import anndata
import torch
from dagster_pipes import open_dagster_pipes

from sc_curation_pipeline.defs.mrvi_compute import (
    LATENT_KEY,
    SAMPLE_KEY,
    leiden_on_rep,
    train_mrvi_u_latent,
)


def main() -> None:
    with open_dagster_pipes() as pipes:
        path = pipes.get_extra("filtered_path")
        resolution = float(pipes.get_extra("leiden_resolution"))
        raw_epochs = pipes.get_extra("max_epochs")
        max_epochs = int(raw_epochs) if raw_epochs else None  # 0/None -> scvi default
        raw_hvg = pipes.get_extra("n_hvg")
        n_hvg = int(raw_hvg) if raw_hvg else 0  # 0 -> train on all genes

        adata = anndata.read_h5ad(path)
        had_sample = SAMPLE_KEY in adata.obs.columns
        n_genes_trained = n_hvg if (n_hvg and adata.n_vars > n_hvg) else int(adata.n_vars)
        pipes.log.info(
            f"loaded {adata.shape} from {path}; sample column "
            f"{'present' if had_sample else 'absent -> single-sample fallback'}; "
            f"training MrVI on {n_genes_trained}/{adata.n_vars} genes (n_hvg={n_hvg or 'all'})"
        )

        adata.obsm[LATENT_KEY] = train_mrvi_u_latent(adata, n_hvg=n_hvg, max_epochs=max_epochs)
        n_clusters = leiden_on_rep(adata, resolution=resolution)
        adata.write_h5ad(path)  # in-place rewrite

        pipes.report_asset_materialization(
            metadata={
                "n_cells": int(adata.n_obs),
                "n_samples": int(adata.obs[SAMPLE_KEY].nunique()),
                "n_clusters": n_clusters,
                "latent_dim": int(adata.obsm[LATENT_KEY].shape[1]),
                "n_genes_total": int(adata.n_vars),
                "n_genes_trained": n_genes_trained,
                "n_hvg": n_hvg if n_hvg else "all",
                "leiden_resolution": resolution,
                "max_epochs": max_epochs if max_epochs is not None else "scvi-default",
                "had_sample_column": had_sample,
                "accelerator": "gpu" if torch.cuda.is_available() else "cpu",
                "filtered_path": path,
            }
        )


if __name__ == "__main__":
    main()

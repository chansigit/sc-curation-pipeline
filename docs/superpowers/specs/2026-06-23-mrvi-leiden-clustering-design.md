# MrVI + Leiden clustering asset (`mrvi_leiden_h5ad`) — design

Date: 2026-06-23

## Goal

Add a terminal Dagster asset `mrvi_leiden_h5ad` that, for each curated sample,
trains an **MrVI** model (sample-aware deep generative model), takes its **u
latent** (sample-corrected cell-state representation), and runs **Leiden**
clustering on that latent. Because MrVI is GPU-trained, the work is **offloaded to
a Slurm GPU job via Dagster Pipes** rather than run in the Dagster process.

## Context / position in the pipeline

```
standardized_h5ad → initially_filtered_h5ad → doublet_scored_h5ad → mrvi_leiden_h5ad (NEW terminal)
```

`mrvi_leiden_h5ad` `deps=["doublet_scored_h5ad"]`, reads the in-place-augmented
`*_filtered.h5ad` (which already has `counts` layer, metacols-normalized `sample`
column when identified, and doublet columns), and writes the clustering back into
the **same** `*_filtered.h5ad`.

## Locked decisions (from brainstorming)

- **Compute**: Dagster Pipes → Slurm GPU job (NOT in-process). A custom Slurm Pipes
  client is in scope (Dagster has no built-in Slurm client).
- **Backend**: MrVI **torch** backend (`setup_anndata(..., backend="torch")` →
  `TorchMRVI`; torch 2.6+cu124 provides CUDA). jax is not used.
- **Latent**: the **u** latent — `get_latent_representation(give_z=False)`.
- **No `sample` column** → **single-sample fallback** (set a constant `sample`
  column so MrVI runs; it degenerates to scVI-like). Always produces output.
- **Cell scope / output**: run on **all cells** (doublets only annotated, not
  removed) → write `obsm["X_mrvi_u"]` + `obs["mrvi_leiden"]` back **in place** to
  `*_filtered.h5ad` (cell count unchanged, so in-place rewrite is valid).
- **Asset name**: `mrvi_leiden_h5ad`.
- **Blocking poll**: the asset blocks while polling `squeue`/`sacct` — accepted.
- **sbatch resources are env-configurable**. Partition MUST be a batch-capable GPU
  partition (`gpu`). NOTE (corrected during end-to-end testing): `dev` is
  interactive-only and rejects `sbatch` ("Batch jobs are not allowed in the 'dev'
  partition"), so it is NOT usable with this sbatch-based client.

## Architecture (3 components)

```
mrvi_leiden_h5ad asset  (orchestration; runs in dg dev / CPU node)
  ├─ resolve *_filtered.h5ad path (resolve_h5ad_path + output_path_for + filtered_path_for)
  ├─ open a Slurm Pipes session: file context-injector + file message-reader on shared scratch
  ├─ render + `sbatch` a GPU job that runs scripts/mrvi_leiden_job.py with the Pipes bootstrap env
  ├─ poll `squeue`/`sacct` (~30–60s cadence, not tight) until terminal state
  └─ read back Pipes materialization metadata; non-zero / FAILED job → dagster.Failure
        │  (sbatch -p <partition> -G 1 ...)
        ▼
scripts/mrvi_leiden_job.py  (external; runs on the GPU node)
  with open_dagster_pipes() as pipes:
    path = pipes.extras["filtered_path"]; res = pipes.extras["leiden_resolution"]; ...
    adata = anndata.read_h5ad(path)
    sample_key = "sample" if "sample" in adata.obs else <constant dummy>
    mask = highly_variable_genes(seurat_v3, n_top_genes=<n_hvg>, layer="counts",
                                 batch_key=sample_key, inplace=False)         # HVG on raw counts
    train_adata = adata[:, mask].copy()                                       # subset COPY; adata kept whole
    MRVI.setup_anndata(train_adata, layer="counts", sample_key=sample_key, backend="torch")
    model = MRVI(train_adata); model.train(accelerator="auto", max_epochs=<cfg>)
    adata.obsm["X_mrvi_u"] = model.get_latent_representation(give_z=False)   # u latent (back onto full-gene adata)
    sc.pp.neighbors(adata, use_rep="X_mrvi_u")
    sc.tl.leiden(adata, resolution=res, key_added="mrvi_leiden", flavor="igraph", n_iterations=2)
    adata.write_h5ad(path)                                                   # in-place
    pipes.report_asset_materialization(metadata={...})
```

### A. `mrvi_leiden_h5ad` asset (`defs/mrvi.py`)

- `@dg.asset(partitions_def=h5ad_partitions, group_name="curation",
  deps=["doublet_scored_h5ad"], retry_policy=RetryPolicy(max_retries=2))`.
- Resolves the filtered path, builds the sbatch params from `CurationSettings`,
  invokes the Slurm Pipes client, and `yield from` / returns its
  `PipesClientCompletedInvocation.get_materialize_result()` so the external
  job's reported metadata becomes this asset's materialization metadata.

### B. Slurm Pipes client (`defs/slurm_pipes.py`)

A small `PipesClient` for Sherlock Slurm:

- Uses `dagster.open_pipes_session(context, context_injector, message_reader)`.
- **context_injector**: file-based (e.g. `PipesFileContextInjector`) — writes the
  Pipes context to a file on shared scratch; the external process reads it via the
  bootstrap env var.
- **message_reader**: file-based (e.g. `PipesFileMessageReader`) on shared scratch
  — the external job writes Pipes messages there; the client reads them.
  (Exact Dagster class names verified at implementation; the requirement is a
  shared-filesystem channel that survives the sbatch boundary.)
- Renders an sbatch script: `#SBATCH -p <partition> -G 1 --time=<t> --mem=<m>
  --cpus-per-task=<c>` (+ optional `-C <gpu_constraint>`), exports the Pipes
  bootstrap env vars (`session.get_bootstrap_env_vars()`), then runs
  `<dl2025 python> scripts/mrvi_leiden_job.py`.
- Submits via `subprocess.run(["sbatch", ...])`, parses the job id, polls
  `sacct -j <id> --format=State --noheader` (fallback `squeue`) on a ~30–60s
  cadence until a terminal state (COMPLETED / FAILED / CANCELLED / TIMEOUT).
- Returns `PipesClientCompletedInvocation`; a non-COMPLETED state raises so the
  asset fails (retriable via the asset's RetryPolicy).

### C. External job script (`scripts/mrvi_leiden_job.py`)

Imports the pure compute helpers from `defs/mrvi_compute.py` (`select_hvg_mask`,
`train_mrvi_u_latent`, `leiden_on_rep`) — shared with the unit tests for testability;
otherwise needs only `scvi`, `scanpy`, `anndata`, `dagster_pipes`. Runs under the
dl2025 venv python (which has the editable package installed). HVG selection
(seurat_v3) trains MrVI on a gene-subset copy; the written-back h5ad keeps all genes.
Reproducibility: `scvi.settings.seed = 0`. Reports metadata: `n_cells`, `n_samples`,
`n_clusters` (unique leiden), `latent_dim`, `n_genes_total`, `n_genes_trained`,
`n_hvg`, `leiden_resolution`, `max_epochs`, `accelerator`, `had_sample_column`.

## Configuration (`CurationSettings`, env-driven)

| env | default | meaning |
|---|---|---|
| `SC_CURATION_MRVI_PARTITION` | `gpu` | sbatch `-p`; must be batch-capable + GPU (`gpu`). NOT `dev` (interactive-only, rejects sbatch) |
| `SC_CURATION_MRVI_TIME` | `01:00:00` | sbatch `--time` (short → faster scheduling) |
| `SC_CURATION_MRVI_CPUS` | `4` | `--cpus-per-task` |
| `SC_CURATION_MRVI_MEM` | `32GB` | `--mem` (system RAM) |
| `SC_CURATION_MRVI_GPU_CONSTRAINT` | `` (empty) | optional `-C` (e.g. `GPU_MEM:24GB`); empty = no constraint = fastest scheduling |
| `SC_CURATION_MRVI_N_HVG` | `2000` | top-N HVGs (seurat_v3, batch_key=sample) for training; `0` = all genes. Output h5ad keeps all genes regardless |
| `SC_CURATION_MRVI_MAX_EPOCHS` | `` (empty → scvi default) | MrVI training epochs |
| `SC_CURATION_LEIDEN_RESOLUTION` | `1.0` | Leiden resolution |

`-G 1` is fixed. The Slurm Pipes client never auto-picks the partition by data
size (kept simple/predictable; the user tunes via env).

## Outputs

Written in place into `*_filtered.h5ad`:
- `obsm["X_mrvi_u"]` — the MrVI u latent (n_obs × latent_dim).
- `obs["mrvi_leiden"]` — Leiden cluster labels (categorical).
- neighbors graph artifacts (`obsp`/`uns`) as scanpy writes them.

## Wiring / integration

- `registration.py`: add `mrvi_leiden_h5ad` to `assets`.
- `standardized_h5ad_job` selection: add `"mrvi_leiden_h5ad"`.
- `sensors.py`: `_TERMINAL_ASSET = AssetKey("mrvi_leiden_h5ad")` (new terminal; a
  sample is "done" only after clustering completes).
- README: add the MrVI/Leiden section + the new obs/obsm + the sbatch env table.
- Existing samples (e.g. test2): after this lands they are "not done" (mrvi not
  materialized); backfill the `mrvi_leiden_h5ad` step alone (reads the existing
  `*_filtered.h5ad`), no re-run of the upstream chain.

## Error handling

- Slurm job FAILED/TIMEOUT/CANCELLED → `dagster.Failure` (retriable via RetryPolicy).
  Unlike metacols (nice-to-have, non-fatal), clustering is the asset's product, so
  a failure should surface as a red run.
- sbatch submission error (bad partition, quota) → `dagster.Failure` (allow_retries
  controlled by whether it looks transient).

## Testing

- **Pure / unit (CI, no Slurm/GPU)**:
  - sbatch script **rendering**: params → expected `#SBATCH` lines.
  - `sacct`/`squeue` **state parsing**: fake CLI output → running/done/failed.
  - Slurm Pipes client flow with `subprocess` **mocked** (sbatch returns a job id;
    sacct returns COMPLETED) + a fake message file → client completes.
  - `run_leiden_on_rep(adata, rep_key, resolution)` on a synthetic `obsm` → adds
    `mrvi_leiden`.
- **Slow / optional**: a real TorchMRVI train on tiny synthetic data
  (`max_epochs=1`, CPU) exercising the external script's core (no Slurm/Pipes).
- **End-to-end** (Slurm + GPU): manual, on test2 (cannot run in CI).

## Risks / open items

- Exact Dagster Pipes file-based class names (`PipesFileContextInjector` /
  `PipesFileMessageReader`) to be confirmed at implementation; fall back to the
  documented shared-FS Pipes pattern if names differ.
- MrVI with a single (dummy) sample: confirm `TorchMRVI` trains with one sample
  category (degenerate but should run); else handle.
- ~~`dev` partition for small/fast-queue runs~~ — RESOLVED during testing: `dev`
  rejects `sbatch` (interactive-only), so it cannot be used by this client. Use
  `gpu` (the only batch-capable GPU partition for this user; no `owners` GPU
  partition available). Short `--time` + 1 GPU backfills reasonably even when the
  `gpu` queue is deep.
- GPU memory for large samples: the optional `-C GPU_MEM` knob + partition choice
  cover this; default leaves it unconstrained for fastest scheduling.

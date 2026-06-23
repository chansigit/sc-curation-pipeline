import glob
import json
import os

import dagster as dg
import numpy as np
import scipy.sparse as sp
import anndata as ad
import h5py

import stancounts
import stangene

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.settings import CurationSettings, path_for_partition_key
from sc_curation_pipeline.defs.standardize import build_standardized_adata, write_standardized
from sc_curation_pipeline.defs.harmonize_apply import apply_harmonization

H5AD_PATH_TAG = "sc/h5ad_path"
SPECIES_TAG = "sc/species"
SPECIES_MARKER_PREFIX = ".species."


def count_n_genes_detected(counts) -> int:
    """Number of genes with counts>0 in >=1 cell (name-independent; cheap gate)."""
    if sp.issparse(counts):
        return int((np.asarray(counts.tocsr().getnnz(axis=0)).ravel() > 0).sum())
    C = np.asarray(counts)
    return int(((C != 0).sum(axis=0) > 0).sum())


def _masked_sum_per_cell(C, mask, is_sparse, n_cells) -> np.ndarray:
    """Per-cell summed counts over the columns selected by ``mask`` (0s if empty)."""
    if not mask.any():
        return np.zeros(n_cells)
    if is_sparse:
        return np.asarray(C[:, mask].sum(axis=1)).ravel()
    return C[:, mask].sum(axis=1)


def compute_count_qc(counts, var_names, species=None) -> dict:
    """QC metrics computed on an in-memory counts matrix (sparse or dense).

    ``species`` (a stangene code/name) selects species-aware mitochondrial and
    hemoglobin gene detection via ``stangene.mito_mask`` / ``stangene.hb_mask``.
    Without a species, mito falls back to a generic ``MT-`` prefix and hb is left
    empty (hemoglobin symbols are too species-specific for a generic rule). Both
    fractions are reported per cell (for obs + the QC plot) and as medians.
    """
    is_sparse = sp.issparse(counts)
    C = counts.tocsr() if is_sparse else np.asarray(counts)
    n_cells, n_vars = int(C.shape[0]), int(C.shape[1])

    if species:
        mt_mask = stangene.mito_mask(var_names, species)
        hb_mask = stangene.hb_mask(var_names, species)
    else:
        up = np.char.upper(np.asarray([str(v) for v in var_names], dtype=str))
        mt_mask = np.char.startswith(up, "MT-")
        hb_mask = np.zeros(len(up), dtype=bool)  # no generic hemoglobin set without a species

    if is_sparse:
        counts_per_cell = np.asarray(C.sum(axis=1)).ravel().astype(np.float64)
        genes_per_cell = C.getnnz(axis=1).astype(np.float64)
        nnz_total = int(C.nnz)
    else:
        counts_per_cell = C.sum(axis=1).astype(np.float64)
        genes_per_cell = (C != 0).sum(axis=1).astype(np.float64)
        nnz_total = int((C != 0).sum())
    mito_per_cell = _masked_sum_per_cell(C, mt_mask, is_sparse, n_cells)
    hb_per_cell = _masked_sum_per_cell(C, hb_mask, is_sparse, n_cells)

    total = n_cells * n_vars
    total_counts = float(counts_per_cell.sum())
    n_genes_detected = count_n_genes_detected(C)
    with np.errstate(divide="ignore", invalid="ignore"):
        mito_pct_per_cell = np.where(counts_per_cell > 0,
                                     100.0 * mito_per_cell / counts_per_cell, 0.0)
        hb_pct_per_cell = np.where(counts_per_cell > 0,
                                   100.0 * hb_per_cell / counts_per_cell, 0.0)
    return {
        "n_cells": n_cells,
        "n_vars": n_vars,
        "n_genes_detected": n_genes_detected,
        "total_counts": total_counts,
        "median_counts_per_cell": float(np.median(counts_per_cell)) if n_cells else 0.0,
        "median_genes_per_cell": float(np.median(genes_per_cell)) if n_cells else 0.0,
        "median_pct_counts_mt": float(np.median(mito_pct_per_cell)) if n_cells else 0.0,
        "median_pct_counts_hb": float(np.median(hb_pct_per_cell)) if n_cells else 0.0,
        "sparsity": (1.0 - nnz_total / total) if total else 0.0,
        "per_cell": {"counts": counts_per_cell, "genes": genes_per_cell,
                     "mito_pct": mito_pct_per_cell, "hb_pct": hb_pct_per_cell},
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


def resolve_species_for(context, src_path) -> tuple[str, str]:
    """Resolve the sample's species from the sc/species tag, else the marker file.

    Returns (canonical_species, raw_code). Missing/ambiguous marker or an
    unknown code -> dg.Failure(allow_retries=False) (a typo/forgotten species
    file is a permanent problem, not a transient one).
    """
    code = (context.run.tags.get(SPECIES_TAG) or "").strip()
    if not code:
        # No tag (e.g. manual materialize): scan the sample folder for a single
        # .species.<code> marker.
        folder = os.path.dirname(src_path)
        try:
            codes = [
                f[len(SPECIES_MARKER_PREFIX):]
                for f in os.listdir(folder)
                if f.startswith(SPECIES_MARKER_PREFIX) and f[len(SPECIES_MARKER_PREFIX):]
            ]
        except OSError:
            codes = []
        code = codes[0].strip() if len(codes) == 1 else ""
    if not code:
        raise dg.Failure(
            description=(
                f"missing or ambiguous {SPECIES_MARKER_PREFIX}<code> marker for "
                f"{src_path!r}; declare the species, e.g. {SPECIES_MARKER_PREFIX}hs"
            ),
            metadata={"partition": dg.MetadataValue.text(context.partition_key)},
            allow_retries=False,
        )
    try:
        return stangene.resolve_species(code), code
    except ValueError as exc:
        raise dg.Failure(
            description=f"unknown species code for {src_path!r}: {exc}",
            metadata={
                "partition": dg.MetadataValue.text(context.partition_key),
                "species_code": dg.MetadataValue.text(code),
            },
            allow_retries=False,
        )


def _harmonization_stats(mapping_table) -> dict:
    """Mapping counts from a stangene mapping_table (gene features only)."""
    status = mapping_table["mapping_status"]
    gene = status != "non_gene_feature"
    mapped = gene & (status != "unmapped")
    n_gene = int(gene.sum())
    n_mapped = int(mapped.sum())
    return {
        "n_genes_mapped": n_mapped,
        "n_unmapped": int((status == "unmapped").sum()),
        "mapping_rate": (n_mapped / n_gene) if n_gene else 0.0,
    }


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
    # Resolve species early (cheap tag/marker lookup) so a missing/unknown
    # species marker fast-fails before the heavier counts/standardize work.
    species, species_code = resolve_species_for(context, path)

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

    # Hard gates first (cheap, name-independent) so sub-threshold samples reject
    # before the heavier harmonize/standardize/write work.
    n_cells = int(counts.shape[0])
    if n_cells < curation.min_cells:
        raise dg.Failure(
            description=f"rejected: n_cells {n_cells} < min_cells {curation.min_cells}",
            metadata={"n_cells": dg.MetadataValue.int(n_cells),
                      "min_cells": dg.MetadataValue.int(curation.min_cells)},
            allow_retries=False,
        )
    n_genes_detected = count_n_genes_detected(counts)
    if n_genes_detected < curation.min_genes:
        raise dg.Failure(
            description=f"rejected: n_genes_detected {n_genes_detected} < min_genes {curation.min_genes}",
            metadata={"n_genes_detected": dg.MetadataValue.int(n_genes_detected),
                      "min_genes": dg.MetadataValue.int(curation.min_genes)},
            allow_retries=False,
        )

    # Harmonize gene names to canonical symbols. get_counts already ran on the
    # original var_names (.raw alignment); renaming only relabels var, so the
    # counts matrix columns stay positionally aligned. Doing it before QC means
    # the QC (incl. MT- mito detection) and the written file use canonical names.
    harmon = stangene.harmonize_anndata(adata, species)
    apply_harmonization(adata, harmon)
    harmon_stats = _harmonization_stats(harmon.mapping_table)

    qc = compute_count_qc(counts, adata.var_names, species=species)
    # Per-cell contamination fractions onto obs (scanpy-style names) so they ride
    # into the written file — and, via the row subset, into downstream cell_filtered.
    # Row order matches: harmonization only relabels var, counts rows == adata.obs.
    adata.obs["pct_counts_mt"] = qc["per_cell"]["mito_pct"]
    adata.obs["pct_counts_hb"] = qc["per_cell"]["hb_pct"]

    # Identify standard metadata roles in obs (stanmetacols) and normalize confident
    # picks to canonical columns (sample / cell_type_*); record the full ranking in
    # uns. Non-fatal: a stanmetacols/LLM hiccup must never block the standardized
    # write (LLM path may stall on compute nodes without network).
    try:
        from sc_curation_pipeline.defs.metacols import identify_and_normalize, render_metacols_md
        metacols = identify_and_normalize(
            adata, use_llm=curation.metacols_use_llm,
            provider=curation.metacols_provider, model=curation.metacols_model,
            base_url=curation.metacols_base_url or None,
            api_key_env=curation.metacols_api_key_env or None)
        adata.uns["metacols"] = json.dumps(metacols)
        metacols_md = render_metacols_md(metacols)
    except Exception as exc:  # noqa: BLE001 - role identification is non-fatal
        context.log.warning(f"stanmetacols identification failed: {exc!r}")
        metacols = {"method": f"⚠️ failed: {exc}", "assigned": {}, "ranking": {}}
        metacols_md = f"⚠️ stanmetacols 未运行: {exc}"

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
            pc["counts"], pc["genes"], pc["mito_pct"], pc["hb_pct"],
            sample_label=context.partition_key))
    except Exception as exc:  # noqa: BLE001 - plotting is non-fatal
        context.log.warning(f"QC plot rendering failed: {exc!r}")
        qc_plots = dg.MetadataValue.md(f"⚠️ 图未生成: {exc}")

    yield dg.MaterializeResult(
        metadata={
            "output_path": dg.MetadataValue.path(out_path),
            "counts_source": dg.MetadataValue.text(counts_source),
            "source_h5ad": dg.MetadataValue.path(path),
            "species": dg.MetadataValue.text(species),
            "species_code": dg.MetadataValue.text(species_code),
            "harmonized": dg.MetadataValue.bool(True),
            "n_genes_mapped": dg.MetadataValue.int(harmon_stats["n_genes_mapped"]),
            "n_unmapped": dg.MetadataValue.int(harmon_stats["n_unmapped"]),
            "mapping_rate": dg.MetadataValue.float(harmon_stats["mapping_rate"]),
            "metacols_method": dg.MetadataValue.text(metacols["method"]),
            "metacols_result": dg.MetadataValue.md(metacols_md),
            "qc_plots": qc_plots,
            "n_cells": dg.MetadataValue.int(qc["n_cells"]),
            "n_genes_detected": dg.MetadataValue.int(qc["n_genes_detected"]),
            "n_vars": dg.MetadataValue.int(qc["n_vars"]),
            "total_counts": dg.MetadataValue.float(qc["total_counts"]),
            "median_counts_per_cell": dg.MetadataValue.float(qc["median_counts_per_cell"]),
            "median_genes_per_cell": dg.MetadataValue.float(qc["median_genes_per_cell"]),
            "median_pct_counts_mt": dg.MetadataValue.float(qc["median_pct_counts_mt"]),
            "median_pct_counts_hb": dg.MetadataValue.float(qc["median_pct_counts_hb"]),
            "sparsity": dg.MetadataValue.float(qc["sparsity"]),
            "layers": dg.MetadataValue.json(list(std.layers.keys())),
            "obsm": dg.MetadataValue.json(list(std.obsm.keys())),
        }
    )


h5ad_qc_job = dg.define_asset_job(
    name="h5ad_qc_job",
    # Each discovered sample runs the full chain in one run: h5ad_qc (standardize +
    # write) then its downstream cell_filtered (read the file, filter cells).
    selection=dg.AssetSelection.assets("h5ad_qc", "cell_filtered"),
)

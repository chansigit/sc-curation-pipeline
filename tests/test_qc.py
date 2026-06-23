import os

import dagster as dg
import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.qc import (
    compute_count_qc, standardized_h5ad, standardized_h5ad_job, output_path_for,
)
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for
from sc_curation_pipeline.defs.partitions import h5ad_partitions


def _counts(n=200, g=8000, seed=0):
    # n_genes_detected must be able to exceed default min_genes(5000) in pass tests
    rng = np.random.RandomState(seed)
    m = rng.poisson(0.5, size=(n, g)).astype(np.float64)
    return m


# ---- compute_count_qc ----

def test_compute_count_qc_dense():
    counts = np.array([[1.0, 0.0, 2.0], [0.0, 3.0, 0.0]])
    qc = compute_count_qc(counts, ["GENE0", "MT-CO1", "RPS3"])
    assert qc["n_cells"] == 2
    assert qc["n_vars"] == 3
    assert qc["n_genes_detected"] == 3  # all 3 genes seen in >=1 cell
    assert qc["total_counts"] == 6.0
    # raw per-cell mito/hb stay under per_cell; top-level exposes only medians.
    assert "mito_pct" not in qc and "ribo_pct" not in qc and "density" not in qc
    assert "sparsity" in qc
    # per-cell mito/hb are computed (obs + the QC plot use them).
    np.testing.assert_array_equal(qc["per_cell"]["counts"], [3.0, 3.0])
    np.testing.assert_allclose(qc["per_cell"]["mito_pct"], [0.0, 100.0])
    np.testing.assert_allclose(qc["per_cell"]["hb_pct"], [0.0, 0.0])  # no species -> no hb set
    # species-aware medians now reported as metadata numbers
    assert qc["median_pct_counts_mt"] == 50.0  # median([0, 100])
    assert qc["median_pct_counts_hb"] == 0.0


def test_compute_count_qc_species_aware_mito_fly():
    # Fruit-fly mito uses the mt: prefix; species-aware detection must catch it,
    # while the default (generic MT-) would miss it.
    counts = np.array([[1.0, 0.0, 4.0], [0.0, 3.0, 0.0]])
    var = ["Act5C", "mt:Cyt-b", "mt:CoI"]
    qc_fly = compute_count_qc(counts, var, species="dm")
    # cell0: mito = mt:CoI col = 4 of total 5 -> 80%; cell1: mt:Cyt-b 3 of 3 -> 100%
    np.testing.assert_allclose(qc_fly["per_cell"]["mito_pct"], [80.0, 100.0])
    # default path (no species) uses MT- prefix -> catches none of the mt: genes
    qc_default = compute_count_qc(counts, var)
    np.testing.assert_array_equal(qc_default["per_cell"]["mito_pct"], [0.0, 0.0])


def test_compute_count_qc_species_aware_hb():
    # Human hemoglobin (HBB/HBA1) is detected via stangene.hb_mask; the generic
    # no-species path has no hemoglobin set, so it catches none.
    counts = np.array([[1.0, 4.0, 0.0], [0.0, 0.0, 3.0]])
    var = ["ACTB", "HBB", "HBA1"]
    qc_hs = compute_count_qc(counts, var, species="hs")
    # cell0: HBB 4 of total 5 -> 80%; cell1: HBA1 3 of 3 -> 100%
    np.testing.assert_allclose(qc_hs["per_cell"]["hb_pct"], [80.0, 100.0])
    qc_default = compute_count_qc(counts, var)
    np.testing.assert_array_equal(qc_default["per_cell"]["hb_pct"], [0.0, 0.0])


def test_compute_count_qc_detected_excludes_allzero_genes():
    counts = np.array([[1.0, 0.0, 0.0], [2.0, 0.0, 0.0]])  # only gene0 detected
    qc = compute_count_qc(counts, ["G0", "G1", "G2"])
    assert qc["n_vars"] == 3
    assert qc["n_genes_detected"] == 1


# ---- asset helpers ----

def _materialize(path, watch, out, key, settings, instance, species="hs"):
    tags = {"sc/h5ad_path": path}
    if species is not None:
        tags["sc/species"] = species
    return dg.materialize(
        [standardized_h5ad], partition_key=key, instance=instance,
        resources={"curation": settings}, tags=tags,
        raise_on_error=False,
    )


def _setup(tmp_path, folder_name, X, write_adata, *, layers=None, min_cells=100, min_genes=5000, var_names=None, obs=None):
    watch = str(tmp_path / "watch"); out = str(tmp_path / "out")
    folder = os.path.join(watch, folder_name)
    path = write_adata(os.path.join(folder, "a.h5ad"), X, var_names=var_names, layers=layers, obs=obs)
    key = partition_key_for(watch, folder)
    # metacols_use_llm=False -> deterministic offline heuristic (no network/key) in tests
    settings = CurationSettings(watch_dir=watch, output_dir=out, min_cells=min_cells,
                                min_genes=min_genes, metacols_use_llm=False)
    inst = dg.DagsterInstance.ephemeral()
    inst.add_dynamic_partitions(h5ad_partitions.name, [key])
    return watch, out, folder, path, key, settings, inst


def test_standardized_h5ad_writes_output_and_qc(tmp_path, write_adata):
    counts = _counts()
    lognorm = sp.csr_matrix(np.log1p(counts / counts.sum(1, keepdims=True) * 1e4))
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "GSE1_s", lognorm, write_adata, layers={"counts": sp.csr_matrix(counts)})
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success

    md = res.asset_materializations_for_node("standardized_h5ad")[0].metadata
    assert md["n_cells"].value == 200
    assert "data:image/png;base64," in md["qc_plots"].value
    assert md["counts_source"].value == "layer:counts"
    assert md["species"].value == "human"            # .species.hs -> human
    assert md["harmonized"].value is True
    assert "mapping_rate" in md
    assert md["median_pct_counts_mt"].value == 0.0   # GENE* names -> no mito/hb
    assert md["median_pct_counts_hb"].value == 0.0
    out_path = output_path_for(out, key, path)
    assert os.path.isfile(out_path)              # written to output dir
    assert os.path.isfile(path)                  # source still present, untouched
    # per-cell contamination fractions land in the written file's obs
    import anndata
    back = anndata.read_h5ad(out_path)
    assert "pct_counts_mt" in back.obs.columns and "pct_counts_hb" in back.obs.columns


def test_standardized_h5ad_renames_var_to_canonical_symbols(tmp_path, write_adata):
    import anndata
    # Real human symbols: TP53 (exact), p53 (alias -> TP53), plus an unmapped one.
    names = ["TP53", "p53", "FOOBAR_NOTAGENE"]
    counts = np.ones((5, 3), dtype=np.float64)
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "human_s", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)}, var_names=names,
        min_cells=1, min_genes=1)
    res = _materialize(path, watch, out, key, settings, inst, species="hs")
    assert res.success, res

    md = res.asset_materializations_for_node("standardized_h5ad")[0].metadata
    assert md["species"].value == "human"
    assert md["n_genes_mapped"].value >= 2  # TP53 + p53 both resolve to TP53

    back = anndata.read_h5ad(output_path_for(out, key, path))
    # TP53 appears (twice from TP53 + p53 -> made unique); unmapped kept as-is
    assert any(n.startswith("TP53") for n in back.var_names)
    assert "FOOBAR_NOTAGENE" in list(back.var_names)
    assert "original_feature_name" in back.var.columns
    assert list(back.var["original_feature_name"]) == names


def test_standardized_h5ad_normalizes_metacols(tmp_path, write_adata):
    import json
    import anndata
    counts = _counts()                       # 200 cells, 8000 genes (passes gates)
    n = counts.shape[0]
    lognorm = sp.csr_matrix(np.log1p(counts / counts.sum(1, keepdims=True) * 1e4))
    rng = np.random.default_rng(1)
    cell_type = rng.choice(["T cell", "B cell", "NK cell", "Monocyte"], size=n)
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "metacols_s", lognorm, write_adata,
        layers={"counts": sp.csr_matrix(counts)}, obs={"cell_type": list(cell_type)})
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success, res

    md = res.asset_materializations_for_node("standardized_h5ad")[0].metadata
    assert md["metacols_method"].value == "heuristic"     # _setup forces use_llm=False
    # the parse result is rendered as a markdown table listing every role
    result_md = md["metacols_result"].value
    assert "cell_type_coarse" in result_md and "✅ `cell_type`" in result_md
    assert "`organ`" in result_md and "`tissue`" in result_md   # new roles shown

    back = anndata.read_h5ad(output_path_for(out, key, path))
    assert "cell_type_coarse" in back.obs.columns         # normalized canonical column
    meta = json.loads(back.uns["metacols"])               # full ranking recorded in uns
    assert meta["assigned"].get("cell_type_coarse") == "cell_type"
    # a lone cell-type column is NOT duplicated into the fine role
    assert "cell_type_fine" not in meta["assigned"]
    assert "cell_type_fine" not in back.obs.columns


def test_standardized_h5ad_metacols_failure_non_fatal(tmp_path, write_adata, monkeypatch):
    from sc_curation_pipeline.defs import metacols as mcmod
    counts = _counts()
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "mcfail", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)})

    def _boom(*a, **k):
        raise RuntimeError("metacols boom")
    monkeypatch.setattr(mcmod, "identify_and_normalize", _boom)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is True                            # write must still succeed
    md = res.asset_materializations_for_node("standardized_h5ad")[0].metadata
    assert "failed" in md["metacols_method"].value        # method records the failure
    assert os.path.isfile(output_path_for(out, key, path))  # output still written


def test_standardized_h5ad_missing_species_fast_fail(tmp_path, write_adata):
    counts = _counts()
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "nospecies", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)})
    # no sc/species tag AND no .species.* marker in the folder -> fast-fail
    res = _materialize(path, watch, out, key, settings, inst, species=None)
    assert res.success is False
    msg = [e for e in res.get_step_failure_events() if e.step_key == "standardized_h5ad"][0].event_specific_data.error.message
    assert ".species." in msg
    assert not os.path.isfile(output_path_for(out, key, path))


def test_standardized_h5ad_rejects_too_few_cells(tmp_path, write_adata):
    counts = _counts(n=10)  # < min_cells 100
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "tiny", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)}, min_cells=100, min_genes=1)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    msg = [e for e in res.get_step_failure_events() if e.step_key == "standardized_h5ad"][0].event_specific_data.error.message
    assert "min_cells" in msg
    assert not os.path.isfile(output_path_for(out, key, path))  # no output


def test_standardized_h5ad_rejects_too_few_genes(tmp_path, write_adata):
    counts = _counts(n=200, g=100)  # only 100 genes -> < min_genes 5000
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "fewgenes", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)}, min_cells=1, min_genes=5000)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    msg = [e for e in res.get_step_failure_events() if e.step_key == "standardized_h5ad"][0].event_specific_data.error.message
    assert "min_genes" in msg
    assert not os.path.isfile(output_path_for(out, key, path))


def test_standardized_h5ad_no_counts_fails(tmp_path, write_adata):
    rng = np.random.RandomState(0)
    floats = rng.uniform(0.1, 5.0, size=(200, 100))  # not integer, not log1p
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "nocounts", floats, write_adata, min_cells=1, min_genes=1)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    assert not os.path.isfile(output_path_for(out, key, path))


def test_standardized_h5ad_corrupt_fast_fail(tmp_path):
    watch = str(tmp_path / "watch"); out = str(tmp_path / "out")
    folder = os.path.join(watch, "bad"); os.makedirs(folder)
    path = os.path.join(folder, "broken.h5ad")
    with open(path, "wb") as fh:
        fh.write(b"not an hdf5 file")
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch, output_dir=out)
    inst = dg.DagsterInstance.ephemeral()
    inst.add_dynamic_partitions(h5ad_partitions.name, [key])
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    msg = [e for e in res.get_step_failure_events() if e.step_key == "standardized_h5ad"][0].event_specific_data.error.message
    assert "HDF5" in msg and "max_retries" not in msg


def test_standardized_h5ad_plot_failure_non_fatal(tmp_path, write_adata, monkeypatch):
    from sc_curation_pipeline.defs import plots as plotsmod
    counts = _counts()
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "plotfail", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)})
    def _boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(plotsmod, "render_qc_panel", _boom)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is True
    md = res.asset_materializations_for_node("standardized_h5ad")[0].metadata
    assert "图未生成" in md["qc_plots"].value
    assert os.path.isfile(output_path_for(out, key, path))  # output still written


def test_standardized_h5ad_write_failure_retried(tmp_path, write_adata, monkeypatch):
    from sc_curation_pipeline.defs import qc as qcmod
    counts = _counts()
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "writefail", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)})
    calls = []
    def boom(adata, out_path):
        calls.append(out_path); raise OSError("transient disk hiccup")
    monkeypatch.setattr(qcmod, "write_standardized", boom)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    assert len(calls) == 3  # initial + 2 retries


def test_standardized_h5ad_job_targets_asset():
    assert standardized_h5ad_job.name == "standardized_h5ad_job"


def test_standardized_h5ad_missing_watch_dir_raises(tmp_path):
    # No sc/h5ad_path tag + a non-existent watch_dir -> resolve_h5ad_path fast-fails
    # with a clear SC_CURATION_WATCH_DIR error and writes no output.
    missing = str(tmp_path / "does_not_exist")
    key = "ghost_sample"
    settings = CurationSettings(watch_dir=missing, output_dir=str(tmp_path / "out"))
    inst = dg.DagsterInstance.ephemeral()
    inst.add_dynamic_partitions(h5ad_partitions.name, [key])
    res = dg.materialize(
        [standardized_h5ad], partition_key=key, instance=inst,
        resources={"curation": settings}, raise_on_error=False,
    )
    assert res.success is False
    msg = [e for e in res.get_step_failure_events() if e.step_key == "standardized_h5ad"][0].event_specific_data.error.message
    assert "SC_CURATION_WATCH_DIR" in msg
    assert missing in msg

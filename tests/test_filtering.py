import os

import anndata
import dagster as dg
import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.filtering import initially_filtered_h5ad
from sc_curation_pipeline.defs.qc import output_path_for
from sc_curation_pipeline.defs.filter_cells import filtered_path_for
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for
from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.standardize import write_standardized


def _write_standardized(out_path, counts):
    """Write a standardized-style h5ad (with a counts layer) at out_path."""
    a = anndata.AnnData(X=np.asarray(counts, dtype="float32"))
    a.layers["counts"] = sp.csr_matrix(np.asarray(counts))
    a.obs_names = [f"c{i}" for i in range(a.n_obs)]
    write_standardized(a, out_path)


def _setup(tmp_path, folder_name, counts, *, min_genes_per_cell=2, min_cells=1):
    watch = str(tmp_path / "watch"); out = str(tmp_path / "out")
    folder = os.path.join(watch, folder_name)
    os.makedirs(folder, exist_ok=True)
    src = os.path.join(folder, "a.h5ad")
    open(src, "w").close()  # placeholder; resolve_h5ad_path uses the tag path
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch, output_dir=out,
                                min_cells=min_cells, min_genes_per_cell=min_genes_per_cell)
    # write the upstream standardized file where standardized_h5ad would have put it
    standardized = output_path_for(out, key, src)
    _write_standardized(standardized, counts)
    inst = dg.DagsterInstance.ephemeral()
    inst.add_dynamic_partitions(h5ad_partitions.name, [key])
    return watch, out, src, key, settings, inst, standardized


def _materialize(src, key, settings, instance):
    return dg.materialize(
        [initially_filtered_h5ad], partition_key=key, instance=instance,
        resources={"curation": settings}, tags={"sc/h5ad_path": src},
        raise_on_error=False,
    )


def test_initially_filtered_h5ad_writes_and_counts(tmp_path):
    counts = np.array([[1, 1, 1], [1, 1, 0], [1, 0, 0]])  # 3/2/1 detected genes
    watch, out, src, key, settings, inst, std = _setup(
        tmp_path, "s", counts, min_genes_per_cell=2, min_cells=1)
    res = _materialize(src, key, settings, inst)
    assert res.success

    md = res.asset_materializations_for_node("initially_filtered_h5ad")[0].metadata
    assert md["n_cells_before"].value == 3
    assert md["n_cells_after"].value == 2   # cell with 1 gene dropped
    assert md["n_cells_removed"].value == 1
    fpath = filtered_path_for(std)
    assert os.path.isfile(fpath)
    back = anndata.read_h5ad(fpath)
    assert back.n_obs == 2
    assert os.path.isfile(std)  # upstream full file preserved


def test_initially_filtered_h5ad_too_few_after_fast_fail(tmp_path):
    counts = np.array([[1, 1, 1], [1, 0, 0]])  # at thr 2, only 1 cell remains
    watch, out, src, key, settings, inst, std = _setup(
        tmp_path, "tiny", counts, min_genes_per_cell=2, min_cells=2)
    res = _materialize(src, key, settings, inst)
    assert res.success is False
    msg = [e for e in res.get_step_failure_events()
           if e.step_key == "initially_filtered_h5ad"][0].event_specific_data.error.message
    assert "min_cells" in msg
    assert not os.path.isfile(filtered_path_for(std))  # no output written


def test_initially_filtered_h5ad_missing_standardized_fails(tmp_path):
    watch = str(tmp_path / "watch"); out = str(tmp_path / "out")
    folder = os.path.join(watch, "nofile"); os.makedirs(folder)
    src = os.path.join(folder, "a.h5ad"); open(src, "w").close()
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch, output_dir=out)
    inst = dg.DagsterInstance.ephemeral()
    inst.add_dynamic_partitions(h5ad_partitions.name, [key])
    res = _materialize(src, key, settings, inst)  # standardized file never written
    assert res.success is False

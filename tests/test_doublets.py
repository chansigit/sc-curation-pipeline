import os

import anndata
import dagster as dg
import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.doublets import compute_doublet_scores, doublet_scored_h5ad
from sc_curation_pipeline.defs.qc import output_path_for
from sc_curation_pipeline.defs.filter_cells import filtered_path_for
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for
from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.standardize import write_standardized


def _adata(counts, obs=None):
    a = anndata.AnnData(X=np.asarray(counts, dtype="float32"))
    a.layers["counts"] = sp.csr_matrix(np.asarray(counts))
    a.obs_names = [f"c{i}" for i in range(a.n_obs)]
    if obs:
        for k, v in obs.items():
            a.obs[k] = list(v)
    return a


# ---- compute_doublet_scores ----

def test_compute_doublet_scores_whole_dataset_shapes():
    rng = np.random.default_rng(0)
    a = _adata(rng.poisson(0.5, size=(300, 120)))   # no 'sample' col -> whole dataset
    scores, predicted, failed = compute_doublet_scores(a, sample_key="sample")
    assert scores.shape == (300,) and scores.dtype == np.float64
    assert predicted.shape == (300,) and predicted.dtype == bool
    assert "doublet_score" not in a.obs.columns       # pure: input not mutated


def test_compute_doublet_scores_per_sample_runs_each_group():
    rng = np.random.default_rng(1)
    a = _adata(rng.poisson(0.5, size=(300, 120)),
               obs={"sample": ["A"] * 150 + ["B"] * 150})
    scores, predicted, failed = compute_doublet_scores(a, sample_key="sample")
    assert scores.shape == (300,)
    # at least one group scored (finite values present)
    assert np.isfinite(scores).any()


def test_compute_doublet_scores_tiny_group_is_nan_nonfatal():
    rng = np.random.default_rng(2)
    counts = np.vstack([rng.poisson(0.5, size=(200, 120)),
                        rng.poisson(0.5, size=(3, 120))])   # group B: 3 cells -> Scrublet fails
    a = _adata(counts, obs={"sample": ["A"] * 200 + ["B"] * 3})
    scores, predicted, failed = compute_doublet_scores(a, sample_key="sample")
    assert "B" in failed                                # failed gracefully
    assert np.all(np.isnan(scores[200:]))               # NaN for the failed group
    assert (~predicted[200:]).all()                     # False where unavailable


# ---- doublet_scored_h5ad asset ----

def test_doublet_scored_h5ad_writes_obs_columns(tmp_path):
    watch = str(tmp_path / "watch"); out = str(tmp_path / "out")
    folder = os.path.join(watch, "s"); os.makedirs(folder)
    src = os.path.join(folder, "a.h5ad"); open(src, "w").close()  # tag path placeholder
    key = partition_key_for(watch, folder)
    # place a filtered file where doublet_scored_h5ad will look for it
    filtered = filtered_path_for(output_path_for(out, key, src))
    rng = np.random.default_rng(0)
    write_standardized(_adata(rng.poisson(0.5, size=(250, 120)),
                              obs={"sample": ["A"] * 250}), filtered)

    settings = CurationSettings(watch_dir=watch, output_dir=out)
    inst = dg.DagsterInstance.ephemeral()
    inst.add_dynamic_partitions(h5ad_partitions.name, [key])
    res = dg.materialize(
        [doublet_scored_h5ad], partition_key=key, instance=inst,
        resources={"curation": settings}, tags={"sc/h5ad_path": src},
        raise_on_error=False,
    )
    assert res.success, res

    back = anndata.read_h5ad(filtered)
    assert "doublet_score" in back.obs.columns
    assert "predicted_doublet" in back.obs.columns
    md = res.asset_materializations_for_node("doublet_scored_h5ad")[0].metadata
    assert md["batch_key"].value == "sample"
    assert md["n_cells"].value == 250

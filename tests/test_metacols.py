"""Tests for the stanmetacols integration (role identification + obs normalization)."""

import anndata as ad
import numpy as np
import pandas as pd
from stanmetacols import Candidate

from sc_curation_pipeline.defs.metacols import (
    METACOL_ROLES,
    MIN_ASSIGN_SCORE,
    identify_and_normalize,
    normalize_roles,
)


class _FakeResult:
    """Minimal stand-in for MetaColsResult: .method, .roles, .top(role)."""

    def __init__(self, method, roles):
        self.method = method
        self.roles = roles

    def top(self, role):
        cands = self.roles.get(role, [])
        return cands[0] if cands else None


def _cand(role, column, *, kind="single", score=0.9):
    return Candidate(role=role, column=column, kind=kind, score=score,
                     reason="test", source="heuristic")


def _adata(obs: pd.DataFrame):
    a = ad.AnnData(X=np.zeros((len(obs), 2), dtype=np.float32), obs=obs.copy())
    a.obs_names = [f"cell{i}" for i in range(len(obs))]
    return a


def test_normalize_copies_confident_single_to_canonical():
    a = _adata(pd.DataFrame({"orig.ident": ["a", "b", "a", "b"], "ct": ["T", "B", "T", "B"]}))
    res = _FakeResult("heuristic", {
        "sample": [_cand("sample", "orig.ident", score=0.9)],
        "cell_type_coarse": [_cand("cell_type_coarse", "ct", score=0.8)],
        "cell_type_fine": [],
    })
    summary = normalize_roles(a, res)
    # canonical columns created, equal to their source; source column kept
    assert list(a.obs["sample"]) == list(a.obs["orig.ident"])
    assert list(a.obs["cell_type_coarse"]) == list(a.obs["ct"])
    assert "cell_type_fine" not in a.obs.columns          # empty role -> not assigned
    assert summary["assigned"] == {"sample": "orig.ident", "cell_type_coarse": "ct"}
    assert summary["method"] == "heuristic"


def test_normalize_skips_below_threshold_non_single_and_missing():
    a = _adata(pd.DataFrame({"maybe_sample": ["a", "b"], "x": ["p", "q"]}))
    res = _FakeResult("heuristic", {
        "sample": [_cand("sample", "maybe_sample", score=MIN_ASSIGN_SCORE - 0.01)],   # too weak
        "cell_type_coarse": [_cand("cell_type_coarse", "a + b", kind="composite", score=0.99)],  # not single
        "cell_type_fine": [_cand("cell_type_fine", "ghost_col", score=0.99)],         # not in obs
    })
    summary = normalize_roles(a, res)
    assert summary["assigned"] == {}
    for role in METACOL_ROLES:
        assert role not in a.obs.columns                  # nothing materialized
    # ranking is still recorded for provenance even when nothing is assigned
    assert summary["ranking"]["cell_type_coarse"][0]["kind"] == "composite"


def test_identify_and_normalize_offline_heuristic():
    # use_llm=False -> deterministic heuristic; no network / API key needed.
    rng = np.random.default_rng(0)
    n = 200
    a = _adata(pd.DataFrame({
        "cell_type": rng.choice(["T cell", "B cell", "NK cell", "Monocyte"], size=n),
        "batch": rng.choice(["s1", "s2", "s3"], size=n),
    }))
    summary = identify_and_normalize(a, use_llm=False)
    assert summary["method"] == "heuristic"
    # the cell-type column should be recognized and copied to the canonical column
    assert summary["assigned"].get("cell_type_coarse") == "cell_type"
    assert list(a.obs["cell_type_coarse"]) == list(a.obs["cell_type"])

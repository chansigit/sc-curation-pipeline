"""Tests for the stanmetacols integration (role identification + obs normalization)."""

import anndata as ad
import numpy as np
import pandas as pd
from stanmetacols import Candidate

from sc_curation_pipeline.defs.metacols import (
    MIN_ASSIGN_SCORE,
    NORMALIZE_ROLES,
    identify_and_normalize,
    normalize_roles,
    render_metacols_md,
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
    for role in NORMALIZE_ROLES:
        assert role not in a.obs.columns                  # nothing materialized
    # ranking is still recorded for provenance even when nothing is assigned
    assert summary["ranking"]["cell_type_coarse"][0]["kind"] == "composite"


def test_single_celltype_column_not_duplicated_into_fine():
    # One cell-type column ranked top for BOTH celltype roles must land in only the
    # higher-scoring role (coarse), never be duplicated into fine.
    a = _adata(pd.DataFrame({"cell_type": ["T", "B", "T", "B"]}))
    res = _FakeResult("heuristic", {
        "sample": [],
        "cell_type_coarse": [_cand("cell_type_coarse", "cell_type", score=0.84)],
        "cell_type_fine": [_cand("cell_type_fine", "cell_type", score=0.70)],
    })
    summary = normalize_roles(a, res)
    assert summary["assigned"] == {"cell_type_coarse": "cell_type"}
    assert "cell_type_coarse" in a.obs.columns
    assert "cell_type_fine" not in a.obs.columns          # not duplicated


def test_organ_and_tissue_are_normalized():
    # the new organ/tissue roles are normalized to canonical obs columns too
    a = _adata(pd.DataFrame({"organ_col": ["brain", "liver"], "tis": ["cortex", "lobe"]}))
    res = _FakeResult("llm (openai)", {
        "organ": [_cand("organ", "organ_col", score=0.9)],
        "tissue": [_cand("tissue", "tis", score=0.8)],
    })
    summary = normalize_roles(a, res)
    assert summary["assigned"] == {"organ": "organ_col", "tissue": "tis"}
    assert list(a.obs["organ"]) == ["brain", "liver"]
    assert list(a.obs["tissue"]) == ["cortex", "lobe"]


def test_render_metacols_md_table():
    summary = {
        "method": "llm (openai)",
        "assigned": {"sample": "donor_id", "organ": "organ_col"},
        "ranking": {
            "sample": [{"column": "donor_id", "kind": "single", "score": 0.95,
                        "reason": "r", "source": "llm"}],
            "organ": [{"column": "organ_col", "kind": "single", "score": 0.9,
                       "reason": "r", "source": "llm"}],
            "tissue": [],  # absent role -> placeholder row, no crash
        },
    }
    md = render_metacols_md(summary)
    assert "method: `llm (openai)`" in md
    assert "| role |" in md and "| `sample` |" in md
    assert "✅ `donor_id`" in md          # normalized role flagged
    assert "`organ_col`" in md
    assert "| `tissue` |" in md           # empty role still rendered


def test_provider_args_ignored_when_offline():
    # provider/model/base_url/api_key_env are accepted but irrelevant with use_llm=False
    # (offline heuristic); a missing api_key_env var must not crash.
    a = _adata(pd.DataFrame({"cell_type": ["T", "B", "T", "B"]}))
    summary = identify_and_normalize(
        a, use_llm=False, provider="openai", model="doubao-x",
        base_url="https://example/v1", api_key_env="NONEXISTENT_KEY_VAR_XYZ")
    assert summary["method"] == "heuristic"


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

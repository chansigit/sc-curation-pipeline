"""Identify standard metadata roles in ``obs`` (via stanmetacols) and normalize the
chosen columns to canonical names.

stanmetacols *ranks* obs columns per role but does not decide; this module applies
the pipeline's policy for the three roles we care about (``sample``,
``cell_type_coarse``, ``cell_type_fine`` — the QC numerics like pct_mt/pct_hb we
compute ourselves and deliberately leave out). For each role it takes the top-1
candidate and, when that candidate is a plain ``single`` column present in obs and
scored confidently enough, copies it to a canonical obs column of the same name.
Composite / barcode candidates and low-confidence picks are recorded but NOT turned
into a canonical column. The full ranking is returned for provenance (uns + Dagster
metadata).
"""

import stanmetacols

# Roles the pipeline normalizes. The numeric QC roles are excluded on purpose —
# h5ad_qc computes pct_counts_mt / pct_counts_hb itself.
METACOL_ROLES = ["sample", "cell_type_coarse", "cell_type_fine"]
# Assign a canonical column only when the top pick clears this score; weaker picks
# (especially cell-type look-alikes) are recorded but not materialized into obs.
MIN_ASSIGN_SCORE = 0.5


def _candidate_dict(c) -> dict:
    """A stanmetacols Candidate as a plain JSON-serializable dict."""
    return {"column": c.column, "kind": c.kind, "score": float(c.score),
            "reason": c.reason, "source": c.source}


def normalize_roles(adata, result) -> dict:
    """Apply the assign policy to a stanmetacols result; mutate obs; return summary.

    For each role the top-1 candidate is copied to ``obs[<role>]`` only when it is a
    ``single`` column present in obs scoring >= ``MIN_ASSIGN_SCORE``. The source
    column is left untouched. Returns a JSON-serializable
    ``{"method", "assigned", "ranking"}`` (``assigned`` maps role -> source column).
    """
    assigned: dict[str, str] = {}
    for role in METACOL_ROLES:
        cand = result.top(role)
        if (cand is not None and cand.kind == "single"
                and cand.score >= MIN_ASSIGN_SCORE
                and cand.column in adata.obs.columns):
            adata.obs[role] = adata.obs[cand.column].values  # canonical copy; source kept
            assigned[role] = cand.column
    ranking = {role: [_candidate_dict(c) for c in result.roles.get(role, [])]
               for role in METACOL_ROLES}
    return {"method": result.method, "assigned": assigned, "ranking": ranking}


def identify_and_normalize(adata, *, use_llm: bool = True) -> dict:
    """Rank the three metadata roles on ``adata`` and normalize confident picks.

    ``use_llm`` follows stanmetacols semantics: True tries the LLM and falls back to
    the offline heuristic if it is unavailable; False forces the deterministic
    heuristic (no network/key). Requests only ``METACOL_ROLES`` to keep the prompt
    small. Mutates ``adata.obs``; never writes files.
    """
    result = stanmetacols.rank_meta_columns(adata, roles=METACOL_ROLES, use_llm=use_llm)
    return normalize_roles(adata, result)

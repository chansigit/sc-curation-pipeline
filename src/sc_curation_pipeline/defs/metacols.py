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
# 0.5 is the midpoint of the 0..1 score: in the heuristic a cell-type column scores
# 0.4*name + 0.4*vocab + 0.2*card_fit, so clearing 0.5 takes more than a single weak
# (name-only or vocab-only) signal — it filters the most common false positives.
MIN_ASSIGN_SCORE = 0.5


def _candidate_dict(c) -> dict:
    """A stanmetacols Candidate as a plain JSON-serializable dict."""
    return {"column": c.column, "kind": c.kind, "score": float(c.score),
            "reason": c.reason, "source": c.source}


def normalize_roles(adata, result) -> dict:
    """Apply the assign policy to a stanmetacols result; mutate obs; return summary.

    A role's top-1 candidate qualifies when it is a ``single`` column present in obs
    scoring >= ``MIN_ASSIGN_SCORE``. A given source column is then assigned to at
    most ONE role — the highest-scoring one (ties favour the earlier role, i.e.
    coarse over fine) — so a dataset carrying a single cell-type column is normalized
    to ``cell_type_coarse`` only, NOT duplicated into ``cell_type_fine``. The source
    column is left untouched. Returns a JSON-serializable
    ``{"method", "assigned", "ranking"}`` (``assigned`` maps role -> source column).
    """
    # Collect qualifying top-1 picks per role, preserving METACOL_ROLES order.
    picks: dict[str, tuple[str, float]] = {}
    for role in METACOL_ROLES:
        cand = result.top(role)
        if (cand is not None and cand.kind == "single"
                and cand.score >= MIN_ASSIGN_SCORE
                and cand.column in adata.obs.columns):
            picks[role] = (cand.column, float(cand.score))

    # One source column -> one role (highest score; ties keep the earlier role,
    # so a lone cell-type column lands in coarse and is not duplicated into fine).
    winner: dict[str, str] = {}  # source column -> winning role
    for role, (col, score) in picks.items():
        if col not in winner or score > picks[winner[col]][1]:
            winner[col] = role

    assigned: dict[str, str] = {}
    for role, (col, _) in picks.items():
        if winner[col] == role:
            adata.obs[role] = adata.obs[col].values  # canonical copy; source kept
            assigned[role] = col

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

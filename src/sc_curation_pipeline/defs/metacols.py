"""Identify standard metadata roles in ``obs`` (via stanmetacols) and normalize the
chosen columns to canonical names.

stanmetacols *ranks* obs columns for every role but does not decide. This module:
- requests the **full** role set (``roles=None`` -> all stanmetacols roles), so the
  recorded result is a complete parse (incl. organ / tissue and the numeric QC
  roles), surfaced as Dagster markdown + ``uns["metacols"]``;
- applies a normalize policy only to the **categorical/grouping** roles
  (``NORMALIZE_ROLES``): for each, when the top-1 candidate is a plain ``single``
  column present in obs and scored confidently enough, it is copied to a canonical
  obs column of the same name. The numeric QC roles (pct_mt/pct_hb/…) are shown but
  never written — h5ad_qc computes pct_counts_mt / pct_counts_hb itself.
Composite / barcode candidates and low-confidence picks are recorded but NOT turned
into a canonical column.
"""

import os

import stanmetacols

# Roles copied to canonical obs columns when confidently identified (the
# categorical/grouping roles). Numeric QC roles are deliberately excluded — the
# pipeline computes those itself — though they still appear in the recorded parse.
NORMALIZE_ROLES = ["sample", "cell_type_coarse", "cell_type_fine", "organ", "tissue"]
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
    # Collect qualifying top-1 picks for the normalizable roles, in NORMALIZE_ROLES order.
    picks: dict[str, tuple[str, float]] = {}
    for role in NORMALIZE_ROLES:
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

    # ranking covers EVERY role in the result (full parse), not just the normalized ones.
    ranking = {role: [_candidate_dict(c) for c in cands]
               for role, cands in result.roles.items()}
    return {"method": result.method, "assigned": assigned, "ranking": ranking}


def identify_and_normalize(adata, *, use_llm: bool = True, provider: str = "anthropic",
                           model: str = "claude-opus-4-8", base_url: str | None = None,
                           api_key_env: str | None = None) -> dict:
    """Rank ALL metadata roles on ``adata`` and normalize the categorical picks.

    Requests the full stanmetacols role set (``roles=None``) so the recorded parse is
    complete (incl. organ / tissue); :func:`normalize_roles` then writes only the
    ``NORMALIZE_ROLES`` it can confidently place. ``use_llm`` follows stanmetacols
    semantics: True tries the LLM and falls back to the offline heuristic if it is
    unavailable; False forces the deterministic heuristic (no network/key).
    ``provider``/``model``/``base_url`` select the LLM backend — provider ``"openai"``
    targets any OpenAI-compatible endpoint (e.g. Volcengine ARK / Doubao) via
    ``base_url``. ``api_key_env`` names the env var holding the key (resolved here and
    passed to stanmetacols). Mutates ``adata.obs``; never writes files.
    """
    api_key = os.environ.get(api_key_env) if api_key_env else None
    result = stanmetacols.rank_meta_columns(
        adata, roles=None, use_llm=use_llm,
        provider=provider, model=model,
        base_url=base_url or None, api_key=api_key,
    )
    return normalize_roles(adata, result)


def render_metacols_md(summary: dict) -> str:
    """Render a stanmetacols parse summary as a Dagster-metadata markdown table.

    One row per role (top-1 candidate), an ``obs`` column flagging which roles were
    normalized to canonical obs columns. ``summary`` is the dict returned by
    :func:`identify_and_normalize` (or the non-fatal fallback dict).
    """
    assigned = summary.get("assigned", {})
    ranking = summary.get("ranking", {})
    lines = [
        f"**stanmetacols** · method: `{summary.get('method', '?')}`",
        "",
        "| role | → obs | top column | score | kind | source |",
        "|---|---|---|---|---|---|",
    ]
    for role, cands in ranking.items():
        obs_mark = f"✅ `{assigned[role]}`" if role in assigned else "—"
        top = cands[0] if cands else None
        if top:
            lines.append(
                f"| `{role}` | {obs_mark} | `{top['column']}` | "
                f"{top['score']:.2f} | {top['kind']} | {top['source']} |"
            )
        else:
            lines.append(f"| `{role}` | {obs_mark} | — | | | |")
    return "\n".join(lines)

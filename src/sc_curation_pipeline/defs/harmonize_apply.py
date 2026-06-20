"""Apply a stangene HarmonizationResult to an AnnData: rename var to canonical symbols.

Policy (pipeline-side, deliberately NOT in stangene which never overwrites names):
- var_names become the canonical ``gene_symbol_harmonized``;
- features that are unmapped / non-gene / have no symbol KEEP their original name;
- the original names are stashed in ``var["original_feature_name"]``;
- duplicate resulting names are de-duplicated via AnnData's make-unique;
- the stangene mapping columns are merged into ``adata.var`` for provenance.
The expression matrix and all layers are untouched — only var labels change.
"""

import pandas as pd

_MAPPING_COLS = [
    "gene_id_harmonized", "gene_symbol_harmonized", "mapping_status",
    "mapping_confidence", "mapping_source", "mapping_notes",
]
_KEEP_ORIGINAL_STATUS = {"unmapped", "non_gene_feature"}


def _canonical_name(symbol, status, original: str) -> str:
    """Canonical symbol, or the original name when unmapped/non-gene/blank."""
    if status in _KEEP_ORIGINAL_STATUS:
        return original
    if symbol is None or (isinstance(symbol, float) and pd.isna(symbol)):
        return original
    s = str(symbol).strip()
    return s if s else original


def apply_harmonization(adata, result):
    """Rename adata.var to canonical symbols per a stangene HarmonizationResult.

    ``result.mapping_table`` must be row-aligned to ``adata.var_names`` (as
    produced by ``stangene.harmonize_anndata``). Mutates and returns ``adata``.
    """
    mt = result.mapping_table
    if len(mt) != adata.n_vars:
        raise ValueError(
            f"harmonization rows ({len(mt)}) != adata.n_vars ({adata.n_vars})"
        )

    originals = list(adata.var_names)
    symbols = list(mt["gene_symbol_harmonized"])
    statuses = list(mt["mapping_status"])
    new_names = [
        _canonical_name(sym, st, orig)
        for sym, st, orig in zip(symbols, statuses, originals)
    ]

    # Merge provenance columns (positional) before relabeling the index.
    for col in _MAPPING_COLS:
        if col in mt.columns:
            adata.var[col] = list(mt[col])
    adata.var["original_feature_name"] = originals

    adata.var_names = pd.Index(new_names)
    adata.var_names_make_unique()
    return adata

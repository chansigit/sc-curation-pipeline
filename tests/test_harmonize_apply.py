import types

import anndata
import numpy as np
import pandas as pd

from sc_curation_pipeline.defs.harmonize_apply import apply_harmonization


def _result(rows):
    """Build a stand-in HarmonizationResult.mapping_table from row dicts."""
    df = pd.DataFrame(rows)
    return types.SimpleNamespace(mapping_table=df)


def _adata(var_names):
    a = anndata.AnnData(X=np.arange(2 * len(var_names), dtype="float32").reshape(2, -1))
    a.var_names = list(var_names)
    return a


def test_renames_mapped_keeps_unmapped():
    a = _adata(["TP53", "p53", "WEIRDGENE"])
    res = _result([
        {"gene_symbol_harmonized": "TP53", "mapping_status": "exact_symbol",
         "gene_id_harmonized": "ENSG00000141510"},
        {"gene_symbol_harmonized": "TP53", "mapping_status": "alias_symbol",
         "gene_id_harmonized": "ENSG00000141510"},
        {"gene_symbol_harmonized": None, "mapping_status": "unmapped",
         "gene_id_harmonized": None},
    ])
    out = apply_harmonization(a, res)
    # mapped -> symbol (TP53 twice -> made unique); unmapped -> original kept
    assert list(out.var_names) == ["TP53", "TP53-1", "WEIRDGENE"]
    assert list(out.var["original_feature_name"]) == ["TP53", "p53", "WEIRDGENE"]
    assert "mapping_status" in out.var.columns
    assert "gene_id_harmonized" in out.var.columns


def test_non_gene_feature_keeps_original():
    a = _adata(["CD3_ADT"])
    res = _result([{"gene_symbol_harmonized": "", "mapping_status": "non_gene_feature",
                    "gene_id_harmonized": None}])
    out = apply_harmonization(a, res)
    assert list(out.var_names) == ["CD3_ADT"]


def test_blank_symbol_falls_back_to_original():
    a = _adata(["GeneX"])
    res = _result([{"gene_symbol_harmonized": "   ", "mapping_status": "exact_symbol",
                    "gene_id_harmonized": "ENSGX"}])
    out = apply_harmonization(a, res)
    assert list(out.var_names) == ["GeneX"]


def test_matrix_columns_unchanged():
    a = _adata(["A", "B"])
    X_before = a.X.copy()
    res = _result([
        {"gene_symbol_harmonized": "AA", "mapping_status": "exact_symbol", "gene_id_harmonized": "i1"},
        {"gene_symbol_harmonized": "BB", "mapping_status": "exact_symbol", "gene_id_harmonized": "i2"},
    ])
    out = apply_harmonization(a, res)
    np.testing.assert_array_equal(out.X, X_before)  # only labels changed
    assert list(out.var_names) == ["AA", "BB"]


def test_length_mismatch_raises():
    a = _adata(["A", "B"])
    res = _result([{"gene_symbol_harmonized": "AA", "mapping_status": "exact_symbol",
                    "gene_id_harmonized": "i1"}])
    try:
        apply_harmonization(a, res)
        assert False, "expected ValueError"
    except ValueError as e:
        assert "n_vars" in str(e)

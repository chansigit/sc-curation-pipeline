import pytest

from sc_curation_pipeline.defs import settings as S


def test_field_defaults(settings_factory):
    cs = settings_factory()
    assert cs.done_marker == ".done"
    assert cs.h5ad_glob == "*.h5ad"
    assert cs.scan_interval_sec == 30
    assert cs.min_cells == 100
    assert cs.min_genes == 5000
    assert cs.min_genes_per_cell == 400


def test_build_reads_min_genes_per_cell(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/w")
    monkeypatch.setenv("SC_CURATION_OUTPUT_DIR", "/out")
    monkeypatch.delenv("SC_CURATION_MIN_GENES_PER_CELL", raising=False)
    assert S.build_curation_settings().min_genes_per_cell == 400  # default
    monkeypatch.setenv("SC_CURATION_MIN_GENES_PER_CELL", "250")
    assert S.build_curation_settings().min_genes_per_cell == 250
    monkeypatch.setenv("SC_CURATION_MIN_GENES_PER_CELL", "")       # invalid -> default
    assert S.build_curation_settings().min_genes_per_cell == 400


def test_build_requires_output_dir(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/w")
    monkeypatch.delenv("SC_CURATION_OUTPUT_DIR", raising=False)
    with pytest.raises(ValueError) as e:
        S.build_curation_settings()
    assert "SC_CURATION_OUTPUT_DIR" in str(e.value)


def test_build_reads_output_dir_and_min_genes(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/w")
    monkeypatch.setenv("SC_CURATION_OUTPUT_DIR", "/out")
    monkeypatch.setenv("SC_CURATION_MIN_GENES", "3000")
    cs = S.build_curation_settings()
    assert cs.output_dir == "/out"
    assert cs.min_genes == 3000


def test_build_curation_settings_env_defaults(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/data/watch")
    monkeypatch.setenv("SC_CURATION_OUTPUT_DIR", "/data/out")
    monkeypatch.delenv("SC_CURATION_DONE_MARKER", raising=False)
    monkeypatch.delenv("SC_CURATION_H5AD_GLOB", raising=False)
    monkeypatch.delenv("SC_CURATION_SCAN_INTERVAL_SEC", raising=False)
    monkeypatch.delenv("SC_CURATION_MIN_CELLS", raising=False)
    monkeypatch.delenv("SC_CURATION_MIN_GENES", raising=False)
    cs = S.build_curation_settings()
    assert cs.watch_dir == "/data/watch"
    assert cs.done_marker == ".done"
    assert cs.scan_interval_sec == 30
    assert cs.min_cells == 100
    assert cs.min_genes == 5000


def test_build_curation_settings_env_overrides(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/w")
    monkeypatch.setenv("SC_CURATION_OUTPUT_DIR", "/out")
    monkeypatch.setenv("SC_CURATION_MIN_CELLS", "250")
    monkeypatch.setenv("SC_CURATION_MIN_GENES", "8000")
    monkeypatch.setenv("SC_CURATION_SCAN_INTERVAL_SEC", "60")
    cs = S.build_curation_settings()
    assert cs.min_cells == 250
    assert cs.min_genes == 8000
    assert cs.scan_interval_sec == 60


def test_sanitize_key_roundtrip():
    key = S.partition_key_for("/watch", "/watch/GSE123/sampleA")
    assert "/" not in key
    assert key == S.sanitize_key("GSE123/sampleA")
    assert S.path_for_partition_key(key) == "GSE123/sampleA"


def test_sanitize_key_single_level():
    key = S.partition_key_for("/watch", "/watch/foo")
    assert key == "foo"
    assert S.path_for_partition_key(key) == "foo"


def test_sanitize_key_no_collision():
    # A nested path and a flat folder literally containing the separator must
    # NOT collapse to the same partition key (silent sample loss).
    k_nested = S.partition_key_for("/watch", "/watch/GSE1/sampleB")
    k_flat = S.partition_key_for("/watch", "/watch/GSE1__sampleB")
    assert k_nested != k_flat
    assert S.path_for_partition_key(k_nested) == "GSE1/sampleB"
    assert S.path_for_partition_key(k_flat) == "GSE1__sampleB"


@pytest.mark.parametrize("setval", [None, "", "   "])
def test_watch_dir_missing_or_empty_raises(monkeypatch, setval):
    # Unset, empty, or whitespace-only -> clear ValueError at load time
    # (not a silent no-op sensor).
    if setval is None:
        monkeypatch.delenv("SC_CURATION_WATCH_DIR", raising=False)
    else:
        monkeypatch.setenv("SC_CURATION_WATCH_DIR", setval)
    with pytest.raises(ValueError) as excinfo:
        S.build_curation_settings()
    assert "SC_CURATION_WATCH_DIR" in str(excinfo.value)


def test_malformed_numeric_env_degrades_to_default(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/w")
    monkeypatch.setenv("SC_CURATION_OUTPUT_DIR", "/out")
    monkeypatch.setenv("SC_CURATION_SCAN_INTERVAL_SEC", "")    # empty
    monkeypatch.setenv("SC_CURATION_MIN_CELLS", "abc")        # invalid
    monkeypatch.setenv("SC_CURATION_MIN_GENES", "")           # empty
    cs = S.build_curation_settings()
    assert cs.scan_interval_sec == 30
    assert cs.min_cells == 100
    assert cs.min_genes == 5000

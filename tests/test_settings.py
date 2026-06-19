import pytest

from sc_curation_pipeline.defs import settings as S


def test_field_defaults(settings_factory):
    cs = settings_factory()
    assert cs.done_marker == ".done"
    assert cs.h5ad_glob == "*.h5ad"
    assert cs.scan_interval_sec == 30
    assert cs.min_cells == 100
    assert cs.max_mito_pct == 20.0


def test_build_curation_settings_env_defaults(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/data/watch")
    monkeypatch.delenv("SC_CURATION_DONE_MARKER", raising=False)
    monkeypatch.delenv("SC_CURATION_H5AD_GLOB", raising=False)
    monkeypatch.delenv("SC_CURATION_SCAN_INTERVAL_SEC", raising=False)
    monkeypatch.delenv("SC_CURATION_MIN_CELLS", raising=False)
    monkeypatch.delenv("SC_CURATION_MAX_MITO_PCT", raising=False)
    cs = S.build_curation_settings()
    assert cs.watch_dir == "/data/watch"
    assert cs.done_marker == ".done"
    assert cs.scan_interval_sec == 30
    assert cs.min_cells == 100
    assert cs.max_mito_pct == 20.0


def test_build_curation_settings_env_overrides(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/w")
    monkeypatch.setenv("SC_CURATION_MIN_CELLS", "250")
    monkeypatch.setenv("SC_CURATION_MAX_MITO_PCT", "12.5")
    monkeypatch.setenv("SC_CURATION_SCAN_INTERVAL_SEC", "60")
    cs = S.build_curation_settings()
    assert cs.min_cells == 250
    assert cs.max_mito_pct == 12.5
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


def test_watch_dir_missing_raises(monkeypatch):
    monkeypatch.delenv("SC_CURATION_WATCH_DIR", raising=False)
    with pytest.raises(KeyError) as excinfo:
        S.build_curation_settings()
    assert "SC_CURATION_WATCH_DIR" in str(excinfo.value)


def test_malformed_numeric_env_degrades_to_default(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/w")
    monkeypatch.setenv("SC_CURATION_SCAN_INTERVAL_SEC", "")    # empty
    monkeypatch.setenv("SC_CURATION_MIN_CELLS", "abc")        # invalid
    monkeypatch.setenv("SC_CURATION_MAX_MITO_PCT", "")        # empty
    cs = S.build_curation_settings()
    assert cs.scan_interval_sec == 30
    assert cs.min_cells == 100
    assert cs.max_mito_pct == 20.0

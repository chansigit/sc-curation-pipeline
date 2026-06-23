import base64
import re

import numpy as np

from sc_curation_pipeline.defs.plots import _logy_ticks, downsample_index, render_qc_panel

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_DATA_URI = re.compile(r"data:image/png;base64,([A-Za-z0-9+/=]+)\)")


def _decode_png(markdown: str) -> bytes:
    m = _DATA_URI.search(markdown)
    assert m, f"no base64 PNG data URI found in: {markdown[:80]!r}..."
    return base64.b64decode(m.group(1))


def test_render_qc_panel_returns_inline_png():
    rng = np.random.default_rng(0)
    counts = rng.integers(100, 5000, size=200).astype(float)
    genes = rng.integers(50, 2000, size=200).astype(float)
    mito_pct = rng.uniform(0, 30, size=200)
    hb_pct = rng.uniform(0, 10, size=200)
    md = render_qc_panel(counts, genes, mito_pct, hb_pct, sample_label="GSE1_sampleA")
    assert "data:image/png;base64," in md
    assert _decode_png(md).startswith(PNG_MAGIC)


def test_render_qc_panel_constant_columns_do_not_crash():
    # All-zero / constant per-cell arrays would make violinplot raise; the panel
    # must still render a valid PNG (the flat-marker fallback).
    n = 50
    zeros = np.zeros(n)
    md = render_qc_panel(zeros, zeros, zeros, zeros, sample_label="empty")
    assert _decode_png(md).startswith(PNG_MAGIC)


def test_render_qc_panel_empty_sample_does_not_crash():
    empty = np.array([], dtype=float)
    md = render_qc_panel(empty, empty, empty, empty, sample_label="nocells")
    assert _decode_png(md).startswith(PNG_MAGIC)


def test_logy_ticks_scheme():
    # 1/2/3/5/7 per decade from 100, always covering the first decade through 1000
    assert _logy_ticks(950) == [100, 200, 300, 500, 700, 1000]
    big = _logy_ticks(42000)
    assert big[:6] == [100, 200, 300, 500, 700, 1000]
    for v in (2000, 3000, 5000, 7000, 10000, 20000, 30000):
        assert v in big                            # log-friendly subset every decade
    for v in (4000, 6000, 8000, 11000, 40000):
        assert v not in big                        # not strict every-1000


def test_render_qc_panel_log_violins_render_with_positive_data():
    # positive data exercises the log-y path for total_counts / genes_per_cell
    rng = np.random.default_rng(1)
    counts = rng.integers(200, 40000, size=300).astype(float)
    genes = rng.integers(80, 6000, size=300).astype(float)
    pct = rng.uniform(0, 20, size=300)
    md = render_qc_panel(counts, genes, pct, pct, sample_label="logcase")
    assert _decode_png(md).startswith(PNG_MAGIC)


def test_downsample_index_returns_all_when_under_cap():
    idx = downsample_index(10, cap=50)
    assert np.array_equal(idx, np.arange(10))


def test_downsample_index_caps_and_stays_in_range():
    idx = downsample_index(1000, cap=100)
    assert idx.shape[0] == 100
    assert idx.min() >= 0 and idx.max() < 1000
    assert len(np.unique(idx)) == 100  # sampled without replacement

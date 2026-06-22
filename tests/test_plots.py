import base64
import re

import numpy as np

from sc_curation_pipeline.defs.plots import downsample_index, render_qc_panel

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


def test_downsample_index_returns_all_when_under_cap():
    idx = downsample_index(10, cap=50)
    assert np.array_equal(idx, np.arange(10))


def test_downsample_index_caps_and_stays_in_range():
    idx = downsample_index(1000, cap=100)
    assert idx.shape[0] == 100
    assert idx.min() >= 0 and idx.max() < 1000
    assert len(np.unique(idx)) == 100  # sampled without replacement

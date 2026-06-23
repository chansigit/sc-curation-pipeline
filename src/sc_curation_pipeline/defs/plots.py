"""Render the classic scanpy-style QC panel for one sample as an inline image.

The panel is built with matplotlib directly from the per-cell arrays that
``compute_qc`` already produces (no second read of the h5ad, no full in-memory
load), so it preserves the memory-aware/streaming design of the QC step. The
result is a markdown string with a base64-embedded PNG, suitable for a Dagster
``MetadataValue.md`` — nothing is ever written to disk.
"""

import base64
import io

import matplotlib

# Headless: compute nodes have no display. MUST precede the pyplot import.
matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.ticker import FixedFormatter, FixedLocator, NullLocator  # noqa: E402

# Plotting millions of points in a scatter is slow and bloats the PNG. Violins
# and all reported medians still use EVERY cell; only the two scatters are
# downsampled (with a fixed seed, so the picture is reproducible).
SCATTER_MAX_POINTS = 50_000


def downsample_index(n: int, cap: int = SCATTER_MAX_POINTS, seed: int = 0) -> np.ndarray:
    """Indices to plot: all of them if ``n <= cap``, else a fixed random subset."""
    if n <= cap:
        return np.arange(n)
    rng = np.random.default_rng(seed)
    return rng.choice(n, size=cap, replace=False)


def _logy_ticks(vmax: float) -> list[int]:
    """Tick values for a log y-axis: a 1/2/3/5/7-per-decade subset from 100 up to
    the data max (always covering the first decade through 1000). The 1/2/3/5/7
    spacing keeps labels readable at every magnitude, instead of colliding near
    each decade's top the way a fixed linear step (every 100, every 1000) does."""
    mult = (1, 2, 3, 5, 7)
    top = max(vmax, 1000)
    ticks, decade = [], 100
    while decade <= top:
        ticks += [m * decade for m in mult if m * decade <= top]
        decade *= 10
    return ticks


def _violin(ax, data: np.ndarray, title: str, *, logy: bool = False) -> None:
    # A log y-axis cannot show non-positive values; drop them for the distribution
    # (degenerate empty/zero-count cells are filtered downstream anyway).
    if logy:
        data = data[data > 0]
    n = data.shape[0]
    # violinplot needs variance; a constant (e.g. all-zero) column would make it
    # raise. Fall back to a flat marker at the constant value.
    if n > 0 and float(np.ptp(data)) > 0:
        ax.violinplot(data, showmedians=True)
    elif n > 0:
        ax.scatter([1], [data[0]], marker="_", s=400)
    ax.set_title(title)
    ax.set_xticks([])
    if logy and n > 0:
        ax.set_yscale("log")
        ticks = _logy_ticks(float(data.max()))
        ax.yaxis.set_major_locator(FixedLocator(ticks))
        ax.yaxis.set_major_formatter(FixedFormatter([str(t) for t in ticks]))
        ax.yaxis.set_minor_locator(NullLocator())  # only the requested ticks


def render_qc_panel(
    counts: np.ndarray,
    genes: np.ndarray,
    mito_pct: np.ndarray,
    hb_pct: np.ndarray,
    *,
    sample_label: str,
    scatter_cap: int = SCATTER_MAX_POINTS,
) -> str:
    """Render the QC panel for one sample as a markdown string with an inline PNG.

    Layout (single figure): row 1 = three violins (total_counts, genes_per_cell,
    mito_pct); row 2 = two scatters (counts x mito%, counts x genes) + an
    hb_pct violin in the 6th cell. ``total_counts`` and ``genes_per_cell`` use a
    log y-axis with a 1/2/3/5/7-per-decade tick subset; the scatters keep linear
    axes. The four inputs are per-cell 1-D arrays of equal length
    (n_cells). Returns ``"![qc ...](data:image/png;base64,...)"``. Writes no files.
    """
    counts = np.asarray(counts, dtype=float)
    genes = np.asarray(genes, dtype=float)
    mito_pct = np.asarray(mito_pct, dtype=float)
    hb_pct = np.asarray(hb_pct, dtype=float)
    n = counts.shape[0]

    fig, axes = plt.subplots(2, 3, figsize=(12, 7))
    try:
        _violin(axes[0, 0], counts, "total_counts", logy=True)
        _violin(axes[0, 1], genes, "genes_per_cell", logy=True)
        _violin(axes[0, 2], mito_pct, "mito_pct")

        idx = downsample_index(n, scatter_cap)
        shown = f"  (showing {len(idx):,} of {n:,})" if n > scatter_cap else ""
        axes[1, 0].scatter(counts[idx], mito_pct[idx], s=4, alpha=0.4)
        axes[1, 0].set_xlabel("total_counts")
        axes[1, 0].set_ylabel("mito_pct")
        axes[1, 0].set_title("counts x mito%" + shown)
        axes[1, 1].scatter(counts[idx], genes[idx], s=4, alpha=0.4)
        axes[1, 1].set_xlabel("total_counts")
        axes[1, 1].set_ylabel("genes_per_cell")
        axes[1, 1].set_title("counts x genes" + shown)
        _violin(axes[1, 2], hb_pct, "hb_pct")

        fig.suptitle(f"QC: {sample_label}  (n_cells={n:,})")
        fig.tight_layout(rect=(0, 0, 1, 0.96))

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=100, bbox_inches="tight")
    finally:
        plt.close(fig)

    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"![qc panel for {sample_label}](data:image/png;base64,{b64})"

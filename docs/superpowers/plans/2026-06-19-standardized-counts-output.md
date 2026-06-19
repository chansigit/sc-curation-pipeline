# Standardized counts output + QC-on-counts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the single `h5ad_qc` asset both write a standardized counts-bearing `.h5ad` to an independent output dir AND recompute QC (metrics + plots) on the counts, gated by two hard thresholds; move counts-sourcing into stancounts as a reusable `get_counts()`.

**Architecture:** Per sample (partition): load h5ad → `stancounts.get_counts(adata)` (existing layer / X / .raw / reverse_log1p) → hard gates `min_cells` & `min_genes` → build standardized adata (`layers["counts"]` + `X=normalize_total(1e4)+log1p`, velocity layers preserved) → write `.h5ad` to `SC_CURATION_OUTPUT_DIR` (never touch source) → compute QC on counts + render plots → one `MaterializeResult` (no asset checks).

**Tech Stack:** Python 3.12, Dagster 1.13.10, scanpy/anndata/scipy/numpy/matplotlib (dl2025 venv), stancounts (local editable).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-19-standardized-counts-output-design.md` — every task's requirements implicitly include it.
- Runtime: dl2025 venv python `/scratch/users/chensj16/venvs/dl2025/.venv/bin/python`. NEVER run pytest/python on a login node; we are on compute node `sh02-06n11` (allocation) — run there.
- Always run pytest with `MPLCONFIGDIR=$SCRATCH/.mplconfig` (matplotlib font cache off `$HOME`); create it once: `mkdir -p $SCRATCH/.mplconfig`.
- `/home/users/chensj16/s` is a symlink to `/scratch/users/chensj16`. stancounts repo = `/home/users/chensj16/s/projects/stancounts` (== `/scratch/users/chensj16/projects/stancounts`). It is ALREADY editable-installed in dl2025 → source edits are live immediately; NO reinstall needed.
- Pipeline repo (`PROJ`) = `/scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline`. It is on branch `main`; commit directly to `main` (project convention).
- Counts whitelist (verbatim): `("counts","count","raw_counts","counts_raw","umi","umis","umi_counts","X_counts")`. Velocity exclude (verbatim): `("spliced","unspliced","ambiguous","spliced_counts","unspliced_counts","matrix")`.
- `X = normalize_total(target_sum=1e4)` then `log1p`. `layers["counts"]` = integer counts. Preserve velocity/other layers + obs/var/obsm/obsp/uns.
- Defaults: `min_cells=100`, `min_genes=5000` (n_genes_detected = #genes with counts>0 in ≥1 cell). `max_mito_pct` and `is_raw_counts` are REMOVED.
- Error policy: corrupt/non-HDF5 → `Failure(allow_retries=False)`; `CountsUnavailable` → `Failure(allow_retries=False)`; below `min_cells`/`min_genes` → `Failure(allow_retries=False)` with the numbers in metadata, no output; read/write/transient → retriable `Failure`; plot failure → non-fatal note.
- End every git commit message with: `Claude-Session: https://claude.ai/code/session_01KBwUybm1J5fhKtQodn2hBf`
- Discipline: a task's deliverable is committed only after its tests are green.

---

### Task 1: stancounts — `get_counts()` + `CountsUnavailable`

**Files:**
- Create: `/home/users/chensj16/s/projects/stancounts/src/stancounts/counts.py`
- Modify: `/home/users/chensj16/s/projects/stancounts/src/stancounts/__init__.py`
- Modify: `/home/users/chensj16/s/projects/stancounts/pyproject.toml` (version bump)
- Test: `/home/users/chensj16/s/projects/stancounts/tests/test_counts.py`

**Interfaces:**
- Consumes: `stancounts.core.reverse_log1p`, `stancounts.detect.detect_normalization`.
- Produces: `stancounts.get_counts(adata, *, prefer_layers=..., exclude_layers=..., base="e", robust=True, allow_recovery=True, n_sample=200, seed=0) -> dict` returning `{"counts": <n_obs×n_vars aligned to adata.var_names>, "source": str}` (source ∈ `"layer:<name>"|"X"|"raw"|"recovered"`; recovered adds `"base"`). Raises `stancounts.CountsUnavailable`.

- [ ] **Step 1: Write the failing tests** — create `tests/test_counts.py`:

```python
"""Tests for stancounts.get_counts (source selection + recovery)."""

import anndata
import numpy as np
import pytest
import scipy.sparse as sp

import stancounts
from stancounts import CountsUnavailable, get_counts


def _counts(n_cells=120, n_genes=60, seed=0):
    rng = np.random.RandomState(seed)
    return rng.poisson(0.6, size=(n_cells, n_genes)).astype(np.float64)


def _lognorm(counts, target_sum=1e4):
    lib = counts.sum(axis=1, keepdims=True)
    lib[lib == 0] = 1
    return np.log1p(counts / lib * target_sum)


def test_from_counts_layer():
    counts = _counts()
    ad = anndata.AnnData(X=_lognorm(counts))
    ad.layers["counts"] = sp.csr_matrix(counts)
    res = get_counts(ad)
    assert res["source"] == "layer:counts"
    np.testing.assert_array_equal(np.asarray(res["counts"].todense()), counts)


def test_whitelist_beats_X():
    counts = _counts()
    ad = anndata.AnnData(X=sp.csr_matrix(counts))  # X is integer too
    ad.layers["raw_counts"] = sp.csr_matrix(counts)
    res = get_counts(ad)
    assert res["source"] == "layer:raw_counts"  # whitelist checked before X


def test_from_X_integer():
    counts = _counts()
    ad = anndata.AnnData(X=sp.csr_matrix(counts))
    res = get_counts(ad)
    assert res["source"] == "X"
    np.testing.assert_array_equal(np.asarray(res["counts"].todense()), counts)


def test_from_raw_aligned():
    counts = _counts(n_genes=60)
    ad = anndata.AnnData(X=_lognorm(counts[:, :40]))  # adata has 40 genes
    ad.var_names = [f"G{i}" for i in range(40)]
    raw = anndata.AnnData(X=sp.csr_matrix(counts))    # raw has 60 genes (superset)
    raw.var_names = [f"G{i}" for i in range(60)]
    ad.raw = raw
    res = get_counts(ad)
    assert res["source"] == "raw"
    assert res["counts"].shape == (counts.shape[0], 40)
    np.testing.assert_array_equal(np.asarray(res["counts"].todense()), counts[:, :40])


def test_recovered_from_log1p():
    counts = _counts()
    ad = anndata.AnnData(X=sp.csr_matrix(_lognorm(counts)))  # only log1p X, no counts
    res = get_counts(ad)
    assert res["source"] == "recovered"
    np.testing.assert_array_equal(np.asarray(res["counts"].todense()), counts)


def test_velocity_layers_not_mistaken_for_counts():
    counts = _counts()
    ad = anndata.AnnData(X=sp.csr_matrix(_lognorm(counts)))
    ad.layers["spliced"] = sp.csr_matrix(_counts(seed=1))    # integer, but velocity
    ad.layers["unspliced"] = sp.csr_matrix(_counts(seed=2))
    res = get_counts(ad)
    assert res["source"] == "recovered"  # spliced/unspliced excluded -> recover from X


def test_unavailable_raises():
    rng = np.random.RandomState(0)
    floats = rng.uniform(0.1, 5.0, size=(50, 30))  # non-integer, not log1p of counts
    ad = anndata.AnnData(X=floats)
    with pytest.raises(CountsUnavailable):
        get_counts(ad, allow_recovery=False)
```

- [ ] **Step 2: Run to verify failure**

Run: `mkdir -p $SCRATCH/.mplconfig && MPLCONFIGDIR=$SCRATCH/.mplconfig /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest /home/users/chensj16/s/projects/stancounts/tests/test_counts.py -q`
Expected: FAIL — `ImportError: cannot import name 'get_counts'` / `CountsUnavailable`.

- [ ] **Step 3: Implement** — create `src/stancounts/counts.py`:

```python
"""Obtain integer counts from any AnnData: existing layer, X, .raw, or recover."""

from __future__ import annotations

import numpy as np
import scipy.sparse as sp

from stancounts.core import reverse_log1p
from stancounts.detect import detect_normalization

DEFAULT_PREFER_LAYERS = (
    "counts", "count", "raw_counts", "counts_raw",
    "umi", "umis", "umi_counts", "X_counts",
)
DEFAULT_EXCLUDE_LAYERS = (
    "spliced", "unspliced", "ambiguous",
    "spliced_counts", "unspliced_counts", "matrix",
)


class CountsUnavailable(ValueError):
    """Integer counts could not be found in layers/X/.raw nor recovered from X."""


def _sample_idx(n_rows: int, n_sample: int, seed: int) -> np.ndarray:
    rng = np.random.RandomState(seed)
    if n_rows <= n_sample:
        return np.arange(n_rows)
    return rng.choice(n_rows, n_sample, replace=False)


def _is_integer_matrix(M, *, n_sample: int = 200, seed: int = 0) -> bool:
    """True if sampled nonzero values are finite, non-negative, near-integer."""
    if M is None:
        return False
    idx = _sample_idx(M.shape[0], n_sample, seed)
    if sp.issparse(M):
        csr = M.tocsr()
        parts = [csr.data[csr.indptr[i]:csr.indptr[i + 1]] for i in idx]
        data = np.concatenate(parts) if parts else np.array([], dtype=float)
    else:
        sub = np.asarray(M[idx])
        data = sub[sub != 0].ravel()
    if data.size == 0:
        return False
    data = data.astype(np.float64)
    if not np.all(np.isfinite(data)) or np.any(data < 0):
        return False
    return bool(np.allclose(data, np.round(data), rtol=0, atol=1e-6))


def get_counts(
    adata,
    *,
    prefer_layers=DEFAULT_PREFER_LAYERS,
    exclude_layers=DEFAULT_EXCLUDE_LAYERS,
    base: str = "e",
    robust: bool = True,
    allow_recovery: bool = True,
    n_sample: int = 200,
    seed: int = 0,
) -> dict:
    """Return integer counts (aligned to adata.var_names) from any AnnData.

    Priority: whitelist integer layer -> integer X -> integer .raw (aligned)
    -> reverse_log1p(X) if X is log1p. Raises CountsUnavailable otherwise.
    Velocity layers (spliced/unspliced/...) are never treated as counts.
    """
    exclude = set(exclude_layers)

    # 1. whitelist layers, in order
    for name in prefer_layers:
        if name in exclude or name not in adata.layers:
            continue
        M = adata.layers[name]
        if _is_integer_matrix(M, n_sample=n_sample, seed=seed):
            return {"counts": M, "source": f"layer:{name}"}

    # 2. X is integer
    if _is_integer_matrix(adata.X, n_sample=n_sample, seed=seed):
        return {"counts": adata.X, "source": "X"}

    # 3. .raw, aligned to adata.var_names (raw must cover all genes)
    raw = adata.raw
    if raw is not None and _is_integer_matrix(raw.X, n_sample=n_sample, seed=seed):
        raw_pos = {g: i for i, g in enumerate(list(raw.var_names))}
        if all(g in raw_pos for g in adata.var_names):
            cols = [raw_pos[g] for g in adata.var_names]
            rawX = raw.X
            aligned = rawX.tocsc()[:, cols].tocsr() if sp.issparse(rawX) else np.asarray(rawX)[:, cols]
            return {"counts": aligned, "source": "raw"}

    # 4. recover from log1p-normalized X
    if allow_recovery:
        det = detect_normalization(adata.X, n_sample=n_sample, seed=seed)
        if det["is_log1p"]:
            rec = reverse_log1p(adata.X, base=det["base"], robust=robust)
            return {"counts": rec["counts"], "source": "recovered", "base": det["base"]}

    raise CountsUnavailable(
        "no integer counts in whitelist layers / X / .raw, and X is not log1p-recoverable"
    )
```

- [ ] **Step 4: Export from `__init__.py`** — replace its body with:

```python
"""stancounts: Recover raw counts from log1p-normalized single-cell expression matrices."""

from stancounts.core import reverse_log1p, reverse_log1p_anndata
from stancounts.counts import CountsUnavailable, get_counts
from stancounts.detect import detect_normalization, is_log1p_normalized

__version__ = "0.2.0"
__all__ = [
    "reverse_log1p",
    "reverse_log1p_anndata",
    "get_counts",
    "CountsUnavailable",
    "detect_normalization",
    "is_log1p_normalized",
]
```

And bump `pyproject.toml` `version = "0.1.0"` → `version = "0.2.0"`.

- [ ] **Step 5: Run to verify pass**

Run: `MPLCONFIGDIR=$SCRATCH/.mplconfig /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest /home/users/chensj16/s/projects/stancounts/tests/test_counts.py -q`
Expected: PASS (7 passed). (Editable install → no reinstall needed.)

- [ ] **Step 6: Commit (in the stancounts repo)**

```bash
cd /home/users/chensj16/s/projects/stancounts
git add src/stancounts/counts.py src/stancounts/__init__.py pyproject.toml tests/test_counts.py
git commit -F - <<'EOF'
feat: get_counts() — obtain integer counts from any AnnData (layer/X/.raw/recover)

New public get_counts() + CountsUnavailable. Whitelists count-like layers,
excludes velocity layers (spliced/unspliced/...), integer-verifies, falls back
to .raw (aligned to var_names) then reverse_log1p. Bumps to 0.2.0.

Claude-Session: https://claude.ai/code/session_01KBwUybm1J5fhKtQodn2hBf
EOF
```

---

### Task 2: pipeline settings — add `output_dir` + `min_genes`, remove `max_mito_pct`

**Files:**
- Modify: `PROJ/src/sc_curation_pipeline/defs/settings.py`
- Modify: `PROJ/tests/test_settings.py`
- Modify: `PROJ/tests/conftest.py` (settings_factory)
- Modify: `PROJ/tests/test_sensor.py` (5 `CurationSettings(...)` call sites)

**Interfaces:**
- Produces: `CurationSettings` fields `watch_dir, done_marker, h5ad_glob, scan_interval_sec, min_cells, min_genes, output_dir` (NO `max_mito_pct`). `build_curation_settings()` requires both `SC_CURATION_WATCH_DIR` and `SC_CURATION_OUTPUT_DIR` (non-empty), reads `SC_CURATION_MIN_GENES` (default 5000).

- [ ] **Step 1: Update settings tests** — in `tests/test_settings.py`: delete the three `max_mito_pct` assertions (lines ~12, ~27, ~37) and the `SC_CURATION_MAX_MITO_PCT` env lines (~21, ~33, ~81, ~85); add `min_genes` + `output_dir` coverage:

```python
def test_field_defaults(settings_factory):
    cs = settings_factory()
    assert cs.done_marker == ".done"
    assert cs.h5ad_glob == "*.h5ad"
    assert cs.scan_interval_sec == 30
    assert cs.min_cells == 100
    assert cs.min_genes == 5000


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
```

Also update `test_build_curation_settings_env_defaults` and `test_build_curation_settings_env_overrides` and `test_malformed_numeric_env_degrades_to_default`: set `SC_CURATION_OUTPUT_DIR` and replace any `max_mito_pct` assertion with `min_genes` (default 5000). Keep the existing `test_watch_dir_missing_or_empty_raises` as-is.

- [ ] **Step 2: Run to verify failure**

Run: `MPLCONFIGDIR=$SCRATCH/.mplconfig /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_settings.py -q` (from `PROJ`)
Expected: FAIL — `min_genes`/`output_dir` unknown; `max_mito_pct` removed errors.

- [ ] **Step 3: Implement settings** — in `settings.py`, replace the `CurationSettings` class body and the `build_curation_settings` return:

```python
class CurationSettings(dg.ConfigurableResource):
    """Env-driven configuration for the h5ad curation + QC pipeline."""

    watch_dir: str
    output_dir: str
    done_marker: str = ".done"
    h5ad_glob: str = "*.h5ad"
    scan_interval_sec: int = 30
    min_cells: int = 100
    min_genes: int = 5000
```

In `build_curation_settings()`, after the `watch_dir` block add an `output_dir` block and update the return:

```python
    output_dir = os.environ.get("SC_CURATION_OUTPUT_DIR", "").strip()
    if not output_dir:
        raise ValueError(
            "SC_CURATION_OUTPUT_DIR is required and must be a non-empty path "
            "(the standardized .h5ad output directory; never the source dir)."
        )
    return CurationSettings(
        watch_dir=watch_dir,
        output_dir=output_dir,
        done_marker=os.getenv("SC_CURATION_DONE_MARKER", ".done"),
        h5ad_glob=os.getenv("SC_CURATION_H5AD_GLOB", "*.h5ad"),
        scan_interval_sec=_env_int("SC_CURATION_SCAN_INTERVAL_SEC", 30),
        min_cells=_env_int("SC_CURATION_MIN_CELLS", 100),
        min_genes=_env_int("SC_CURATION_MIN_GENES", 5000),
    )
```

(Delete the `max_mito_pct=...` line and the now-unused `_env_float` only if nothing else uses it — `_env_float` is now unused; remove it and its definition.)

- [ ] **Step 4: Update conftest + sensor call sites**

In `tests/conftest.py` `settings_factory` `kwargs`: remove `max_mito_pct=20.0`; add `output_dir=overrides.pop("output_dir", str(tmp_path / "out"))` handling and `min_genes=5000`. Concretely:

```python
    def _make(**overrides):
        watch = overrides.pop("watch_dir", str(tmp_path / "watch"))
        Path(watch).mkdir(parents=True, exist_ok=True)
        out = overrides.pop("output_dir", str(tmp_path / "out"))
        kwargs = dict(
            watch_dir=watch,
            output_dir=out,
            done_marker=".done",
            h5ad_glob="*.h5ad",
            scan_interval_sec=30,
            min_cells=100,
            min_genes=5000,
        )
        kwargs.update(overrides)
        return CurationSettings(**kwargs)
```

In `tests/test_sensor.py`, every `CurationSettings(watch_dir=watch...)` (5 sites: lines ~45, 66, 82, 94, 111) gains `output_dir=str(tmp_path / "out")`. Each test already has `tmp_path`. Example: `CurationSettings(watch_dir=watch, output_dir=str(tmp_path / "out"), scan_interval_sec=30)`.

- [ ] **Step 5: Run to verify pass**

Run: `MPLCONFIGDIR=$SCRATCH/.mplconfig /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_settings.py tests/test_sensor.py -q` (from `PROJ`)
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
git add src/sc_curation_pipeline/defs/settings.py tests/test_settings.py tests/conftest.py tests/test_sensor.py
git commit -F - <<'EOF'
feat(settings): add output_dir (required) + min_genes (5000); remove max_mito_pct

Claude-Session: https://claude.ai/code/session_01KBwUybm1J5fhKtQodn2hBf
EOF
```

---

### Task 3: pipeline `defs/standardize.py` (new)

**Files:**
- Create: `PROJ/src/sc_curation_pipeline/defs/standardize.py`
- Test: `PROJ/tests/test_standardize.py`

**Interfaces:**
- Consumes: scanpy, anndata, the counts matrix from `stancounts.get_counts`.
- Produces: `build_standardized_adata(adata, counts, *, target_sum=1e4) -> AnnData` (mutates `adata` in place and returns it: sets `layers["counts"]`, `X = normalize_total(target_sum)+log1p`, preserves other layers/annotations). `write_standardized(adata, out_path) -> None` (makedirs + `write_h5ad`).

- [ ] **Step 1: Write the failing tests** — `tests/test_standardize.py`:

```python
import os

import anndata
import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.standardize import build_standardized_adata, write_standardized


def _counts(n=40, g=20, seed=0):
    return np.random.RandomState(seed).poisson(0.7, size=(n, g)).astype(np.float64)


def test_build_sets_counts_layer_and_lognorm_X():
    counts = _counts()
    ad = anndata.AnnData(X=np.zeros_like(counts))
    ad.layers["spliced"] = sp.csr_matrix(_counts(seed=9))  # velocity layer preserved
    out = build_standardized_adata(ad, sp.csr_matrix(counts), target_sum=1e4)

    cl = out.layers["counts"]
    np.testing.assert_array_equal(np.asarray(cl.todense()), counts)
    # X == log1p(normalize_total(counts, 1e4))
    lib = counts.sum(axis=1, keepdims=True); lib[lib == 0] = 1
    expected = np.log1p(counts / lib * 1e4)
    X = out.X.todense() if sp.issparse(out.X) else out.X
    np.testing.assert_allclose(np.asarray(X), expected, rtol=1e-4, atol=1e-4)
    assert "spliced" in out.layers  # velocity preserved


def test_write_standardized_creates_file(tmp_path):
    counts = _counts()
    ad = anndata.AnnData(X=np.zeros_like(counts))
    out = build_standardized_adata(ad, sp.csr_matrix(counts))
    path = str(tmp_path / "nested" / "sample.h5ad")
    write_standardized(out, path)
    assert os.path.isfile(path)
    back = anndata.read_h5ad(path)
    assert "counts" in back.layers
```

- [ ] **Step 2: Run to verify failure**

Run: `MPLCONFIGDIR=$SCRATCH/.mplconfig /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_standardize.py -q`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement** — `src/sc_curation_pipeline/defs/standardize.py`:

```python
"""Build & write the standardized counts-bearing AnnData for one sample."""

import os

import numpy as np
import scanpy as sc


def build_standardized_adata(adata, counts, *, target_sum: float = 1e4):
    """Set layers['counts']=counts (integer) and X=normalize_total+log1p(counts).

    Mutates `adata` in place and returns it. Other layers (e.g. spliced/unspliced)
    and obs/var/obsm/obsp/uns are preserved. The counts layer keeps integer dtype;
    normalization runs on a float copy in X.
    """
    adata.layers["counts"] = counts
    adata.X = counts.astype(np.float32)  # float copy for normalization; counts layer untouched
    sc.pp.normalize_total(adata, target_sum=target_sum)
    sc.pp.log1p(adata)
    return adata


def write_standardized(adata, out_path: str) -> None:
    """Write the standardized AnnData to out_path (creating parent dirs)."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    adata.write_h5ad(out_path)
```

- [ ] **Step 4: Run to verify pass**

Run: `MPLCONFIGDIR=$SCRATCH/.mplconfig /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_standardize.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
git add src/sc_curation_pipeline/defs/standardize.py tests/test_standardize.py
git commit -F - <<'EOF'
feat(standardize): build_standardized_adata (counts layer + lognorm X) + write_standardized

Claude-Session: https://claude.ai/code/session_01KBwUybm1J5fhKtQodn2hBf
EOF
```

---

### Task 4: pipeline `qc.py` — `compute_count_qc` + rewritten `h5ad_qc` asset

**Files:**
- Modify: `PROJ/src/sc_curation_pipeline/defs/qc.py`
- Modify: `PROJ/tests/test_qc.py` (rewrite)
- Modify: `PROJ/tests/conftest.py` (add an h5ad builder that supports layers/raw)

**Interfaces:**
- Consumes: `stancounts.get_counts`/`CountsUnavailable`, `build_standardized_adata`/`write_standardized` (Task 3), `render_qc_panel` (plots.py), `resolve_h5ad_path`, `CurationSettings` (Task 2), `path_for_partition_key`.
- Produces: `compute_count_qc(counts, var_names) -> dict` (keys: `n_cells, n_vars, n_genes_detected, total_counts, median_counts_per_cell, median_genes_per_cell, density, sparsity, mito_pct, ribo_pct, per_cell{counts,genes,mito_pct}`); `output_path_for(output_dir, partition_key, src_path) -> str` (pure, no context — testable directly); rewritten `h5ad_qc` asset (no AssetCheckSpec, no AssetCheckResult).

- [ ] **Step 1: Add an h5ad builder to conftest** — append to `tests/conftest.py`:

```python
@pytest.fixture
def write_adata():
    """Write an h5ad with given X and optional layers/raw. Returns the path."""
    def _make(path, X, *, var_names=None, layers=None, raw_X=None, raw_var_names=None):
        n_obs, n_vars = X.shape
        if var_names is None:
            var_names = [f"GENE{i}" for i in range(n_vars)]
        ad = anndata.AnnData(X=X)
        ad.var_names = list(var_names)
        ad.obs_names = [f"cell{i}" for i in range(n_obs)]
        if layers:
            for k, v in layers.items():
                ad.layers[k] = v
        if raw_X is not None:
            rv = raw_var_names or [f"GENE{i}" for i in range(raw_X.shape[1])]
            raw = anndata.AnnData(X=raw_X)
            raw.var_names = list(rv)
            ad.raw = raw
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        ad.write_h5ad(path)
        return path
    return _make
```

(Add `import anndata` at the top of conftest if not present — it already imports `anndata as ad`; reuse as `anndata` by adding `import anndata`, or use the existing `ad` alias. Use the existing `ad` alias: replace `anndata.AnnData`/`anndata.read_h5ad` with `ad.AnnData` to avoid a second import.)

- [ ] **Step 2: Write the failing tests** — replace `tests/test_qc.py` entirely:

```python
import os

import dagster as dg
import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.qc import (
    compute_count_qc, h5ad_qc, h5ad_qc_job, output_path_for,
)
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for
from sc_curation_pipeline.defs.partitions import h5ad_partitions


def _counts(n=200, g=8000, seed=0):
    # n_genes_detected must be able to exceed default min_genes(5000) in pass tests
    rng = np.random.RandomState(seed)
    m = rng.poisson(0.5, size=(n, g)).astype(np.float64)
    return m


# ---- compute_count_qc ----

def test_compute_count_qc_dense():
    counts = np.array([[1.0, 0.0, 2.0], [0.0, 3.0, 0.0]])
    qc = compute_count_qc(counts, ["GENE0", "MT-CO1", "RPS3"])
    assert qc["n_cells"] == 2
    assert qc["n_vars"] == 3
    assert qc["n_genes_detected"] == 3  # all 3 genes seen in >=1 cell
    assert qc["total_counts"] == 6.0
    assert abs(qc["mito_pct"] - 50.0) < 1e-6      # MT-CO1 col sum 3 / 6
    np.testing.assert_array_equal(qc["per_cell"]["counts"], [3.0, 3.0])
    np.testing.assert_allclose(qc["per_cell"]["mito_pct"], [0.0, 100.0])


def test_compute_count_qc_detected_excludes_allzero_genes():
    counts = np.array([[1.0, 0.0, 0.0], [2.0, 0.0, 0.0]])  # only gene0 detected
    qc = compute_count_qc(counts, ["G0", "G1", "G2"])
    assert qc["n_vars"] == 3
    assert qc["n_genes_detected"] == 1


# ---- asset helpers ----

def _materialize(path, watch, out, key, settings, instance):
    return dg.materialize(
        [h5ad_qc], partition_key=key, instance=instance,
        resources={"curation": settings}, tags={"sc/h5ad_path": path},
        raise_on_error=False,
    )


def _setup(tmp_path, folder_name, X, write_adata, *, layers=None, min_cells=100, min_genes=5000, var_names=None):
    watch = str(tmp_path / "watch"); out = str(tmp_path / "out")
    folder = os.path.join(watch, folder_name)
    path = write_adata(os.path.join(folder, "a.h5ad"), X, var_names=var_names, layers=layers)
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch, output_dir=out, min_cells=min_cells, min_genes=min_genes)
    inst = dg.DagsterInstance.ephemeral()
    inst.add_dynamic_partitions(h5ad_partitions.name, [key])
    return watch, out, folder, path, key, settings, inst


def test_h5ad_qc_writes_output_and_qc(tmp_path, write_adata):
    counts = _counts()
    lognorm = sp.csr_matrix(np.log1p(counts / counts.sum(1, keepdims=True) * 1e4))
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "GSE1_s", lognorm, write_adata, layers={"counts": sp.csr_matrix(counts)})
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success

    md = res.asset_materializations_for_node("h5ad_qc")[0].metadata
    assert md["n_cells"].value == 200
    assert "data:image/png;base64," in md["qc_plots"].value
    assert md["counts_source"].value == "layer:counts"
    out_path = output_path_for(out, key, path)
    assert os.path.isfile(out_path)              # written to output dir
    assert os.path.isfile(path)                  # source still present, untouched


def test_h5ad_qc_rejects_too_few_cells(tmp_path, write_adata):
    counts = _counts(n=10)  # < min_cells 100
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "tiny", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)}, min_cells=100, min_genes=1)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    msg = [e for e in res.get_step_failure_events() if e.step_key == "h5ad_qc"][0].event_specific_data.error.message
    assert "min_cells" in msg
    assert not os.path.isfile(output_path_for(out, key, path))  # no output


def test_h5ad_qc_rejects_too_few_genes(tmp_path, write_adata):
    counts = _counts(n=200, g=100)  # only 100 genes -> < min_genes 5000
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "fewgenes", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)}, min_cells=1, min_genes=5000)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    msg = [e for e in res.get_step_failure_events() if e.step_key == "h5ad_qc"][0].event_specific_data.error.message
    assert "min_genes" in msg
    assert not os.path.isfile(output_path_for(out, key, path))


def test_h5ad_qc_no_counts_fails(tmp_path, write_adata):
    rng = np.random.RandomState(0)
    floats = rng.uniform(0.1, 5.0, size=(200, 100))  # not integer, not log1p
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "nocounts", floats, write_adata, min_cells=1, min_genes=1)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    assert not os.path.isfile(output_path_for(out, key, path))


def test_h5ad_qc_corrupt_fast_fail(tmp_path):
    watch = str(tmp_path / "watch"); out = str(tmp_path / "out")
    folder = os.path.join(watch, "bad"); os.makedirs(folder)
    path = os.path.join(folder, "broken.h5ad")
    with open(path, "wb") as fh:
        fh.write(b"not an hdf5 file")
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch, output_dir=out)
    inst = dg.DagsterInstance.ephemeral()
    inst.add_dynamic_partitions(h5ad_partitions.name, [key])
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    msg = [e for e in res.get_step_failure_events() if e.step_key == "h5ad_qc"][0].event_specific_data.error.message
    assert "HDF5" in msg and "max_retries" not in msg


def test_h5ad_qc_plot_failure_non_fatal(tmp_path, write_adata, monkeypatch):
    from sc_curation_pipeline.defs import plots as plotsmod
    counts = _counts()
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "plotfail", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)})
    monkeypatch.setattr(plotsmod, "render_qc_panel", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is True
    md = res.asset_materializations_for_node("h5ad_qc")[0].metadata
    assert "图未生成" in md["qc_plots"].value
    assert os.path.isfile(output_path_for(out, key, path))  # output still written


def test_h5ad_qc_write_failure_retried(tmp_path, write_adata, monkeypatch):
    from sc_curation_pipeline.defs import qc as qcmod
    counts = _counts()
    watch, out, folder, path, key, settings, inst = _setup(
        tmp_path, "writefail", sp.csr_matrix(counts), write_adata,
        layers={"counts": sp.csr_matrix(counts)})
    calls = []
    def boom(adata, out_path):
        calls.append(out_path); raise OSError("transient disk hiccup")
    monkeypatch.setattr(qcmod, "write_standardized", boom)
    res = _materialize(path, watch, out, key, settings, inst)
    assert res.success is False
    assert len(calls) == 3  # initial + 2 retries


def test_h5ad_qc_job_targets_asset():
    assert h5ad_qc_job.name == "h5ad_qc_job"
```

- [ ] **Step 3: Run to verify failure**

Run: `MPLCONFIGDIR=$SCRATCH/.mplconfig /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_qc.py -q`
Expected: FAIL — `compute_count_qc`/`output_path_for` missing, asset signature changed.

- [ ] **Step 4: Implement `qc.py`** — replace the file contents with (keep `resolve_h5ad_path` unchanged from the current file; shown trimmed here as `# ... unchanged ...`):

```python
import os

import dagster as dg
import numpy as np
import scipy.sparse as sp
import anndata as ad
import h5py

import stancounts

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.settings import CurationSettings, path_for_partition_key
from sc_curation_pipeline.defs.standardize import build_standardized_adata, write_standardized

H5AD_PATH_TAG = "sc/h5ad_path"


def compute_count_qc(counts, var_names) -> dict:
    """QC metrics computed on an in-memory counts matrix (sparse or dense)."""
    is_sparse = sp.issparse(counts)
    C = counts.tocsr() if is_sparse else np.asarray(counts)
    n_cells, n_vars = int(C.shape[0]), int(C.shape[1])

    up = np.char.upper(np.asarray([str(v) for v in var_names], dtype=str))
    mito_mask = np.char.startswith(up, "MT-")
    ribo_mask = np.char.startswith(up, "RPS") | np.char.startswith(up, "RPL")

    if is_sparse:
        counts_per_cell = np.asarray(C.sum(axis=1)).ravel().astype(np.float64)
        genes_per_cell = C.getnnz(axis=1).astype(np.float64)
        detected_per_gene = np.asarray(C.getnnz(axis=0)).ravel()
        nnz_total = int(C.nnz)
        mito_per_cell = (np.asarray(C[:, mito_mask].sum(axis=1)).ravel()
                         if mito_mask.any() else np.zeros(n_cells))
        ribo_per_cell = (np.asarray(C[:, ribo_mask].sum(axis=1)).ravel()
                         if ribo_mask.any() else np.zeros(n_cells))
    else:
        counts_per_cell = C.sum(axis=1).astype(np.float64)
        genes_per_cell = (C != 0).sum(axis=1).astype(np.float64)
        detected_per_gene = (C != 0).sum(axis=0)
        nnz_total = int((C != 0).sum())
        mito_per_cell = C[:, mito_mask].sum(axis=1) if mito_mask.any() else np.zeros(n_cells)
        ribo_per_cell = C[:, ribo_mask].sum(axis=1) if ribo_mask.any() else np.zeros(n_cells)

    total = n_cells * n_vars
    total_counts = float(counts_per_cell.sum())
    n_genes_detected = int((np.asarray(detected_per_gene).ravel() > 0).sum())
    with np.errstate(divide="ignore", invalid="ignore"):
        mito_pct_per_cell = np.where(counts_per_cell > 0,
                                     100.0 * mito_per_cell / counts_per_cell, 0.0)
    return {
        "n_cells": n_cells,
        "n_vars": n_vars,
        "n_genes_detected": n_genes_detected,
        "total_counts": total_counts,
        "median_counts_per_cell": float(np.median(counts_per_cell)) if n_cells else 0.0,
        "median_genes_per_cell": float(np.median(genes_per_cell)) if n_cells else 0.0,
        "density": (nnz_total / total) if total else 0.0,
        "sparsity": (1.0 - nnz_total / total) if total else 0.0,
        "mito_pct": (100.0 * mito_per_cell.sum() / total_counts) if total_counts else 0.0,
        "ribo_pct": (100.0 * ribo_per_cell.sum() / total_counts) if total_counts else 0.0,
        "per_cell": {"counts": counts_per_cell, "genes": genes_per_cell, "mito_pct": mito_pct_per_cell},
    }


def resolve_h5ad_path(context, settings):
    # ... unchanged from current qc.py ...
    ...


def output_path_for(output_dir, partition_key, src_path) -> str:
    """Mirror the sample's relative folder under output_dir (never the source)."""
    rel_folder = path_for_partition_key(partition_key)
    return os.path.join(output_dir, rel_folder, os.path.basename(src_path))


@dg.asset(
    partitions_def=h5ad_partitions,
    group_name="curation",
    retry_policy=dg.RetryPolicy(max_retries=2),
)
def h5ad_qc(context: dg.AssetExecutionContext, curation: CurationSettings):
    """Standardize one sample (counts layer + lognorm X), write it out, QC on counts."""
    path = resolve_h5ad_path(context, curation)
    if not h5py.is_hdf5(path):
        raise dg.Failure(
            description=f"not a valid HDF5/h5ad file: {path!r} (file signature not found)",
            metadata={"partition": dg.MetadataValue.text(context.partition_key),
                      "h5ad_path": dg.MetadataValue.path(path)},
            allow_retries=False,
        )
    try:
        adata = ad.read_h5ad(path)
    except Exception as exc:  # transient I/O on a valid HDF5 -> retriable
        raise dg.Failure(
            description=f"failed to read h5ad at {path!r}: {exc}",
            metadata={"error": dg.MetadataValue.text(repr(exc))},
        )

    try:
        res = stancounts.get_counts(adata)
    except stancounts.CountsUnavailable as exc:
        raise dg.Failure(
            description=f"no recoverable counts for {path!r}: {exc}",
            metadata={"h5ad_path": dg.MetadataValue.path(path)},
            allow_retries=False,
        )
    counts, counts_source = res["counts"], res["source"]

    qc = compute_count_qc(counts, adata.var_names)

    if qc["n_cells"] < curation.min_cells:
        raise dg.Failure(
            description=f"rejected: n_cells {qc['n_cells']} < min_cells {curation.min_cells}",
            metadata={"n_cells": dg.MetadataValue.int(qc["n_cells"]),
                      "min_cells": dg.MetadataValue.int(curation.min_cells)},
            allow_retries=False,
        )
    if qc["n_genes_detected"] < curation.min_genes:
        raise dg.Failure(
            description=f"rejected: n_genes_detected {qc['n_genes_detected']} < min_genes {curation.min_genes}",
            metadata={"n_genes_detected": dg.MetadataValue.int(qc["n_genes_detected"]),
                      "min_genes": dg.MetadataValue.int(curation.min_genes)},
            allow_retries=False,
        )

    out_path = output_path_for(curation.output_dir, context.partition_key, path)
    try:
        std = build_standardized_adata(adata, counts)
        write_standardized(std, out_path)
    except Exception as exc:  # disk/write hiccup -> retriable
        raise dg.Failure(
            description=f"failed to write standardized h5ad to {out_path!r}: {exc}",
            metadata={"error": dg.MetadataValue.text(repr(exc))},
        )

    try:
        from sc_curation_pipeline.defs.plots import render_qc_panel
        pc = qc["per_cell"]
        qc_plots = dg.MetadataValue.md(render_qc_panel(
            pc["counts"], pc["genes"], pc["mito_pct"], sample_label=context.partition_key))
    except Exception as exc:  # noqa: BLE001 - plotting is non-fatal
        context.log.warning(f"QC plot rendering failed: {exc!r}")
        qc_plots = dg.MetadataValue.md(f"⚠️ 图未生成: {exc}")

    yield dg.MaterializeResult(
        metadata={
            "output_path": dg.MetadataValue.path(out_path),
            "counts_source": dg.MetadataValue.text(counts_source),
            "source_h5ad": dg.MetadataValue.path(path),
            "qc_plots": qc_plots,
            "n_cells": dg.MetadataValue.int(qc["n_cells"]),
            "n_genes_detected": dg.MetadataValue.int(qc["n_genes_detected"]),
            "n_vars": dg.MetadataValue.int(qc["n_vars"]),
            "total_counts": dg.MetadataValue.float(qc["total_counts"]),
            "median_counts_per_cell": dg.MetadataValue.float(qc["median_counts_per_cell"]),
            "median_genes_per_cell": dg.MetadataValue.float(qc["median_genes_per_cell"]),
            "density": dg.MetadataValue.float(qc["density"]),
            "sparsity": dg.MetadataValue.float(qc["sparsity"]),
            "mito_pct": dg.MetadataValue.float(qc["mito_pct"]),
            "ribo_pct": dg.MetadataValue.float(qc["ribo_pct"]),
            "layers": dg.MetadataValue.json(list(std.layers.keys())),
            "obsm": dg.MetadataValue.json(list(std.obsm.keys())),
        }
    )


h5ad_qc_job = dg.define_asset_job(
    name="h5ad_qc_job",
    selection=dg.AssetSelection.assets("h5ad_qc"),
)
```

> Note: paste the CURRENT `resolve_h5ad_path` body verbatim where `# ... unchanged ...` is. Remove the old `compute_qc`, the `AssetCheckSpec` list, and all `AssetCheckResult` yields.

- [ ] **Step 5: Run to verify pass**

Run: `MPLCONFIGDIR=$SCRATCH/.mplconfig /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_qc.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
git add src/sc_curation_pipeline/defs/qc.py tests/test_qc.py tests/conftest.py
git commit -F - <<'EOF'
feat(qc): h5ad_qc now standardizes (counts+lognorm) & writes output; QC on counts; hard gates

compute_count_qc runs on counts; asset: load -> get_counts -> min_cells/min_genes
hard gates (fast-fail, no output) -> standardize+write to SC_CURATION_OUTPUT_DIR
-> QC + plots. Removes all asset checks (max_mito_pct/is_raw_counts gone).

Claude-Session: https://claude.ai/code/session_01KBwUybm1J5fhKtQodn2hBf
EOF
```

---

### Task 5: dependency wiring (pyproject) + README + full green

**Files:**
- Modify: `PROJ/pyproject.toml`
- Modify: `PROJ/README.md`

**Interfaces:** none (packaging + docs).

- [ ] **Step 1: Declare stancounts as a dependency (uv source → local editable)** — in `PROJ/pyproject.toml` add `stancounts` to the dev group and pin it to the local path so `uv sync` uses the editable local checkout, not PyPI 0.1.0:

```toml
[dependency-groups]
dev = [
    "dagster-dg-cli",
    "dagster-webserver",
    "pytest>=8",
    "scanpy>=1.11",
    "anndata>=0.12",
    "matplotlib>=3.7",
    "stancounts",
]

[tool.uv.sources]
stancounts = { path = "/scratch/users/chensj16/projects/stancounts", editable = true }
```

- [ ] **Step 2: Update README** — add a short subsection under the QC/behavior section documenting: the step now writes a standardized `.h5ad` (counts in `layers["counts"]`, `X=normalize_total(1e4)+log1p`, velocity layers preserved) to `SC_CURATION_OUTPUT_DIR`; counts sourcing order (layer/X/.raw/recover via stancounts); hard gates `SC_CURATION_MIN_CELLS` (100) and `SC_CURATION_MIN_GENES` (5000) reject sub-threshold samples (fast-fail, no output); `SC_CURATION_OUTPUT_DIR` is required; `max_mito_pct`/`is_raw_counts` removed. Also add `SC_CURATION_OUTPUT_DIR` and `SC_CURATION_MIN_GENES` to the env-var table and remove `SC_CURATION_MAX_MITO_PCT`.

- [ ] **Step 3: Run the FULL suite (both repos)**

```bash
mkdir -p $SCRATCH/.mplconfig
MPLCONFIGDIR=$SCRATCH/.mplconfig /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest \
  /home/users/chensj16/s/projects/stancounts/tests/test_counts.py \
  /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline/tests/ -q
```
Expected: all PASS (pipeline tests + stancounts get_counts tests). Real-data stancounts tests skip cleanly.

- [ ] **Step 4: Commit**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
git add pyproject.toml README.md
git commit -F - <<'EOF'
chore: declare stancounts (local editable) dep; document standardized-counts output

Claude-Session: https://claude.ai/code/session_01KBwUybm1J5fhKtQodn2hBf
EOF
```

---

## Self-review notes (coverage)

- Spec §3 flow → Task 4 asset. §4 get_counts → Task 1. §5 components → Tasks 2/3/4/5. §7 normalization → Task 3. §8 output naming → `output_path_for` (Task 4). §9 hard gates → Task 4. §10 QC metadata/plots → Task 4 (plots.py reused). §11 error matrix → Task 4 (read=retriable, write=retriable, get_counts/gates=fast-fail, plot=non-fatal). §12 deps → Task 1 (already editable) + Task 5. §14 tests → each task's tests.
- Type consistency: `get_counts` returns `{"counts","source"}` (Task 1) consumed in Task 4; `build_standardized_adata(adata, counts)` / `write_standardized(adata, out_path)` (Task 3) consumed in Task 4; `compute_count_qc(counts, var_names)` keys match the asset's metadata reads.
- YAGNI: no IOManager, no second asset, no scanpy plotting (plots.py reused).
```

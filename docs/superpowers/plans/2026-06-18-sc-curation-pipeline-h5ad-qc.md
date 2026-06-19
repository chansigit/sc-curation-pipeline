# h5ad QC Curation Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dagster pipeline that watches a configurable directory for newly-uploaded single-cell `.h5ad` files (one folder = one sample, completion signalled by a `.done` marker), runs lightweight QC on each, and surfaces every result purely as Dagster asset metadata + asset checks.

**Architecture:** A marker-driven `@sensor` recursively scans `SC_CURATION_WATCH_DIR`, registers each completed folder as a key on a `DynamicPartitionsDefinition("h5ad_samples")`, and fires one idempotent `RunRequest` per new sample. A partitioned `h5ad_qc` asset resolves the folder's `.h5ad` path, computes structural + count QC via a pure `compute_qc` function (anndata backed-mode reads, manual chunked counts to avoid loading the whole matrix), emits `MaterializeResult` metadata and threshold `AssetCheckResult`s, and raises `dagster.Failure` on hard errors with `RetryPolicy(max_retries=2)` for transient I/O. All config flows from env vars through a `CurationSettings(ConfigurableResource)`.

**Tech Stack:** Dagster 1.13.10 (assets, dynamic partitions, sensors, asset checks, ConfigurableResource), anndata 0.12.10, scanpy 1.11.5, scipy, numpy, pandas, pytest; the project is editable-installed into the `dl2025` uv venv.

## Global Constraints

- Dagster == 1.13.10.
- Runtime env = dl2025 venv (Python 3.12) at `/scratch/users/chensj16/venvs/dl2025/.venv`; run `dg`/`pytest` from there. (Project metadata pins `requires-python >=3.10,<3.15`.)
- Output is Dagster asset metadata + asset checks ONLY — no external output files, no catalog/DB.
- Never modify or write into source data directories beyond what the uploader places there.
- Discovery is driven by a `.done` marker file in each sample folder; one folder = one h5ad = one partition.
- Partition key = the h5ad's folder path relative to `SC_CURATION_WATCH_DIR` (path separators sanitized); absolute path carried in run tags + metadata.
- Config via env vars: `SC_CURATION_WATCH_DIR` (required), `SC_CURATION_DONE_MARKER='.done'`, `SC_CURATION_H5AD_GLOB='*.h5ad'`, `SC_CURATION_SCAN_INTERVAL_SEC=30`, `SC_CURATION_MIN_CELLS=100`, `SC_CURATION_MAX_MITO_PCT=20`.
- Error model: hard errors (unreadable/corrupt h5ad, `.done` present but no h5ad) -> raise `dagster.Failure` (red run); soft QC gates (min cells, max mito%, raw-counts) -> `AssetCheckResult(passed=False)` (red check, green run); `RetryPolicy(max_retries=2)` on the asset.
- Partition def name is exactly `h5ad_samples`. Package is `sc_curation_pipeline`. New code lives under `src/sc_curation_pipeline/defs/`; tests under `tests/`.
- Defs auto-load via `src/sc_curation_pipeline/definitions.py` (`load_from_defs_folder`). Resources can ONLY be registered by returning them inside a `Definitions(resources={...})` object — a bare module-scope resource instance is silently ignored.

## File Structure

| File | Create/Modify | Single responsibility |
|---|---|---|
| `.gitignore` | Modify | Ensure `.dagster_home/`, `.dg/`, venv, `__pycache__`, `*.egg-info` are ignored (most already present). |
| `pyproject.toml` | Modify | Add `pytest`, `scanpy`, `anndata` to the `dev` dependency group (lines 9-13). |
| `src/sc_curation_pipeline/defs/settings.py` | Create | `CurationSettings(ConfigurableResource)` + `build_curation_settings()` env-default factory + `partition_key_for(...)` / `path_for_partition_key(...)` key helpers. |
| `src/sc_curation_pipeline/defs/partitions.py` | Create | `h5ad_partitions = DynamicPartitionsDefinition(name="h5ad_samples")`. |
| `src/sc_curation_pipeline/defs/qc.py` | Create | `compute_qc(path, memory_cap)` pure function; `h5ad_qc` partitioned asset with check_specs + RetryPolicy; `h5ad_qc_job`. |
| `src/sc_curation_pipeline/defs/sensors.py` | Create | `discover_samples(watch_dir, done_marker, h5ad_glob)` pure scanner + `watch_h5ad_dir` sensor. |
| `src/sc_curation_pipeline/defs/registration.py` | Create | One `@definitions` function bundling asset, job, sensor, and the `curation` resource (the only place resources are registered). |
| `tests/conftest.py` | Create | Fixtures: synthetic AnnData writer, temp watch-dir builder, settings factory. |
| `tests/test_settings.py` | Create | Tests for env defaults + key sanitization round-trip. |
| `tests/test_qc.py` | Create | Tests for `compute_qc` (sparse/dense/normalized/empty) + `h5ad_qc` materialize (metadata, checks, Failure). |
| `tests/test_sensor.py` | Create | Tests for `discover_samples` + `watch_h5ad_dir` (registration, dedup, missing-done, missing-h5ad). |

---

### Task 1: Bootstrap — git init, .gitignore, initial commit

**Files:**
- Modify: `.gitignore` (append two lines)
- Create: git repository at project root

**Interfaces:**
- Consumes: nothing.
- Produces: a committed baseline of the existing scaffold; all later tasks end with a commit.

- [ ] **Step 1: Establish precondition check** — infrastructure task (no code under test yet). Record whether the directory is already a git repo (always prints a definitive answer):

```bash
test -d /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline/.git && echo "REPO EXISTS" || echo "NOT A REPO YET"
```

- [ ] **Step 2: Run the precondition check** — run the command above; expected output is exactly one of these two lines:

```
NOT A REPO YET
```

(If it prints `REPO EXISTS`, a `.git` already exists; skip `git init` in Step 3.)

- [ ] **Step 3: Write minimal implementation** — initialize the repo and ensure Dagster runtime dirs are ignored. The existing `.gitignore` already covers `__pycache__/`, `*.egg-info/`, and `.venv`; append the two Dagster-specific entries:

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline init
printf '\n# Dagster\n.dagster_home/\n.dg/\n' >> /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline/.gitignore
```

- [ ] **Step 4: Run test to verify it passes** — confirm the repo exists and tracked files exclude ignored dirs:

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline status --porcelain | grep -E '\.venv|__pycache__|\.dagster_home|egg-info' || echo "NO IGNORED FILES STAGED"
```

Expected output:

```
NO IGNORED FILES STAGED
```

- [ ] **Step 5: Commit**

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline add -A
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline commit -m "chore: initial commit of sc-curation-pipeline scaffold + .gitignore for Dagster"
```

---

### Task 2: Bootstrap — dev dependencies + editable install

**Files:**
- Modify: `pyproject.toml:9-13` (the `[dependency-groups] dev` list)

**Interfaces:**
- Consumes: nothing.
- Produces: a `dl2025` venv that can `import sc_curation_pipeline`, run `pytest`, and run `dg check defs`. Establishes the test command `/scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest` used by every later task.

- [ ] **Step 1: Establish precondition check** — bootstrap task. Verify deterministically whether the project is already editable-installed in the dl2025 venv (always prints a definitive line):

```bash
/scratch/users/chensj16/venvs/dl2025/.venv/bin/python -c "import sc_curation_pipeline; print('IMPORT_OK')" 2>/dev/null || echo "NOT_INSTALLED"
```

- [ ] **Step 2: Run the precondition check** — run the command above; expected output is exactly one of these two lines:

```
NOT_INSTALLED
```

(If it prints `IMPORT_OK` the project is already installed; the install in Step 3 is still required to register `pytest` and pin the dev group.)

- [ ] **Step 3: Write minimal implementation** — add `pytest`, `scanpy`, `anndata` to the dev group, then editable-install the project. Edit `pyproject.toml` lines 9-13 from:

```toml
[dependency-groups]
dev = [
    "dagster-dg-cli",
    "dagster-webserver",
]
```

to:

```toml
[dependency-groups]
dev = [
    "dagster-dg-cli",
    "dagster-webserver",
    "pytest>=8",
    "scanpy>=1.11",
    "anndata>=0.12",
]
```

Then install pytest into the venv and editable-install the project (scanpy/anndata are already present in `dl2025`, so `uv pip install` will be a fast no-op for those):

```bash
/scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pip install "pytest>=8"
uv pip install -p /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -e /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
```

- [ ] **Step 4: Run test to verify it passes** — verify imports and the Dagster defs check both work:

```bash
/scratch/users/chensj16/venvs/dl2025/.venv/bin/python -c "import sc_curation_pipeline, pytest, scanpy, anndata; print('IMPORT_OK')"
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/dg check defs && echo DEFS_OK
```

Expected output (the import line prints exactly `IMPORT_OK`; the `dg check defs` line, on success, ends with exactly `DEFS_OK`):

```
IMPORT_OK
DEFS_OK
```

- [ ] **Step 5: Commit**

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline add pyproject.toml uv.lock
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline commit -m "chore: add pytest/scanpy/anndata to dev group and editable-install into dl2025"
```

---

### Task 3: CurationSettings resource + partition-key helpers

**Files:**
- Create: `src/sc_curation_pipeline/defs/settings.py`
- Test: `tests/test_settings.py`
- Create: `tests/conftest.py` (settings fixture portion)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `class CurationSettings(dg.ConfigurableResource)` with fields `watch_dir: str`, `done_marker: str = ".done"`, `h5ad_glob: str = "*.h5ad"`, `scan_interval_sec: int = 30`, `min_cells: int = 100`, `max_mito_pct: float = 20.0`.
  - `def build_curation_settings() -> CurationSettings` — env-default factory.
  - `def sanitize_key(rel_path: str) -> str` — turns a relative folder path into a Dagster-legal partition key.
  - `def partition_key_for(watch_dir: str, folder: str) -> str` — `sanitize_key(relpath(folder, watch_dir))`.
  - `def path_for_partition_key(key: str) -> str` — reverse of `sanitize_key` back to a relative POSIX path (used only for display; the authoritative absolute h5ad path travels in run tags/metadata).

- [ ] **Step 1: Write the failing test** — create `tests/conftest.py` with the settings fixture and `tests/test_settings.py`.

`tests/conftest.py`:

```python
import os
from pathlib import Path

import numpy as np
import pytest
import scipy.sparse as sp
import anndata as ad

from sc_curation_pipeline.defs.settings import CurationSettings


@pytest.fixture
def settings_factory(tmp_path):
    """Return a callable producing CurationSettings rooted at a temp watch dir."""

    def _make(**overrides):
        watch = overrides.pop("watch_dir", str(tmp_path / "watch"))
        Path(watch).mkdir(parents=True, exist_ok=True)
        kwargs = dict(
            watch_dir=watch,
            done_marker=".done",
            h5ad_glob="*.h5ad",
            scan_interval_sec=30,
            min_cells=100,
            max_mito_pct=20.0,
        )
        kwargs.update(overrides)
        return CurationSettings(**kwargs)

    return _make


def _write_h5ad(path, X, var_names=None, add_raw=False, add_extras=False):
    n_obs, n_vars = X.shape
    if var_names is None:
        var_names = [f"GENE{i}" for i in range(n_vars)]
    adata = ad.AnnData(X=X)
    adata.var_names = list(var_names)
    adata.obs_names = [f"cell{i}" for i in range(n_obs)]
    adata.obs["batch"] = ["b"] * n_obs
    adata.var["gene_ids"] = list(var_names)
    if add_raw:
        adata.raw = adata
    if add_extras:
        adata.layers["counts"] = X.copy()
        adata.obsm["X_pca"] = np.zeros((n_obs, 2), dtype=np.float32)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    adata.write_h5ad(path)
    return path


@pytest.fixture
def h5ad_writer():
    """Return the _write_h5ad helper for building synthetic h5ad files."""
    return _write_h5ad


@pytest.fixture
def make_sparse_counts():
    """Return a callable building an integer CSR count matrix with MT-/RPS genes."""

    def _make(n_obs=120, n_vars=10, seed=0):
        rng = np.random.default_rng(seed)
        dense = rng.integers(0, 5, size=(n_obs, n_vars)).astype(np.float32)
        var_names = [f"GENE{i}" for i in range(n_vars - 2)] + ["MT-CO1", "RPS3"]
        return sp.csr_matrix(dense), var_names

    return _make
```

`tests/test_settings.py`:

```python
import importlib

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


def test_watch_dir_missing_raises(monkeypatch):
    monkeypatch.delenv("SC_CURATION_WATCH_DIR", raising=False)
    with pytest.raises(KeyError) as excinfo:
        S.build_curation_settings()
    assert "SC_CURATION_WATCH_DIR" in str(excinfo.value)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_settings.py -v
```

Expected: collection error / failures because `sc_curation_pipeline.defs.settings` does not exist:

```
ModuleNotFoundError: No module named 'sc_curation_pipeline.defs.settings'
```

- [ ] **Step 3: Write minimal implementation** — create `src/sc_curation_pipeline/defs/settings.py`:

```python
import os

import dagster as dg

# Partition keys must avoid Dagster-illegal characters (path separators).
# We encode the relative folder path by replacing "/" with this token.
_SEP_TOKEN = "__"


class CurationSettings(dg.ConfigurableResource):
    """Env-driven configuration for the h5ad QC pipeline."""

    watch_dir: str
    done_marker: str = ".done"
    h5ad_glob: str = "*.h5ad"
    scan_interval_sec: int = 30
    min_cells: int = 100
    max_mito_pct: float = 20.0


def build_curation_settings() -> CurationSettings:
    """Construct CurationSettings from environment variables.

    SC_CURATION_WATCH_DIR is required: read via os.environ so direct attribute
    access returns the resolved string AND a missing value raises KeyError
    loudly (spec §5.1 "missing -> clear error"). Optional fields fall back to
    defaults via os.getenv with explicit casts.
    """
    return CurationSettings(
        watch_dir=os.environ["SC_CURATION_WATCH_DIR"],
        done_marker=os.getenv("SC_CURATION_DONE_MARKER", ".done"),
        h5ad_glob=os.getenv("SC_CURATION_H5AD_GLOB", "*.h5ad"),
        scan_interval_sec=int(os.getenv("SC_CURATION_SCAN_INTERVAL_SEC", "30")),
        min_cells=int(os.getenv("SC_CURATION_MIN_CELLS", "100")),
        max_mito_pct=float(os.getenv("SC_CURATION_MAX_MITO_PCT", "20")),
    )


def sanitize_key(rel_path: str) -> str:
    """Turn a relative POSIX folder path into a Dagster-legal partition key."""
    return rel_path.strip("/").replace("/", _SEP_TOKEN)


def path_for_partition_key(key: str) -> str:
    """Reverse sanitize_key back to a relative POSIX path (display only)."""
    return key.replace(_SEP_TOKEN, "/")


def partition_key_for(watch_dir: str, folder: str) -> str:
    """Partition key for a sample folder relative to the watch dir."""
    rel = os.path.relpath(folder, watch_dir).replace(os.sep, "/")
    return sanitize_key(rel)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_settings.py -v
```

Expected output (last line):

```
6 passed
```

- [ ] **Step 5: Commit**

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline add src/sc_curation_pipeline/defs/settings.py tests/conftest.py tests/test_settings.py
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline commit -m "feat: CurationSettings resource + env factory + partition-key helpers"
```

---

### Task 4: compute_qc pure function

**Files:**
- Create: `src/sc_curation_pipeline/defs/qc.py` (function only; asset added in Task 6)
- Test: `tests/test_qc.py` (compute_qc tests only; asset tests added in Task 6)

**Interfaces:**
- Consumes: `tests/conftest.py` fixtures `h5ad_writer`, `make_sparse_counts`.
- Produces: `def compute_qc(path: str, memory_cap: int = 50_000_000) -> dict` returning keys: `path, file_size_bytes, mtime, n_cells, n_genes, X_dtype, is_sparse, has_raw, layers, obsm, obsp, obs_columns, n_obs_columns, var_columns, n_var_columns, total_counts, median_counts_per_cell, median_genes_per_cell, density, sparsity, mito_pct, ribo_pct, is_raw_counts`.

- [ ] **Step 1: Write the failing test** — create `tests/test_qc.py`:

```python
import numpy as np
import scipy.sparse as sp

from sc_curation_pipeline.defs.qc import compute_qc


def test_compute_qc_sparse_counts(tmp_path, h5ad_writer, make_sparse_counts):
    X, var_names = make_sparse_counts(n_obs=120, n_vars=10, seed=1)
    path = h5ad_writer(str(tmp_path / "s" / "a.h5ad"), X,
                       var_names=var_names, add_raw=True, add_extras=True)
    out = compute_qc(path)
    assert out["n_cells"] == 120
    assert out["n_genes"] == 10
    assert out["is_sparse"] is True
    assert out["has_raw"] is True
    assert "counts" in out["layers"]
    assert "X_pca" in out["obsm"]
    assert "batch" in out["obs_columns"]
    assert "gene_ids" in out["var_columns"]
    assert out["is_raw_counts"] is True
    assert out["total_counts"] > 0
    assert out["mito_pct"] >= 0.0
    assert out["ribo_pct"] >= 0.0
    assert out["file_size_bytes"] > 0


def test_compute_qc_dense(tmp_path, h5ad_writer):
    import os
    from datetime import datetime

    X = np.array([[1.0, 0.0, 2.0], [0.0, 3.0, 0.0]], dtype=np.float32)
    path = h5ad_writer(str(tmp_path / "d" / "x.h5ad"), X,
                       var_names=["GENE0", "MT-CO1", "RPS3"])
    out = compute_qc(path)
    assert out["n_cells"] == 2
    assert out["n_genes"] == 3
    assert out["is_sparse"] is False
    assert out["is_raw_counts"] is True
    assert out["total_counts"] == 6.0
    # mito = MT-CO1 column sum (0+3=3) over total 6 -> 50%
    assert abs(out["mito_pct"] - 50.0) < 1e-6
    # ribo = RPS3 column sum (2+0=2) over total 6 -> ~33.33%
    assert abs(out["ribo_pct"] - (100.0 * 2.0 / 6.0)) < 1e-6
    # file metadata: absolute path + ISO-8601 mtime string
    assert out["path"] == os.path.abspath(path)
    assert isinstance(out["mtime"], str)
    datetime.fromisoformat(out["mtime"])  # parses as ISO-8601


def test_compute_qc_normalized_is_not_raw(tmp_path, h5ad_writer):
    X = np.log1p(np.array([[1.0, 4.0], [2.0, 8.0]], dtype=np.float32))
    path = h5ad_writer(str(tmp_path / "n" / "n.h5ad"), X, var_names=["GENE0", "GENE1"])
    out = compute_qc(path)
    assert out["is_raw_counts"] is False


def test_compute_qc_empty_matrix(tmp_path, h5ad_writer):
    import os
    from datetime import datetime

    X = sp.csr_matrix(np.zeros((5, 4), dtype=np.float32))
    path = h5ad_writer(str(tmp_path / "e" / "e.h5ad"), X,
                       var_names=["GENE0", "GENE1", "MT-CO1", "RPS3"])
    out = compute_qc(path)
    assert out["n_cells"] == 5
    assert out["total_counts"] == 0.0
    assert out["mito_pct"] == 0.0
    assert out["ribo_pct"] == 0.0
    assert out["density"] == 0.0
    assert out["sparsity"] == 1.0
    # file metadata: absolute path + ISO-8601 mtime string
    assert out["path"] == os.path.abspath(path)
    assert isinstance(out["mtime"], str)
    datetime.fromisoformat(out["mtime"])  # parses as ISO-8601


def test_compute_qc_streaming_matches_inmemory(tmp_path, h5ad_writer, make_sparse_counts):
    X, var_names = make_sparse_counts(n_obs=200, n_vars=8, seed=2)
    path = h5ad_writer(str(tmp_path / "str" / "s.h5ad"), X, var_names=var_names)
    full = compute_qc(path, memory_cap=10_000_000)
    streamed = compute_qc(path, memory_cap=16)  # tiny cap forces row chunking
    assert streamed["total_counts"] == full["total_counts"]
    assert streamed["median_counts_per_cell"] == full["median_counts_per_cell"]
    assert streamed["median_genes_per_cell"] == full["median_genes_per_cell"]
    assert abs(streamed["mito_pct"] - full["mito_pct"]) < 1e-9
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_qc.py -v
```

Expected:

```
ModuleNotFoundError: No module named 'sc_curation_pipeline.defs.qc'
```

- [ ] **Step 3: Write minimal implementation** — create `src/sc_curation_pipeline/defs/qc.py` with the verified `compute_qc` (memory-aware, backed-mode, manual chunked counts):

```python
import os
from datetime import datetime, timezone

import numpy as np
import scipy.sparse as sp
import anndata as ad


def compute_qc(path: str, memory_cap: int = 50_000_000) -> dict:
    """Memory-aware QC on an .h5ad file.

    Structural metrics come from backed='r' (cheap). Count metrics are computed
    in memory if n_obs*n_vars <= memory_cap, else X is streamed in row chunks.
    Handles scipy sparse (CSR/CSC) and dense X.
    """
    st = os.stat(path)
    out = {
        "path": os.path.abspath(path),
        "file_size_bytes": int(st.st_size),
        "mtime": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(),
    }

    A = ad.read_h5ad(path, backed="r")
    try:
        n_cells, n_genes = int(A.n_obs), int(A.n_vars)
        X = A.X
        is_sparse = sp.issparse(X) or getattr(X, "format", None) in ("csr", "csc")

        out.update({
            "n_cells": n_cells,
            "n_genes": n_genes,
            "X_dtype": str(X.dtype),
            "is_sparse": bool(is_sparse),
            "has_raw": A.raw is not None,
            "layers": list(A.layers.keys()),
            "obsm": list(A.obsm.keys()),
            "obsp": list(A.obsp.keys()),
            "obs_columns": list(A.obs.columns),
            "n_obs_columns": len(A.obs.columns),
            "var_columns": list(A.var.columns),
            "n_var_columns": len(A.var.columns),
        })

        up = A.var_names.astype(str).str.upper()
        mito_mask = np.asarray(up.str.startswith("MT-"))
        ribo_mask = np.asarray(up.str.startswith("RPS") | up.str.startswith("RPL"))

        total_cells = n_cells * n_genes
        counts_per_cell = np.zeros(n_cells, dtype=np.float64)
        genes_per_cell = np.zeros(n_cells, dtype=np.float64)
        mito_per_cell = np.zeros(n_cells, dtype=np.float64)
        ribo_per_cell = np.zeros(n_cells, dtype=np.float64)
        nnz_total = 0
        is_integer = True

        def _accumulate(block, start):
            nonlocal nnz_total, is_integer
            end = start + block.shape[0]
            if sp.issparse(block):
                block = block.tocsr()
                counts_per_cell[start:end] = np.asarray(block.sum(axis=1)).ravel()
                genes_per_cell[start:end] = block.getnnz(axis=1)
                nnz_total += block.nnz
                if mito_mask.any():
                    mito_per_cell[start:end] = np.asarray(block[:, mito_mask].sum(axis=1)).ravel()
                if ribo_mask.any():
                    ribo_per_cell[start:end] = np.asarray(block[:, ribo_mask].sum(axis=1)).ravel()
                if is_integer and block.nnz:
                    d = block.data
                    if not np.allclose(d, np.round(d), atol=1e-6):
                        is_integer = False
            else:
                block = np.asarray(block)
                counts_per_cell[start:end] = block.sum(axis=1)
                genes_per_cell[start:end] = (block != 0).sum(axis=1)
                nnz_total += int((block != 0).sum())
                if mito_mask.any():
                    mito_per_cell[start:end] = block[:, mito_mask].sum(axis=1)
                if ribo_mask.any():
                    ribo_per_cell[start:end] = block[:, ribo_mask].sum(axis=1)
                if is_integer and block.size:
                    if not np.allclose(block, np.round(block), atol=1e-6):
                        is_integer = False

        if total_cells <= memory_cap:
            _accumulate(A.to_memory().X, 0)
        else:
            chunk = max(1, memory_cap // max(1, n_genes))
            for start in range(0, n_cells, chunk):
                stop = min(start + chunk, n_cells)
                _accumulate(X[start:stop], start)

        total_counts = float(counts_per_cell.sum())
        out.update({
            "total_counts": total_counts,
            "median_counts_per_cell": float(np.median(counts_per_cell)) if n_cells else 0.0,
            "median_genes_per_cell": float(np.median(genes_per_cell)) if n_cells else 0.0,
            "density": (nnz_total / total_cells) if total_cells else 0.0,
            "sparsity": (1.0 - nnz_total / total_cells) if total_cells else 0.0,
            "mito_pct": (100.0 * mito_per_cell.sum() / total_counts) if total_counts else 0.0,
            "ribo_pct": (100.0 * ribo_per_cell.sum() / total_counts) if total_counts else 0.0,
            "is_raw_counts": bool(is_integer),
        })
    finally:
        if A.isbacked and A.file is not None:
            A.file.close()
    return out
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_qc.py -v
```

Expected output (last line):

```
5 passed
```

- [ ] **Step 5: Commit**

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline add src/sc_curation_pipeline/defs/qc.py tests/test_qc.py
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline commit -m "feat: compute_qc pure QC function (backed-mode, chunked counts)"
```

---

### Task 5: Dynamic partitions definition

**Files:**
- Create: `src/sc_curation_pipeline/defs/partitions.py`
- Test: `tests/test_qc.py` (append one tiny test)

**Interfaces:**
- Consumes: nothing.
- Produces: `h5ad_partitions = dg.DynamicPartitionsDefinition(name="h5ad_samples")` — imported by the asset (Task 6) and sensor (Task 7).

- [ ] **Step 1: Write the failing test** — append to `tests/test_qc.py`:

```python
def test_partitions_def_name():
    from sc_curation_pipeline.defs.partitions import h5ad_partitions

    assert h5ad_partitions.name == "h5ad_samples"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_qc.py::test_partitions_def_name -v
```

Expected:

```
ModuleNotFoundError: No module named 'sc_curation_pipeline.defs.partitions'
```

- [ ] **Step 3: Write minimal implementation** — create `src/sc_curation_pipeline/defs/partitions.py`:

```python
import dagster as dg

# One dynamic partition per discovered sample folder. Name is fixed and
# referenced by both the sensor (registration) and the asset.
h5ad_partitions = dg.DynamicPartitionsDefinition(name="h5ad_samples")
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_qc.py::test_partitions_def_name -v
```

Expected output (last line):

```
1 passed
```

- [ ] **Step 5: Commit**

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline add src/sc_curation_pipeline/defs/partitions.py tests/test_qc.py
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline commit -m "feat: h5ad_samples DynamicPartitionsDefinition"
```

---

### Task 6: h5ad_qc partitioned asset + asset checks + job

**Files:**
- Modify: `src/sc_curation_pipeline/defs/qc.py` (append asset + job below `compute_qc`)
- Test: `tests/test_qc.py` (append asset materialize tests)

**Interfaces:**
- Consumes: `compute_qc` (Task 4), `h5ad_partitions` (Task 5), `CurationSettings` + `partition_key_for` (Task 3).
- Produces:
  - `def resolve_h5ad_path(context, settings) -> str` — resolves the absolute h5ad path for the current partition, preferring the `sc/h5ad_path` run tag, else reconstructing from `settings.watch_dir` + the partition key; raises `dg.Failure` if `.done` present but no/multiple h5ad.
  - `h5ad_qc` — `@asset(partitions_def=h5ad_partitions, check_specs=[min_cells, max_mito_pct, is_raw_counts], retry_policy=RetryPolicy(max_retries=2))`. Yields one `MaterializeResult` (full QC metadata) and three `AssetCheckResult`s. Raises `dg.Failure` on unreadable/corrupt h5ad or missing h5ad.
  - `h5ad_qc_job = dg.define_asset_job("h5ad_qc_job", selection=dg.AssetSelection.assets("h5ad_qc"))`.

- [ ] **Step 1: Write the failing test** — append to `tests/test_qc.py`:

```python
import os

import dagster as dg
import pytest

from sc_curation_pipeline.defs.qc import h5ad_qc, h5ad_qc_job  # noqa: E402
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for  # noqa: E402
from sc_curation_pipeline.defs.partitions import h5ad_partitions  # noqa: E402


def _materialize(path_to_h5ad, watch_dir, key, settings, instance):
    return dg.materialize(
        [h5ad_qc],
        partition_key=key,
        instance=instance,
        resources={"curation": settings},
        tags={"sc/h5ad_path": path_to_h5ad},
        raise_on_error=False,
    )


def test_h5ad_qc_materialize_pass(tmp_path, h5ad_writer, make_sparse_counts):
    watch = str(tmp_path / "watch")
    folder = os.path.join(watch, "GSE1_sampleA")
    X, var_names = make_sparse_counts(n_obs=300, n_vars=12, seed=3)
    path = h5ad_writer(os.path.join(folder, "a.h5ad"), X, var_names=var_names)
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch, min_cells=100, max_mito_pct=90.0)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])
    result = _materialize(path, watch, key, settings, instance)
    assert result.success

    mats = result.asset_materializations_for_node("h5ad_qc")
    md = mats[0].metadata
    assert md["n_cells"].value == 300
    assert md["n_genes"].value == 12
    assert mats[0].partition == key
    assert md["h5ad_path"].value == os.path.abspath(path)

    evals = {e.check_name: e for e in result.get_asset_check_evaluations()}
    assert evals["min_cells"].passed is True
    assert evals["is_raw_counts"].passed is True
    assert evals["max_mito_pct"].severity == dg.AssetCheckSeverity.ERROR
    # ERROR-severity checks render red but do NOT fail the run by default.
    assert result.success is True
    assert not result.is_node_failed("h5ad_qc")


def test_h5ad_qc_soft_gate_fails_check_not_run(tmp_path, h5ad_writer, make_sparse_counts):
    watch = str(tmp_path / "watch")
    folder = os.path.join(watch, "tiny")
    X, var_names = make_sparse_counts(n_obs=10, n_vars=6, seed=4)
    path = h5ad_writer(os.path.join(folder, "t.h5ad"), X, var_names=var_names)
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch, min_cells=100, max_mito_pct=20.0)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])
    result = _materialize(path, watch, key, settings, instance)
    assert result.success  # soft gate -> green run

    evals = {e.check_name: e for e in result.get_asset_check_evaluations()}
    assert evals["min_cells"].passed is False  # 10 < 100 -> red check


def test_h5ad_qc_corrupt_raises_failure(tmp_path):
    watch = str(tmp_path / "watch")
    folder = os.path.join(watch, "bad")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "broken.h5ad")
    with open(path, "wb") as fh:
        fh.write(b"this is not an hdf5 file")
    key = partition_key_for(watch, folder)
    settings = CurationSettings(watch_dir=watch)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])
    result = _materialize(path, watch, key, settings, instance)
    assert result.success is False
    assert result.is_node_failed("h5ad_qc")


def test_h5ad_qc_missing_watch_dir_raises(tmp_path):
    # Partition whose run carries NO sc/h5ad_path tag, and a watch_dir that does
    # not exist on disk -> resolve_h5ad_path must raise dg.Failure (spec §5.1).
    missing = str(tmp_path / "does_not_exist")
    key = "ghost_sample"
    settings = CurationSettings(watch_dir=missing)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])
    result = dg.materialize(
        [h5ad_qc],
        partition_key=key,
        instance=instance,
        resources={"curation": settings},
        raise_on_error=False,  # no sc/h5ad_path tag -> falls back to watch dir
    )
    assert result.success is False
    assert result.is_node_failed("h5ad_qc")
    failures = [
        e
        for e in result.get_step_failure_events()
        if e.step_key == "h5ad_qc"
    ]
    assert failures
    msg = failures[0].event_specific_data.error.message
    assert "SC_CURATION_WATCH_DIR" in msg
    assert missing in msg


def test_h5ad_qc_job_targets_asset():
    assert h5ad_qc_job.name == "h5ad_qc_job"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_qc.py -k "h5ad_qc" -v
```

Expected:

```
ImportError: cannot import name 'h5ad_qc' from 'sc_curation_pipeline.defs.qc'
```

- [ ] **Step 3: Write minimal implementation** — append the asset, the path resolver, and the job to `src/sc_curation_pipeline/defs/qc.py` (below `compute_qc`). Add these imports at the TOP of the file (after the existing imports):

```python
import glob

import dagster as dg

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.settings import CurationSettings, path_for_partition_key

H5AD_PATH_TAG = "sc/h5ad_path"
```

Then append below `compute_qc`:

```python
def resolve_h5ad_path(
    context: dg.AssetExecutionContext, settings: CurationSettings
) -> str:
    """Resolve the absolute .h5ad path for the current partition.

    Prefers the sc/h5ad_path run tag (set by the sensor). Falls back to
    reconstructing the folder from watch_dir + the partition key and globbing.
    Raises dagster.Failure if the file is missing or ambiguous.
    """
    key = context.partition_key
    tag_path = context.run.tags.get(H5AD_PATH_TAG)
    if tag_path and os.path.isfile(tag_path):
        return os.path.abspath(tag_path)

    # No usable tag -> we must fall back to the watch dir. Guard it first so a
    # missing/invalid SC_CURATION_WATCH_DIR fails loudly (spec §5.1) instead of
    # silently globbing nothing.
    if not settings.watch_dir or not os.path.isdir(settings.watch_dir):
        raise dg.Failure(
            description=(
                f"SC_CURATION_WATCH_DIR missing or not a directory: "
                f"{settings.watch_dir!r}"
            ),
            metadata={
                "partition": dg.MetadataValue.text(key),
                "watch_dir": dg.MetadataValue.text(str(settings.watch_dir)),
            },
        )

    folder = os.path.join(settings.watch_dir, path_for_partition_key(key))
    matches = sorted(glob.glob(os.path.join(folder, settings.h5ad_glob)))
    if len(matches) != 1:
        raise dg.Failure(
            description=(
                f"expected exactly one h5ad in {folder!r} matching "
                f"{settings.h5ad_glob!r}, found {len(matches)}"
            ),
            metadata={
                "partition": dg.MetadataValue.text(key),
                "folder": dg.MetadataValue.path(folder),
                "matches": dg.MetadataValue.json(matches),
            },
        )
    return os.path.abspath(matches[0])


@dg.asset(
    partitions_def=h5ad_partitions,
    group_name="curation",
    check_specs=[
        dg.AssetCheckSpec(name="min_cells", asset="h5ad_qc"),
        dg.AssetCheckSpec(name="max_mito_pct", asset="h5ad_qc"),
        dg.AssetCheckSpec(name="is_raw_counts", asset="h5ad_qc"),
    ],
    retry_policy=dg.RetryPolicy(max_retries=2),
)
def h5ad_qc(context: dg.AssetExecutionContext, curation: CurationSettings):
    """Lightweight QC on one sample's h5ad; results as metadata + checks."""
    path = resolve_h5ad_path(context, curation)
    try:
        qc = compute_qc(path)
    except dg.Failure:
        raise
    except Exception as exc:  # corrupt / unreadable h5ad -> hard error (red run)
        raise dg.Failure(
            description=f"failed to read/QC h5ad at {path!r}: {exc}",
            metadata={
                "partition": dg.MetadataValue.text(context.partition_key),
                "h5ad_path": dg.MetadataValue.path(path),
                "error": dg.MetadataValue.text(repr(exc)),
            },
        )

    yield dg.MaterializeResult(
        metadata={
            "h5ad_path": dg.MetadataValue.path(qc["path"]),
            "file_size_bytes": dg.MetadataValue.int(qc["file_size_bytes"]),
            "mtime": dg.MetadataValue.text(qc["mtime"]),
            "n_cells": dg.MetadataValue.int(qc["n_cells"]),
            "n_genes": dg.MetadataValue.int(qc["n_genes"]),
            "X_dtype": dg.MetadataValue.text(qc["X_dtype"]),
            "is_sparse": dg.MetadataValue.bool(qc["is_sparse"]),
            "sparsity": dg.MetadataValue.float(qc["sparsity"]),
            "density": dg.MetadataValue.float(qc["density"]),
            "has_raw": dg.MetadataValue.bool(qc["has_raw"]),
            "layers": dg.MetadataValue.json(qc["layers"]),
            "obsm": dg.MetadataValue.json(qc["obsm"]),
            "obsp": dg.MetadataValue.json(qc["obsp"]),
            "obs_columns": dg.MetadataValue.json(qc["obs_columns"]),
            "n_obs_columns": dg.MetadataValue.int(qc["n_obs_columns"]),
            "var_columns": dg.MetadataValue.json(qc["var_columns"]),
            "n_var_columns": dg.MetadataValue.int(qc["n_var_columns"]),
            "total_counts": dg.MetadataValue.float(qc["total_counts"]),
            "median_counts_per_cell": dg.MetadataValue.float(qc["median_counts_per_cell"]),
            "median_genes_per_cell": dg.MetadataValue.float(qc["median_genes_per_cell"]),
            "mito_pct": dg.MetadataValue.float(qc["mito_pct"]),
            "ribo_pct": dg.MetadataValue.float(qc["ribo_pct"]),
            "is_raw_counts": dg.MetadataValue.bool(qc["is_raw_counts"]),
        }
    )

    yield dg.AssetCheckResult(
        passed=qc["n_cells"] >= curation.min_cells,
        check_name="min_cells",
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"n_cells": qc["n_cells"], "min_cells": curation.min_cells},
    )
    yield dg.AssetCheckResult(
        passed=qc["mito_pct"] <= curation.max_mito_pct,
        check_name="max_mito_pct",
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"mito_pct": qc["mito_pct"], "max_mito_pct": curation.max_mito_pct},
    )
    yield dg.AssetCheckResult(
        passed=bool(qc["is_raw_counts"]),
        check_name="is_raw_counts",
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"is_raw_counts": qc["is_raw_counts"]},
    )


h5ad_qc_job = dg.define_asset_job(
    name="h5ad_qc_job",
    selection=dg.AssetSelection.assets("h5ad_qc"),
)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_qc.py -v
```

Expected output (last line):

```
11 passed
```

- [ ] **Step 5: Commit**

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline add src/sc_curation_pipeline/defs/qc.py tests/test_qc.py
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline commit -m "feat: h5ad_qc partitioned asset with QC metadata, soft-gate checks, Failure, job"
```

---

### Task 7: discover_samples scanner + watch_h5ad_dir sensor

**Files:**
- Create: `src/sc_curation_pipeline/defs/sensors.py`
- Test: `tests/test_sensor.py`

**Interfaces:**
- Consumes: `CurationSettings`, `partition_key_for` (Task 3), `h5ad_partitions` (Task 5), `h5ad_qc_job`, `H5AD_PATH_TAG` (Task 6).
- Produces:
  - `def discover_samples(watch_dir: str, done_marker: str, h5ad_glob: str) -> list[tuple[str, str]]` — returns `[(partition_key, abs_h5ad_path), ...]` for every folder that contains the done marker AND exactly one matching h5ad (folders with the marker but zero/multiple h5ads are skipped). Sorted by key.
  - `watch_h5ad_dir` — `@dg.sensor(job=h5ad_qc_job, minimum_interval_seconds=...)` returning a `SensorResult` that registers new keys via `h5ad_partitions.build_add_request(...)` and one `RunRequest(partition_key=k, run_key=k, tags={H5AD_PATH_TAG: path})` per new key; `SkipReason` when nothing new.

- [ ] **Step 1: Write the failing test** — create `tests/test_sensor.py`:

```python
import os

import dagster as dg

from sc_curation_pipeline.defs.sensors import discover_samples, watch_h5ad_dir
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for
from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.qc import H5AD_PATH_TAG


def _make_sample(watch, name, *, with_done=True, n_h5ad=1):
    folder = os.path.join(watch, name)
    os.makedirs(folder, exist_ok=True)
    for i in range(n_h5ad):
        with open(os.path.join(folder, f"f{i}.h5ad"), "wb") as fh:
            fh.write(b"x")
    if with_done:
        open(os.path.join(folder, ".done"), "w").close()
    return folder


def test_discover_samples_only_done_with_one_h5ad(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    good = _make_sample(watch, "good", with_done=True, n_h5ad=1)
    _make_sample(watch, "no_done", with_done=False, n_h5ad=1)   # skipped: no marker
    _make_sample(watch, "two_h5ad", with_done=True, n_h5ad=2)   # skipped: ambiguous
    nested = _make_sample(watch, "GSE1/sampleA", with_done=True, n_h5ad=1)  # nested OK

    found = discover_samples(watch, ".done", "*.h5ad")
    keys = {k for k, _ in found}
    assert keys == {
        partition_key_for(watch, good),
        partition_key_for(watch, nested),
    }
    paths = dict(found)
    assert os.path.isfile(paths[partition_key_for(watch, good)])


def test_sensor_registers_and_requests_new(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    good = _make_sample(watch, "sampleA", with_done=True, n_h5ad=1)
    key = partition_key_for(watch, good)
    settings = CurationSettings(watch_dir=watch, scan_interval_sec=30)

    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )
    result = watch_h5ad_dir(ctx)
    assert isinstance(result, dg.SensorResult)
    assert [r.partition_key for r in result.run_requests] == [key]
    assert result.run_requests[0].run_key == key
    assert result.run_requests[0].tags[H5AD_PATH_TAG].endswith("f0.h5ad")
    dpr = result.dynamic_partitions_requests[0]
    assert dpr.partitions_def_name == "h5ad_samples"
    assert dpr.partition_keys == [key]


def test_sensor_dedups_already_registered(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    good = _make_sample(watch, "sampleA", with_done=True, n_h5ad=1)
    key = partition_key_for(watch, good)
    settings = CurationSettings(watch_dir=watch)

    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])  # pre-registered
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )
    result = watch_h5ad_dir(ctx)
    assert isinstance(result, dg.SkipReason)
    assert result.skip_message


def test_sensor_skips_when_no_done(tmp_path):
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    _make_sample(watch, "sampleA", with_done=False, n_h5ad=1)
    settings = CurationSettings(watch_dir=watch)

    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )
    result = watch_h5ad_dir(ctx)
    assert isinstance(result, dg.SkipReason)


def test_sensor_skips_missing_watch_dir(tmp_path):
    watch = str(tmp_path / "does_not_exist")
    settings = CurationSettings(watch_dir=watch)
    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )
    result = watch_h5ad_dir(ctx)
    assert isinstance(result, dg.SkipReason)


def test_sensor_write_once_no_rerun_on_change(tmp_path):
    # §5.3 write-once: once a sample is registered, mutating its h5ad bytes/mtime
    # (without changing identity) must NOT yield a new RunRequest on the next tick.
    watch = str(tmp_path / "watch")
    os.makedirs(watch, exist_ok=True)
    folder = _make_sample(watch, "sampleA", with_done=True, n_h5ad=1)
    key = partition_key_for(watch, folder)
    h5ad_path = os.path.join(folder, "f0.h5ad")
    settings = CurationSettings(watch_dir=watch)

    instance = dg.DagsterInstance.ephemeral()
    ctx = dg.build_sensor_context(
        instance=instance, resources={"curation": settings}
    )

    # First tick: registers the partition and requests one run.
    first = watch_h5ad_dir(ctx)
    assert isinstance(first, dg.SensorResult)
    assert [r.partition_key for r in first.run_requests] == [key]
    # Apply the dynamic-partition registration as the daemon would.
    instance.add_dynamic_partitions(h5ad_partitions.name, [key])

    # Mutate the already-registered sample's h5ad content + mtime; identity (the
    # folder/partition key) is unchanged.
    with open(h5ad_path, "wb") as fh:
        fh.write(b"yy")
    future = os.stat(h5ad_path).st_mtime + 10_000
    os.utime(h5ad_path, (future, future))

    # Second tick on the same instance: no new RunRequest for the registered key.
    second = watch_h5ad_dir(ctx)
    if isinstance(second, dg.SensorResult):
        assert key not in [r.partition_key for r in second.run_requests]
    else:
        assert isinstance(second, dg.SkipReason)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_sensor.py -v
```

Expected:

```
ModuleNotFoundError: No module named 'sc_curation_pipeline.defs.sensors'
```

- [ ] **Step 3: Write minimal implementation** — create `src/sc_curation_pipeline/defs/sensors.py`:

```python
import glob
import os

import dagster as dg

from sc_curation_pipeline.defs.partitions import h5ad_partitions
from sc_curation_pipeline.defs.qc import H5AD_PATH_TAG, h5ad_qc_job
from sc_curation_pipeline.defs.settings import CurationSettings, partition_key_for

# Fallback tick interval used only when SC_CURATION_SCAN_INTERVAL_SEC is unset.
# The decorator's minimum_interval_seconds is read from the env at import time;
# the resource's scan_interval_sec field is then purely informational.
_DEFAULT_INTERVAL_SEC = 30


def discover_samples(
    watch_dir: str, done_marker: str, h5ad_glob: str
) -> list[tuple[str, str]]:
    """Find completed sample folders under watch_dir.

    A folder qualifies if it contains the done marker AND exactly one file
    matching h5ad_glob. Returns sorted [(partition_key, abs_h5ad_path), ...].
    Folders with the marker but zero or multiple h5ads are skipped.
    """
    if not os.path.isdir(watch_dir):
        return []
    found: list[tuple[str, str]] = []
    for root, _dirs, files in os.walk(watch_dir):
        if done_marker not in files:
            continue
        matches = sorted(glob.glob(os.path.join(root, h5ad_glob)))
        if len(matches) != 1:
            continue
        key = partition_key_for(watch_dir, root)
        found.append((key, os.path.abspath(matches[0])))
    found.sort(key=lambda kv: kv[0])
    return found


@dg.sensor(
    job=h5ad_qc_job,
    minimum_interval_seconds=int(
        os.getenv("SC_CURATION_SCAN_INTERVAL_SEC", str(_DEFAULT_INTERVAL_SEC))
    ),
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def watch_h5ad_dir(
    context: dg.SensorEvaluationContext, curation: CurationSettings
):
    """Marker-driven discovery sensor: register new samples + request one run each."""
    if not os.path.isdir(curation.watch_dir):
        return dg.SkipReason(f"watch dir not found: {curation.watch_dir}")

    discovered = discover_samples(
        curation.watch_dir, curation.done_marker, curation.h5ad_glob
    )
    if not discovered:
        return dg.SkipReason(f"no completed samples under {curation.watch_dir}")

    new = [
        (key, path)
        for key, path in discovered
        if not context.instance.has_dynamic_partition(h5ad_partitions.name, key)
    ]
    if not new:
        return dg.SkipReason("no new samples since last tick")

    new_keys = [key for key, _ in new]
    return dg.SensorResult(
        dynamic_partitions_requests=[h5ad_partitions.build_add_request(new_keys)],
        run_requests=[
            dg.RunRequest(
                partition_key=key,
                run_key=key,
                tags={H5AD_PATH_TAG: path},
            )
            for key, path in new
        ],
    )
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_sensor.py -v
```

Expected output (last line):

```
6 passed
```

- [ ] **Step 5: Commit**

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline add src/sc_curation_pipeline/defs/sensors.py tests/test_sensor.py
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline commit -m "feat: discover_samples scanner + watch_h5ad_dir marker-driven sensor"
```

---

### Task 8: Registration — bundle asset, job, sensor, resource into Definitions

**Files:**
- Create: `src/sc_curation_pipeline/defs/registration.py`
- Test: `tests/test_sensor.py` (append a registration smoke test)

**Interfaces:**
- Consumes: `h5ad_qc`, `h5ad_qc_job` (Task 6), `watch_h5ad_dir` (Task 7), `build_curation_settings` (Task 3).
- Produces: a single `@dg.definitions def defs() -> dg.Definitions` returning `Definitions(assets=[h5ad_qc], jobs=[h5ad_qc_job], sensors=[watch_h5ad_dir], resources={"curation": build_curation_settings()})`. This is the ONLY place the `curation` resource is registered. `load_from_defs_folder` merges it automatically.

- [ ] **Step 1: Write the failing test** — append to `tests/test_sensor.py`:

```python
def test_registration_bundles_everything(monkeypatch):
    monkeypatch.setenv("SC_CURATION_WATCH_DIR", "/tmp/sc_watch_test")
    from sc_curation_pipeline.defs.registration import defs as defs_fn

    d = defs_fn()
    assert isinstance(d, dg.Definitions)
    keys = {k.to_user_string() for k in d.resolve_all_asset_keys()}
    assert "h5ad_qc" in keys
    assert "curation" in d.resources
    assert d.get_sensor_def("watch_h5ad_dir") is not None
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_sensor.py::test_registration_bundles_everything -v
```

Expected:

```
ModuleNotFoundError: No module named 'sc_curation_pipeline.defs.registration'
```

- [ ] **Step 3: Write minimal implementation** — create `src/sc_curation_pipeline/defs/registration.py`:

```python
import dagster as dg

from sc_curation_pipeline.defs.qc import h5ad_qc, h5ad_qc_job
from sc_curation_pipeline.defs.sensors import watch_h5ad_dir
from sc_curation_pipeline.defs.settings import build_curation_settings


@dg.definitions
def defs() -> dg.Definitions:
    """Bundle the curation asset, job, sensor, and the env-driven resource.

    This is the single place the `curation` resource is registered; a bare
    module-scope resource instance would be silently ignored by the loader.
    load_from_defs_folder discovers this @definitions function and merges it.
    """
    return dg.Definitions(
        assets=[h5ad_qc],
        jobs=[h5ad_qc_job],
        sensors=[watch_h5ad_dir],
        resources={"curation": build_curation_settings()},
    )
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/test_sensor.py::test_registration_bundles_everything -v
```

Expected output (last line):

```
1 passed
```

- [ ] **Step 5: Commit**

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline add src/sc_curation_pipeline/defs/registration.py tests/test_sensor.py
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline commit -m "feat: registration.py bundling asset/job/sensor/resource into Definitions"
```

---

### Task 9: Full-suite + dg check defs smoke + acceptance walkthrough

**Files:**
- Test: run the entire `tests/` suite + `dg check defs` (no new source files)

**Interfaces:**
- Consumes: everything from Tasks 3-8.
- Produces: a verified, deployable pipeline. No new symbols.

- [ ] **Step 1: Establish precondition check** — aggregate verification task (no new code/tests authored). Confirm all four test files are present before running the full gate (always prints a definitive count):

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/ --co -q | tail -1
```

Expected: a non-zero collected-tests summary (the full suite is collectible). The authoritative pass count is asserted in Step 4.

- [ ] **Step 2: Run the full suite** — run every test; if any prior task is incomplete it fails here (catches cross-module regressions):

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/ -v
```

Expected at this point: all tests authored across Tasks 3-8 pass.

- [ ] **Step 3: Write minimal implementation** — no code; run the Dagster defs validation (requires `SC_CURATION_WATCH_DIR` to be set because `build_curation_settings()` reads it via `os.environ` at resolution time):

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && SC_CURATION_WATCH_DIR=/tmp/sc_watch_smoke /scratch/users/chensj16/venvs/dl2025/.venv/bin/dg check defs && echo DEFS_OK
```

Expected output (on success the command ends with exactly):

```
DEFS_OK
```

- [ ] **Step 4: Run test to verify it passes** — full suite + defs check both green, then an end-to-end acceptance walkthrough proving the 6 spec success criteria:

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline && /scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/ -q
```

Expected output (last line, exact count is the sum of all tests: 6 settings + 11 qc + 7 sensor = 24):

```
24 passed
```

Then run `dg check defs` (Step 3) and confirm it prints `DEFS_OK` (exit code 0).

Acceptance walkthrough (maps to spec section 9 — run from a `dg dev` session on a compute node with `SC_CURATION_WATCH_DIR` exported to a real scratch dir):
1. In the Dagster UI, turn the `watch_h5ad_dir` sensor ON (it is created with `default_status=STOPPED`, so it will not tick until enabled — otherwise no run is ever observed). Then create `foo/x.h5ad` in the watch dir, THEN `touch foo/.done` -> within one tick the `watch_h5ad_dir` sensor registers partition `foo` and fires one `h5ad_qc` run (verifies criterion 1).
2. In the UI, the `foo` partition of `h5ad_qc` shows full QC metadata and green/red asset checks (criterion 2).
3. A folder without `.done` is NOT processed; ticking again does NOT re-launch `foo` (run_key dedup) (criterion 3).
4. Drop a corrupt h5ad + `.done` -> that partition's run is red with the Failure reason in metadata (criterion 4).
5. Confirm no files were written into the watch dir beyond the uploader's; the pipeline produced only Dagster metadata/checks (criterion 5).
6. `dg check defs` passes and `pytest tests/ -q` is all green (criterion 6).

- [ ] **Step 5: Commit**

```bash
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline add -A
git -C /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline commit -m "test: full-suite green + dg check defs smoke + acceptance walkthrough verified"
```

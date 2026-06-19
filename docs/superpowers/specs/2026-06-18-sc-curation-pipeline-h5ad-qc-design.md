# sc-curation-pipeline — h5ad 摄取 + QC 监控（设计 / spec）

- **日期**: 2026-06-18
- **状态**: 设计已确认，待生成实现计划（implementation plan）
- **项目路径**: `/scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline`

---

## 1. 目标与范围

### 目标
用 Dagster 监控一个（可配置的、位于 `$SCRATCH` 下的）目录，自动对**新上传的单细胞 h5ad 文件**做**轻量摄取 + QC**，结果**全部以 Dagster asset metadata + asset checks** 的形式呈现在 Dagster Web UI 里。

### 范围内（in-scope）
- 递归监控目录；每个"含单个 h5ad 的文件夹"= 一个样本 = 一个 dynamic partition。
- 用 `.done` 标记文件判定上传完成。
- 打开 h5ad、计算一组默认 QC 指标、写入 materialization metadata。
- 用 asset checks 做阈值分诊（pass/fail，UI 绿/红）。

### 非目标（暂不做）
- 不做重度 curation（counts recovery、gene harmonization、scVI、cellbender 等）——以后可作为 Dagster Pipes / Slurm 步骤接入。
- 不落地任何输出文件、不建外部 catalog/DB、不移动或修改源文件。
- 额外 QC 指标（doublet 分数、hemoglobin%、特定 obs 字段存在性检查等）留待后续，设计上预留扩展点。
- 生产化常驻部署（long-running daemon）暂不在本期。

---

## 2. 背景与约束

- **运行平台**: Sherlock HPC。开发期在计算节点用 `dg dev` 启动，UI 在 3000 端口，本地 `ssh -L` 转发查看。
- **环境集成（方案 A）**: 复用现有 uv 环境 `dl2025`（Python 3.12），其中已有 `scanpy 1.11.5`、`anndata 0.12.10`、`h5py`，**且 `dagster`/`dagster-dg-cli`/`dagster-pipes` 均为 `1.13.10`，与本项目脚手架装的版本完全一致**。集成方式：把本项目以可编辑方式装进 `dl2025`，从 `dl2025` 环境运行；asset 内直接 `import scanpy, anndata`。项目自带的 `.venv` 闲置。
- **目录约定**: 一个文件夹一个 h5ad（= 一个样本）。上传方在上传**完成后**于该文件夹内写入一个空的 `.done` 标记文件。
- **存储**: 监控目录在 Lustre（`$SCRATCH`）。设计避免并发写同一文件（不落地输出，天然规避竞争）。

---

## 3. 架构总览（数据流）

```
$SC_CURATION_WATCH_DIR/   (可配置, $SCRATCH 下, 递归)
  ├─ GSE123_sampleA/  { a.h5ad, .done }  ─┐
  ├─ GSE123_sampleB/  { b.h5ad        }   │   watch_h5ad_dir  (sensor, 默认每 ~30s)
  └─ pbmc_run3/       { p.h5ad, .done }  ─┘     1. 递归找含 `.done` 的文件夹
                                                2. 定位其中的 *.h5ad（找不到→记日志跳过）
                                                3. 与已登记 partition 去重，仅取新增
                                                4. add_dynamic_partitions + 对每个新 key 发 RunRequest
                                                     │
        （sampleB 没有 .done → 本期不处理）          ▼
                                              h5ad_qc  asset   (1 partition = 1 个样本)
                                                ├─ 由 partition key + watch_root 解析 h5ad 路径
                                                ├─ anndata.read_h5ad(path, backed='r')
                                                ├─ 用 scanpy 计算默认 QC 指标
                                                ├─ 产出 MaterializeResult(metadata={...})
                                                └─ 产出 AssetCheckResult(s)：阈值门禁 pass/fail
                                                     │
                                                     ▼
                                              Dagster UI: 每样本一格状态网格 + QC metadata + 绿/红检查
```

---

## 4. 项目结构（新增到 `src/sc_curation_pipeline/defs/`，由 `load_from_defs_folder` 自动发现）

```
src/sc_curation_pipeline/defs/
├─ settings.py     # CurationSettings(ConfigurableResource)：从环境变量读配置
├─ partitions.py   # h5ad_partitions = DynamicPartitionsDefinition("h5ad_samples")
├─ qc.py           # h5ad_qc asset(+asset checks) + h5ad_qc_job + QC 计算纯函数
└─ sensors.py      # watch_h5ad_dir sensor（marker 驱动）
tests/
├─ test_qc.py          # QC 纯函数 + asset 物化
├─ test_sensor.py      # sensor 发现/去重/marker 逻辑
└─ conftest.py         # 合成 AnnData / 临时 watch 目录 fixtures
```

---

## 5. 组件设计

### 5.1 配置（`CurationSettings`，环境变量驱动，全部可配）

| 环境变量 | 默认 | 含义 |
|---|---|---|
| `SC_CURATION_WATCH_DIR` | **（必填，无默认）** | 要监控的根目录（`$SCRATCH` 下） |
| `SC_CURATION_H5AD_GLOB` | `*.h5ad` | 在"已标记完成"的文件夹内匹配 h5ad 的模式（发现入口是 `.done` 标记，不是此 glob） |
| `SC_CURATION_DONE_MARKER` | `.done` | 上传完成标记文件名 |
| `SC_CURATION_SCAN_INTERVAL_SEC` | `30` | sensor 最小 tick 间隔（秒） |
| `SC_CURATION_MIN_CELLS` | `100` | asset check：`n_cells` 下限 |
| `SC_CURATION_MAX_MITO_PCT` | `20` | asset check：中位 mito% 上限 |

- 以 `ConfigurableResource` 实现，便于在测试里注入临时目录/阈值。
- `WATCH_DIR` 缺失时 sensor/asset 立即报清晰错误。

### 5.2 Dynamic partitions
- `h5ad_partitions = DynamicPartitionsDefinition(name="h5ad_samples")`。
- **partition key**：取 h5ad 所在文件夹相对 `WATCH_DIR` 的路径，做成人类可读 key（路径分隔符做安全化，避免 Dagster key 非法字符）；h5ad 绝对路径写入 run tag（如 `sc/h5ad_path`）与 asset metadata，供 asset 反解。

### 5.3 Sensor（`watch_h5ad_dir`，marker 驱动）
- `@sensor(job=h5ad_qc_job, minimum_interval_seconds=SCAN_INTERVAL_SEC)`。
- 每次 tick：
  1. 在 `WATCH_DIR` 下递归找所有 `DONE_MARKER` 文件 → 得到"已完成"的文件夹集合。
  2. 每个文件夹定位其中的 `*.h5ad`；找不到或多于一个 → 记日志并跳过（健壮处理）。
  3. 计算 partition key；与 `context.instance.get_dynamic_partitions("h5ad_samples")` 去重，仅保留新增。
  4. `context.instance.add_dynamic_partitions(...)` 注册新 key，并对每个新 key 发 `RunRequest(partition_key=key, run_key=key)`（`run_key` 去重，保证同一样本只触发一次）。
- **写一次语义**：登记后不因文件内容变化自动重跑；需要时在 UI 手动 re-materialize。
- 用 `run_key`（而非 cursor）做幂等去重，简单可靠。

### 5.4 QC asset（`h5ad_qc`，按 `h5ad_samples` 分区）
- 签名：`@asset(partitions_def=h5ad_partitions, check_specs=[...], retry_policy=RetryPolicy(max_retries=2))`。
- 流程：解析路径 → `anndata.read_h5ad(path, backed='r')`（避免整载表达矩阵）→ 计算 QC → 产出 `MaterializeResult(metadata=...)` 与若干 `AssetCheckResult`。
- **默认 QC 指标**（QC 计算抽成纯函数，便于单测与扩展）：
  - 结构类（便宜）：`n_cells`、`n_genes`、`X` dtype、是否稀疏 + 稀疏度、有无 `raw`、`layers`/`obsm`/`obsp` 键、`obs`/`var` 列名与数量。
  - 计数类：总 counts、每细胞中位 counts、每细胞中位 genes、`mito_pct`（`MT-/mt-` 基因）、`ribo_pct`（`RPS/RPL`）。
  - 数据性质：`X` 是否近似整数（原始 counts vs 已归一化/log）——重要分诊信号。
  - 文件：磁盘大小、mtime、h5ad 路径。
- 大文件内存：在 backed 模式下结构类指标几乎零成本；计数类指标按需分块/流式读取 `X`（实现细节，留待 plan）。

### 5.5 Asset checks（阈值分诊）
- `min_cells`：`n_cells >= MIN_CELLS`。
- `max_mito_pct`：中位 `mito_pct <= MAX_MITO_PCT`。
- `is_raw_counts`：`X` 近似整数（提示是否为原始 counts）。
- 软门禁：检查不过 → **红色 check、run 仍成功**（仍能看到 metadata 做分诊）。阈值来自 `CurationSettings`。

---

## 6. 错误处理
- **硬错误**（h5ad 打不开/损坏/`.done` 在但文件缺失/读取异常）：抛 `dagster.Failure(description=原因, metadata=...)` → 该 partition 在 UI 显示**红色 run + 失败原因**。
- **瞬时 I/O 抖动**（Lustre）：`RetryPolicy(max_retries=2)`。
- **软问题**（QC 阈值不达标）：见 5.5，走 asset check（红 check、绿 run）。
- **幂等**：不写文件，重跑任意 partition 安全无副作用。

---

## 7. 环境集成（方案 A）与运行

一次性把项目装进 `dl2025`：
```bash
uv pip install \
  -p /scratch/users/chensj16/venvs/dl2025/.venv/bin/python \
  -e /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
```

设置配置并启动（开发期，计算节点上）：
```bash
export SC_CURATION_WATCH_DIR=/scratch/users/chensj16/<your-watch-dir>
# 用 dl2025 环境的 dg（与项目同为 1.13.10）
dg dev   # 从项目目录运行；UI: http://localhost:3000（本地 ssh -L 3000:<节点>:3000 转发）
```

- 项目自带 `.venv` 闲置（保留无害，亦可删除以免混淆）。
- 生产化（sensor 后台常驻）后续单独设计。

---

## 8. 测试策略（pytest + Dagster 测试工具，在 `dl2025` 环境跑）
- **QC 纯函数**：构造小型合成 `AnnData` → 写临时 h5ad → 跑 QC → 断言各指标（含正常、空、已归一化等情形）。
- **Asset 物化**：`materialize([h5ad_qc], partition_key=..., resources={...})` 断言 metadata 与 AssetCheckResult；损坏文件断言抛 `Failure`。
- **Sensor**：临时 watch 目录放若干 folder/h5ad，部分缺 `.done`、部分缺 h5ad → 用 `build_sensor_context` 断言只有"有 `.done` 且有 h5ad"的被登记成 partition，且重复 tick 不重复触发（`run_key` 去重）。
- 需要时给 dev 依赖组加 `pytest`。

---

## 9. 验收标准（success criteria）
1. 在 watch 目录新建 `foo/x.h5ad` 后**再**放 `foo/.done`，sensor 在一个 tick 内登记 `foo` partition 并触发一次 QC run。
2. 该 run 成功，UI 上 `h5ad_qc` 的该 partition 带有完整 QC metadata，asset checks 显示绿/红。
3. 没有 `.done` 的文件夹**不**被处理；重复 tick **不**重复触发同一样本。
4. 放入一个损坏 h5ad（+`.done`）→ 该 partition run 红色失败、附原因。
5. 全程不在源目录外落地文件、不修改源文件。
6. `dg check defs` 通过；测试全绿。

---

## 10. 未来工作（out of scope, 记录备忘）
- 扩展 QC 指标：doublet（scrublet/scvi）、hemoglobin%、必需 obs/var 字段存在性校验。
- 重度 curation 步骤（counts recovery、gene harmonize、scVI、cellbender）以 **Dagster Pipes + Slurm（sbatch）** 接入——方案 C，与方案 A 共存。
- 可选：把结果汇成中心 catalog（Parquet/DuckDB）或对接 `eca-platform`。
- 生产化：sensor 常驻 + 失败告警。

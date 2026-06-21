# 细胞级过滤(下游 asset `cell_filtered`)—— 设计 spec

> 状态:待 user 评审。日期:2026-06-20。
> 前序:在 [标准化 counts 产出](./2026-06-19-standardized-counts-output-design.md) 与 [基因名标准化](./2026-06-19-gene-name-standardization-design.md) 之上,新增一个**下游**过滤步骤。

## 1. 目标

在 `h5ad_qc`(标准化 + QC + 基因名标准化)产出标准化 `.h5ad` **之后**,新增一个细胞级质量过滤步骤:按**每个细胞检出的基因数**(counts 上 >0 的基因数)过滤,保留 `≥ 阈值`(默认 400)的细胞,剔除低于阈值的。产出**另一份** `_filtered.h5ad`,上游全量文件保留。

## 2. 架构

新增**下游分区 asset `cell_filtered`**,`deps=[h5ad_qc]`,沿用现有动态分区(一样本一分区)。仅在上游 `h5ad_qc` 成功后运行。

## 3. 数据流(每个样本 / 分区)

1. 推出上游标准化文件路径:`OUTPUT_DIR/<rel>/<name>.h5ad`(复用 `output_path_for(output_dir, partition_key, src_path)`;`src_path` 同上游来源,见 §6)。
2. 文件不存在 → `dg.Failure`(**可重试**:上游通常已产出,缺失可能是瞬时/调度抖动)。
3. `anndata.read_h5ad` 载入。
4. **在 `layers["counts"]` 上**算每个细胞检出基因数(>0 的基因数);保留 `genes_per_cell ≥ min_genes_per_cell`(默认 400)的细胞,行子集 `adata[keep_mask]`(X 与所有 layer/obs/obsm 随 anndata 切片同步)。
5. 过滤后 `n_cells_after < min_cells`(默认 100,复用现有阈值)→ `dg.Failure(allow_retries=False)`,**不写** `_filtered` 文件。
6. 写到同目录、加后缀:`OUTPUT_DIR/<rel>/<name>_filtered.h5ad`(写失败 → 可重试 `dg.Failure`)。
7. `yield dg.MaterializeResult`,metadata 见 §7。

## 4. 配置(settings.py)

- **新增** `min_genes_per_cell: int = 400`(env `SC_CURATION_MIN_GENES_PER_CELL`,经 `_env_int` 读取,空/非法 → 默认 400)。
- 复用 `min_cells`(过滤后下限)、`output_dir`。无其它新必填项。

## 5. 组件落点

- `defs/settings.py`:`CurationSettings` 加字段 `min_genes_per_cell: int = 400`;`build_curation_settings` 读 `SC_CURATION_MIN_GENES_PER_CELL`。
- 新 `defs/filter_cells.py`:纯函数
  `filter_cells_by_genes(adata, min_genes_per_cell) -> tuple[AnnData, int, int]`
  —— 在 `layers["counts"]` 上算每细胞检出基因数,返回 `(adata_filtered, n_before, n_after)`;不依赖 Dagster,可独立单测。
- 新 `defs/filtering.py`:`cell_filtered` asset(载入→过滤→门控→写出)+ `cell_filtered_job`(可选,与 `h5ad_qc_job` 对称;若 sensor 用 multi-asset job 触发则纳入,默认仅注册 asset)。
- `defs/registration.py`:`assets=[h5ad_qc, cell_filtered]`(新增 `cell_filtered`)。
- README + `.env` + `.env.example`:文档化 `SC_CURATION_MIN_GENES_PER_CELL`。

## 6. 上游来源路径的解析

复用 `h5ad_qc` 的 `resolve_h5ad_path(context, settings)` 拿到样本源 h5ad 路径,再用现有 `output_path_for(output_dir, partition_key, src_path)` 得到上游标准化文件路径(与 `h5ad_qc` 写出的同一路径),避免重复来源逻辑:

- `standardized = output_path_for(curation.output_dir, context.partition_key, resolve_h5ad_path(context, curation))`
- `filtered` 由 `standardized` 派生:在扩展名前插入 `_filtered`(如 `a.h5ad → a_filtered.h5ad`)。

一个小工具函数 `filtered_path_for(standardized_path) -> str` 负责后缀派生(便于单测)。

## 7. metadata

`filtered_output_path`、`source_standardized`(上游标准化文件路径)、`n_cells_before`、`n_cells_after`、`n_cells_removed`、`min_genes_per_cell`。

## 8. 破坏性操作 vs "不丢信息"

过滤删细胞是破坏性的。化解:**上游标准化文件(全细胞)永久保留**,过滤只产出**另一份** `_filtered.h5ad`。原始全量不丢,过滤是可追溯、可重算的衍生产物——与既有原则一致。

## 9. 错误处理

| 情形 | 行为 |
|---|---|
| 上游标准化文件缺失 / 读取失败 | `dg.Failure`(可重试) |
| 过滤后 `n_cells_after < min_cells` | `dg.Failure(allow_retries=False)`,不写 `_filtered` |
| 写 `_filtered` 失败(磁盘抖动) | `dg.Failure`(可重试) |
| `layers["counts"]` 缺失 | `dg.Failure(allow_retries=False)`(上游应保证存在;缺失=数据异常) |

沿用现有 `RetryPolicy(max_retries=2)`。

## 10. 触发方式

`cell_filtered` 声明 `deps=[h5ad_qc]`。sensor 触发的 job 选择**两个 asset**(`h5ad_qc` + `cell_filtered`),所以每个新样本一次 run 跑完整链:Dagster 按依赖顺序先物化 `h5ad_qc`(写出标准化文件到磁盘),再物化 `cell_filtered`(从磁盘读该文件并过滤)。

- 现有 `h5ad_qc_job` 的 selection 从 `assets("h5ad_qc")` 扩成 `assets("h5ad_qc", "cell_filtered")`(sensor 仍用同一 job;名字保留 `h5ad_qc_job` 或改名,实现时不改 key 以免破坏历史)。
- `cell_filtered` 通过 run tag `sc/h5ad_path` 与 `h5ad_qc` 共享同一源路径解析(§6),所以同一 run 内两者看到同一样本。

## 11. 测试策略

- `filter_cells_by_genes`:阈值边界(检出=400 保留、=399 删)、X 与其它 layer 随行子集同步、`n_before/n_after` 正确、全删=0 细胞的返回。
- `cell_filtered` asset:正常 → 写出 `_filtered.h5ad` + metadata 计数自洽(before−after=removed);过滤后 < `min_cells` → 快速失败、不写;上游文件缺失 → 失败。
- `settings`:`SC_CURATION_MIN_GENES_PER_CELL` 默认 400 + env 覆盖 + 非法退默认。

## 12. 已确认决策

- 独立**下游 asset** `cell_filtered`(非并入 h5ad_qc)。
- 阈值 env `SC_CURATION_MIN_GENES_PER_CELL`,默认 **400**;语义 = 保留 counts 检出基因数 ≥ 阈值的细胞。
- 输出**同目录加后缀** `_filtered.h5ad`;上游全量文件保留。
- 过滤后 < `min_cells`(默认 100,复用)→ 快速失败、不写。

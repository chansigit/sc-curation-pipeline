# 标准化 counts 产出 + counts 上重算 QC —— 设计 spec

> 状态:待 user 评审。日期:2026-06-19。
> 前序:本设计在 [2026-06-18 h5ad-qc 设计](./2026-06-18-sc-curation-pipeline-h5ad-qc-design.md) 之上做实质演进。

## 1. 目标

让单样本的 `h5ad_qc` 步骤从"只读 QC、不产出对象"演进为 **一步同时**:

1. **产出一个标准化的带 counts 的 AnnData**,写成新的 `.h5ad` 到独立输出目录(绝不碰源):
   - `layers["counts"]` = 该样本的总 counts(整数);
   - `X` = `normalize_total(target_sum=1e4)` + `log1p`;
   - **保留** velocity 等其它 layer 与 obs/var/obsm/obsp/uns。
2. **在 counts 上重算 QC**(指标、绘图)作为 asset metadata。
3. 用**两个硬门控**做收录判定:`min_cells`、`min_genes`(检测到的基因数);不达标 → 快速失败、不写输出。

counts 的"获取逻辑"(现有/raw/反推)下沉到独立模块 **stancounts**,本仓库只调用。

## 2. 关键背景:与原设计的取舍变化

| 维度 | 原设计(2026-06-18) | 本设计 |
|---|---|---|
| 产出 | 无外部文件,仅 metadata + checks | **写标准化 `.h5ad`** 到独立输出目录 + metadata |
| QC 算在哪 | 源文件的 X(可能是 scVI 值,指标失真) | **counts 上算**(指标才有意义) |
| 门控形态 | 软门控(标红但 run 绿、不拦) | **硬门控**(不达标 → 快速失败、不写输出) |
| 门控项 | `min_cells`、`max_mito_pct`、`is_raw_counts` | **`min_cells`、`min_genes`**;其余去除 |
| mito%/ribo% | `max_mito_pct` 是 check | 仅作 metadata + 画图,**不再门控**(很多数据无线粒体信息) |
| counts 选源 | 不涉及 | 下沉到 stancounts 新增 `get_counts()` |

设计原则仍坚持:**绝不修改源目录**;损坏/瞬时/画图失败的容错策略沿用既有。

## 3. 架构:单 asset、内部顺序

仍是"一个样本 = 一个 partition = 一次执行"。`h5ad_qc` 内部顺序(早门控、省算):

1. 解析源 h5ad 路径(沿用 `sc/h5ad_path` tag → watch_dir 回退);`h5py.is_hdf5` 不通过 → `Failure(allow_retries=False)`。
2. 全量载入 `adata`(标准化必须全载;大文件比纯 QC 重 —— 见 §11)。
3. `res = stancounts.get_counts(adata)` → `counts`(对齐到 `adata.var_names`)+ `source`;无法获取 → `Failure(allow_retries=False)`,不写输出。
4. **硬门控(早评估、省后续算力)**,均在 counts 上:
   - `n_cells = counts.shape[0]`;`n_cells < min_cells` → `Failure(allow_retries=False)`,metadata 带 `n_cells`/`min_cells`。
   - `n_genes_detected = #{基因 : 该基因在 ≥1 细胞中 counts>0}`;`< min_genes` → `Failure(allow_retries=False)`,metadata 带 `n_genes_detected`/`min_genes`。
5. 组装标准化 adata:`layers["counts"]=counts`;`X = normalize_total(1e4) + log1p`;保留其它 layer 与注释(§7)。
6. 写标准化 `.h5ad` 到 `SC_CURATION_OUTPUT_DIR/<源相对路径>`(§8);写失败 → `Failure`(可重试,磁盘抖动可能瞬时)。
7. 在 counts 上算完整 QC 指标(§10);从 counts 派生逐细胞数组画 QC 图(沿用 `plots.py`)。
8. `yield MaterializeResult`:metadata = 输出路径 + `counts_source` + 全部 QC 数字 + `qc_plots`。**不再 yield AssetCheckResult**(门控已是硬失败,不再是软 check)。

> 注:门控为硬失败 ⇒ 一个**成功(绿)**的 materialization 必然已通过两个门控;被拒样本看不到 QC 图(user 已确认接受)。原 3 个 AssetCheckSpec 全部移除。

## 4. stancounts 改动:新增 `get_counts()`

保持 stancounts 独立、通用(其它项目可复用)。新增一个公开函数 + 一个异常类。

```python
class CountsUnavailable(ValueError):
    """无法从该 AnnData 获取/反推出整数 counts。"""

def get_counts(
    adata,
    *,
    prefer_layers=("counts", "count", "raw_counts", "counts_raw",
                   "umi", "umis", "umi_counts", "X_counts"),
    exclude_layers=("spliced", "unspliced", "ambiguous",
                    "spliced_counts", "unspliced_counts", "matrix"),
    base="e",
    robust=True,
    allow_recovery=True,
    n_sample=200,
    seed=0,
) -> dict:
    """从任意 AnnData 获取对齐到 adata.var_names 的整数总 counts。

    Returns dict: {"counts": <n_obs×n_vars, 与 adata 对齐>, "source": <str>}
    （source ∈ {"layer:<名>", "X", "raw", "recovered"}；recovered 时附 "base"）。
    都拿不到 → raise CountsUnavailable。
    """
```

**优先级(命中即返回):**

1. **白名单 layer**:按 `prefer_layers` 顺序,layer 存在、不在 `exclude_layers`、且**整数校验**通过 → `source=f"layer:{名}"`。
2. **X 即 counts**:`adata.X` 整数校验通过 → `source="X"`。
3. **`adata.raw`**:`raw` 非空、`raw.X` 整数、且 `raw.var_names ⊇ adata.var_names` → 按 `var_names` 取子集对齐 → `source="raw"`。(raw 不覆盖全部基因则跳过,进 4。)
4. **反推**:`allow_recovery` 且 `detect_normalization(adata.X).is_log1p` → `reverse_log1p(adata.X, base=检测base)` → `source="recovered"`。
5. 否则 `raise CountsUnavailable`。

**命名消歧(user 强调):** spliced/unspliced 等 velocity 层也是整数,单靠整数校验会误选 → 必须靠 `exclude_layers` 排除 + `prefer_layers` 白名单。

**整数校验:** 复用 detect 的抽样思路(采样 `n_sample` 行,非零值 `≈round`),对超大矩阵廉价。

**测试(stancounts 内):** counts 层命中、X 整数命中、raw 对齐命中、log1p 反推命中、velocity 层被正确排除(不误选 spliced)、scVI/小数无 counts → `CountsUnavailable`。

## 5. pipeline 组件落点

- `defs/settings.py`:`CurationSettings` 字段调整 ——
  - **新增** `output_dir: str`(env `SC_CURATION_OUTPUT_DIR`,**必填**;缺失/空 → 清晰 `ValueError`,与 `watch_dir` 一致)。
  - **新增** `min_genes: int`(env `SC_CURATION_MIN_GENES`,默认 `5000`)。
  - **移除** `max_mito_pct`(及其 env)。`min_cells` 保留(默认 100)。
- `defs/standardize.py`(新):`build_standardized_adata(adata, counts, *, target_sum=1e4) -> AnnData` —— 设 `layers["counts"]`、`X=normalize_total+log1p`、保留其它 layer 与注释;`write_standardized(adata2, out_path)` —— 建目录并写 `.h5ad`。
- `defs/qc.py`:`compute_qc` 重构为**在传入 counts 矩阵上**算指标(不再自己读文件 / 不再判 X 是否 raw);`h5ad_qc` asset 串联 §3 的流程。
- `defs/plots.py`:不变(逐细胞数组改由 counts 派生)。
- `defs/sensors.py`、`registration.py`、`partitions.py`:不变。
- asset 名:**保持 `h5ad_qc`**(避免改 partition/asset key 破坏历史);职责已是"标准化 + QC"。

## 6. counts 选源规则

见 §4 的优先级与命名消歧。pipeline 侧只负责:拿到 `counts`/`source` 后做归一化、组装、写文件、记录 `counts_source` 到 metadata。

## 7. 归一化配方与对象组装

- `X = sc.pp.normalize_total(target_sum=1e4)` 后 `sc.pp.log1p`(标准 scanpy lognorm)。
- `layers["counts"]` = 对齐后的整数 counts。源 counts 若来自异名 layer(如 `raw_counts`),**重命名**为 `counts`(不保留旧名)。
- **保留**:velocity 层(spliced/unspliced 等)、其它已有 layer、`obs`/`var`/`obsm`/`obsp`/`uns`。
- `var` 维度以 `adata.var_names` 为准(raw 超集时已在 get_counts 内对齐)。

## 8. 输出落点与命名

- 根目录:`SC_CURATION_OUTPUT_DIR`(必填)。
- 路径:**镜像源相对结构** —— 源 `watch_dir/<rel>/<name>.h5ad` → `OUTPUT_DIR/<rel>/<name>.h5ad`。按需建目录;已存在则覆盖(重新 materialize 即重算重写)。
- 绝不写入源目录。

## 9. 门控(硬)

| 门控 | 条件(在 counts 上) | 不达标 |
|---|---|---|
| `min_cells` | `n_cells ≥ min_cells`(默认 100) | `Failure(allow_retries=False)`,不写输出 |
| `min_genes` | `n_genes_detected ≥ min_genes`(默认 5000) | `Failure(allow_retries=False)`,不写输出 |

`n_genes_detected` = 在 ≥1 个细胞中 counts>0 的基因数。`max_mito_pct`、`is_raw_counts` 门控移除。

## 10. QC metadata 与绘图(均在 counts 上)

- 数字:`n_cells`、`n_genes_detected`、`n_vars`、`total_counts`、`median_counts_per_cell`、`median_genes_per_cell`、`density`、`sparsity`、`mito_pct`(无 MT 基因时为 0/N/A)、`ribo_pct`、结构信息(layers/obsm/obs/var 列等)、`counts_source`、`output_path`。
- `qc_plots`:沿用经典面板(3 小提琴 counts/genes/mito% + 2 scatter),逐细胞数组从 counts 派生;无 MT 时 mito 小提琴为平直(已容错)。画图失败 → 非致命降级。

## 11. 错误处理 / 性能

| 情形 | 行为 |
|---|---|
| 非 HDF5/损坏 | `Failure(allow_retries=False)`,清晰原因(沿用) |
| 无法获取 counts(`CountsUnavailable`) | `Failure(allow_retries=False)`,不写输出 |
| `n_cells`/`n_genes_detected` 不达标 | `Failure(allow_retries=False)`,不写输出,metadata 带数值 |
| 合法 HDF5 但读取/标准化中瞬时错误 | `Failure`(可重试,沿用 RetryPolicy(max_retries=2)) |
| 写输出文件失败 | `Failure`(可重试) |
| 画图失败 | 非致命,`qc_plots` 降级为说明(沿用) |

**内存**:标准化需全量载入矩阵,大样本显著重于原纯 QC(原 QC 是 backed 流式)。共享节点上需留意;本步定位即"较重的转换步"。

## 12. 依赖

- stancounts:以 editable 方式装入 dl2025 venv(`pip install -e /home/users/chensj16/s/projects/stancounts`),并在本仓库 `pyproject.toml` dev 组声明。
- scanpy(normalize_total/log1p)、anndata、matplotlib:dl2025 已有,dev 组已声明/补充。

## 13. 已确认项

- `SC_CURATION_MIN_GENES` 默认值 = **5000**(整数据集"检测到的基因数"下限,用于剔除过小/截断的数据)。
- asset **不改名**,保持 `h5ad_qc`(职责现为"标准化 + QC")。

## 14. 测试策略(概览)

- **stancounts**:§4 各选源路径 + velocity 排除 + 无 counts 抛错。
- **standardize**:counts 入层、X 为 lognorm、velocity 层保留、var 对齐;写出文件落在 OUTPUT_DIR 而非源目录。
- **asset**:正常样本 → 写出 + QC metadata + qc_plots;`min_cells`/`min_genes` 不达标 → 失败且不写输出、metadata 带数值;无 counts → 失败不写;损坏 → 快速失败;画图失败 → 非致命。
- **settings**:`output_dir` 必填校验、`min_genes` env、`max_mito_pct` 移除后的回归。

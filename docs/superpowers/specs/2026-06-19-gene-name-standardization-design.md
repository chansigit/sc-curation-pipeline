# 基因名标准化(物种由 `.species.<code>` 标记声明)—— 设计 spec

> 状态:待 user 评审。日期:2026-06-19。
> 前序:在 [标准化 counts 产出](./2026-06-19-standardized-counts-output-design.md) 之上演进。
> 复用独立包:**stangene**(`/home/users/chensj16/s/projects/stangene`,== `/scratch/users/chensj16/projects/stangene`,已 editable 装入 dl2025)。

## 1. 目标

在 `h5ad_qc` 步骤里增加"基因名标准化":把数据集里五花八门的基因标识(Ensembl ID / symbol / 其它 ID;新旧 symbol;临时占位名;不同物种命名)统一成一套规范基因 symbol。物种**不靠自动识别**,而由数据采集时放置的 `.species.<code>` 标记文件显式声明。harmonize 逻辑全部复用 stangene(已有完整 5 级匹配级联 + 10 物种离线参考),pipeline 只调用 + 应用"改名"策略。

## 2. 关键背景:stangene 现状(已探查)

- 已是成熟包,公开 API:`run / load_features / classify_features / load_reference / harmonize / merge_features / 报告函数`;`HarmonizationResult.mapping_table` 含 `gene_symbol_harmonized` / `gene_id_harmonized` / `mapping_status` / `mapping_confidence` / `mapping_source` / `mapping_notes` 等列,并保留 `original_feature_name` / `original_feature_id`。
- **离线参考已随包附带**:`src/stangene/data/refs/<species>/*.parquet`,10 物种:human, mouse, rat, zebrafish, fruit_fly, c_elegans, cynomolgus, rhesus, marmoset, mouse_lemur。**无需联网**。
- harmonize 设计原则:**只加列、绝不覆盖 `var_names`**。因此"把 var_names 换成规范 symbol"是 **pipeline 侧策略**,不进 stangene。
- **缺口**:stangene 不自动识别物种(`run(path, species, ...)` 要传入物种);也没有内存版 adata 入口(`load_features` 只从文件路径读)。本设计补这两点。

## 3. 数据采集新规矩

一个样本文件夹被视为"可处理",需满足(在现有"`.done` + 恰好一个 `*.h5ad`"基础上新增):

- 存在 `.done`(上传完成,不变)。
- 存在恰好一个 `*.h5ad`(不变)。
- **存在一个 `.species.<code>` 标记文件**,`<code>` 为下表可识别的物种码。

### 物种码词表(短码 + stangene 全名都接受)

| 标记 code | stangene 物种 |
|---|---|
| `hs` / `human` | human |
| `mm` / `mouse` | mouse |
| `rn` / `rat` | rat |
| `dr` / `zebrafish` | zebrafish |
| `dm` / `fruit_fly` | fruit_fly |
| `ce` / `c_elegans` | c_elegans |
| `cyno` / `cynomolgus` | cynomolgus |
| `rhesus` | rhesus |
| `marmoset` | marmoset |
| `lemur` / `mouse_lemur` | mouse_lemur |

解析大小写不敏感(`.species.HS` == `.species.hs`)。文件内容忽略,物种码取自文件名 `.species.` 之后的部分。

## 4. stangene 新增(可复用、保持独立)

- `resolve_species(code: str) -> str`:把上表的短码/全名解析成 stangene 规范物种名;未知码 → `raise ValueError`(清晰列出支持的码)。别名表内置。
- `harmonize_anndata(adata, species, *, reference_dir=None) -> HarmonizationResult`:内存版一站式 ——
  1. 从 `adata.var` 构造 FeatureTable(`original_feature_name = adata.var_names`;若 `adata.var` 含 `gene_ids` 列则填 `original_feature_id`,与 stangene `_load_h5ad` 一致);
  2. `classify_features` → `load_reference(species)` → `harmonize`;
  3. 返回 `HarmonizationResult`(mapping_table 行序与 `adata.var_names` 一致)。
  **不写文件、不改 var_names**(与 `stancounts.get_counts` 同风格)。
- 版本号 bump(0.1.0 → 0.2.0)。

## 5. pipeline 改动

### 5.1 sensor / 发现(`defs/sensors.py`)
- `discover_samples` 在判定样本时,除 `.done` + 单一 h5ad 外,还要找到一个 `.species.<code>` 文件并解析出 code(大小写不敏感)。
- 返回结构带上 species code:`[(partition_key, h5ad_path, species_code), ...]`。
- RunRequest 增加 tag `sc/species`(连同已有 `sc/h5ad_path`)。
- **注意**:`.done` 存在但 `.species.*` 缺失/多于一个 → 该样本**仍被发现并触发 run**(便于快速失败暴露问题),species code 置空,交由 asset 快速失败。(避免"静默不处理"。)

### 5.2 asset `h5ad_qc`(`defs/qc.py`)执行顺序
```
resolve_h5ad_path → is_hdf5 fast-fail
  → read_h5ad
  → 解析物种(廉价,早失败):取 sc/species tag,缺失则回退扫描样本文件夹的 .species.* 文件
       → resolve_species(code);缺失/多个/未知 → Failure(allow_retries=False),不写输出
  → get_counts(对齐原 var_names)
  → 硬门控 min_cells / min_genes(在 counts 上;不达标 Failure(allow_retries=False),不写输出)
  → harmonize_anndata(adata, species) → 应用改名策略(§5.3)
  → build_standardized_adata(counts + lognorm X) → write 到 OUTPUT_DIR
  → 在 counts 上算 QC + 画图(此时 var_names 已是 symbol,MT- 识别更准)
  → 一个 MaterializeResult
```
顺序理由:物种解析是 tag/文件扫描,远比 get_counts/标准化便宜,放最前可让"忘放物种标记"的样本尽快失败、不白做重活。

### 5.3 改名策略(pipeline 侧,user 已选"换成规范 symbol")
对 `adata`(及与之同轴的 counts):
- 新 `var_names[i]` = 该 feature 的 `gene_symbol_harmonized`;**为空 / `mapping_status` ∈ {unmapped, non_gene_feature} 的保留原名**。
- 原始名存入 `adata.var["original_feature_name"]`。
- 重名 → `anndata` 风格 `make_unique`(加 `-1`/`-2` 后缀)。
- 把 stangene mapping_table 的列(`gene_id_harmonized` / `gene_symbol_harmonized` / `mapping_status` / `mapping_confidence` / `mapping_source` / `mapping_notes`)并入 `adata.var`。
- 矩阵列(counts / X / layers)不动,仅改标签;改名在 standardize 之前,确保写出的标准化文件与 QC 都用规范名。

### 5.4 settings
无新增必填项(物种来自 per-sample 标记,不是全局 env)。可选:`SC_CURATION_SPECIES_MARKER_PREFIX` 默认 `.species.`(YAGNI,默认硬编码 `.species.` 即可,本设计不加 env)。

## 6. 错误处理

| 情形 | 行为 |
|---|---|
| 缺 `.species.*` / code 不认识 / 物种不被 stangene 支持 | `Failure(allow_retries=False)`,不写输出,原因 + 支持码列表写 metadata |
| 多个 `.species.*` 标记 | `Failure(allow_retries=False)`(歧义),不写输出 |
| harmonize 内部(参考缺失等) | 参考已随包附带,正常不发生;若 `load_reference` 抛错 → `Failure`(可重试) |
| 未映射基因 | **正常**,保留原名 + `mapping_status=unmapped`,不拦样本 |
| 损坏 / 无 counts / 门控不达标 / 瞬时 / 写失败 / 画图失败 | 沿用现有策略 |

## 7. metadata 新增

`species`(规范名)、`species_code`(原始标记码)、`n_genes_mapped`、`n_unmapped`、`mapping_rate`(= mapped / 基因 feature 数)、`harmonized`(bool)。其余沿用(output_path / counts_source / n_cells / n_genes_detected / sparsity / qc_plots …)。

## 8. 组件落点

- **stangene**:`resolve_species`(新,放 `species.py` 或新 `aliases`);`harmonize_anndata`(新,放 `__init__.py` 或 `run.py`);导出 + 测试 + 版本 bump。
- **pipeline**:
  - `defs/sensors.py`:发现逻辑 + `sc/species` tag。
  - `defs/qc.py`:asset 串入 harmonize + 改名;解析物种(tag / 回退扫描);metadata 新增。
  - 新 `defs/harmonize_apply.py`(或并入 qc.py):改名策略 `apply_harmonization(adata, result) -> adata`(小而专,便于单测)。
  - 依赖:`pyproject.toml` dev 组加 `stangene` + `[tool.uv.sources]` editable 本地路径(同 stancounts)。
  - README:文档化 `.species.<code>` 规矩 + 物种码表 + 改名行为。

## 9. 测试策略

- **stangene**:`resolve_species` 覆盖全部 10 物种的短码 + 全名 + 未知码报错;`harmonize_anndata` 在内存 adata(含/不含 `gene_ids` 列)上返回与 var 对齐的 mapping。
- **harmonize_apply**:映射→symbol;未映射保留原名;重名 make_unique;原名入 `original_feature_name`;mapping 列并入 var;矩阵不变。
- **asset**:端到端(给定 `sc/species` tag → 写出文件 var_names 为 symbol + metadata 有 species/mapping_rate);缺/错物种标记 → 快速失败、不写输出;回退扫描 `.species.*` 文件生效。
- **sensor**:`.done` + `.species.hs` → 发现且 tag 带 species;`.done` 无 species 标记 → 仍发现(交 asset 失败)。

## 10. 已确认决策

- 物种来源:`.species.<code>` 标记文件(短码 + 全名);**不自动识别、不交叉校验**。
- var_names 落地:**换成规范 symbol**(未映射留原名;make_unique;原名入列)。
- 缺/错物种标记:**发现并快速失败、不写输出**。
- 覆盖物种:stangene 全部 10 个(含食蟹猴 / 恒河猴 / 狨猴 / mouse lemur)。
- asset 名保持 `h5ad_qc`。

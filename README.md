# sc-curation-pipeline

用 **Dagster** 监控一个目录、对新上传的单细胞 `.h5ad` 文件自动做轻量 **QC**,结果以 **Dagster asset metadata + asset checks** 的形式呈现在 Web UI 里——**不额外落地任何文件**,也不改动源数据。

> 约定:**一个文件夹 = 一个样本 = 一个 `.h5ad`**。上传完成后,在该文件夹里放两个空标记文件来触发处理:`.done`(上传完成)+ `.species.<code>`(物种,如 `.species.hs`)。

---

## 它是怎么工作的

```
$SC_CURATION_WATCH_DIR/   (可配置, 在 $SCRATCH 下, 递归扫描)
  ├─ GSE123_sampleA/  { a.h5ad, .done } ─┐
  ├─ GSE123_sampleB/  { b.h5ad        }  │  sensor  watch_h5ad_dir (默认每 ~30s 一跳)
  └─ proj/pbmc/       { p.h5ad, .done } ─┘    1. 递归找含 .done 的文件夹
                                              2. 文件夹里恰好一个 *.h5ad 才算数
                                              3. 没处理过的 → 注册 dynamic partition + 触发一次 run
                                                   │
        (sampleB 没有 .done → 暂不处理)            ▼
                                            asset  h5ad_qc   (1 分区 = 1 个样本)
                                              ├─ anndata.read_h5ad(backed='r')  省内存
                                              ├─ 恢复 counts + 标准化 + 基因名标准化
                                              ├─ 用 scanpy 算 QC、写进物化 metadata
                                              ├─ 硬性阈值不达标 → 快速失败(红 run)
                                              └─ 通过 → 写标准化 .h5ad 到 OUTPUT_DIR
                                                   │  (同一 run 内自动接力)
                                                   ▼
                                            asset  cell_filtered   (deps=["h5ad_qc"])
                                              ├─ 读上一步的标准化 .h5ad
                                              ├─ 按每细胞检出基因数过滤(默认 ≥400)
                                              └─ 写 *_filtered.h5ad(全细胞文件原样保留)
                                                   │
                                                   ▼
                                            Dagster UI: 每样本一格 + QC metadata + 绿/红 run
```

- **打不开 / 损坏 / 缺文件** → 该分区的 run **变红**(`dagster.Failure`),失败原因在 metadata 里。
- **未达到硬性阈值**(细胞数 < `SC_CURATION_MIN_CELLS` 或基因数 < `SC_CURATION_MIN_GENES`)→ run **变红**(`dagster.Failure`),**不写输出文件**。
- **通过阈值** → run 变绿,标准化 `.h5ad` 写入 `SC_CURATION_OUTPUT_DIR`,QC metadata 在 Asset UI 里可查。
- **同一个 run 紧接着跑 `cell_filtered`**:按每细胞检出基因数(默认 ≥ `SC_CURATION_MIN_GENES_PER_CELL`)过滤,另写一个 `*_filtered.h5ad`;过滤后剩余细胞 < `SC_CURATION_MIN_CELLS` 则该步快速失败、不写过滤文件。

---

## 前置条件

- 在 **Sherlock 计算节点**上运行(用 `sh_dev` 或 `salloc` 拿一个交互节点,**别在登录节点跑**)。
- 一个装好 `dagster==1.13.10` + `scanpy` + `anndata` 的 Python 环境。本仓库用现成的 uv 环境 **`dl2025`**(Python 3.12):
  - 路径 `/scratch/users/chensj16/venvs/dl2025/.venv` —— 这是作者环境,换机器/换人请相应调整。

> 下文命令里出现的 `dl2025` 路径,都按你自己的运行环境替换即可。

---

## 快速开始(TL;DR)

> 首次需先安装(见下方 **第 1 节**)。装好后,每次开工只需:

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
cp .env.example .env        # 首次:填好 SC_CURATION_WATCH_DIR(以后不用再 cp)
SC_UI_BASIC_AUTH="csj:强密码" scripts/serve-ui.sh up   # 后台起 dg dev + ngrok 隧道
```

打开 **https://csj.ngrok.io**,在 UI 里把 `watch_h5ad_dir` sensor 开成 **ON**。往 watch 目录放样本(**先 `*.h5ad`,再 `.done`**),≤30s 后该样本的 QC 就出现在 **Assets → `h5ad_qc`**。

```bash
scripts/serve-ui.sh status   # 状态 + 本地/公网 URL
scripts/serve-ui.sh logs     # 两边日志
scripts/serve-ui.sh down     # 全部停掉
```

> 只想本地看、不公开暴露:省掉 `SC_UI_BASIC_AUTH` 会**裸奔**(脚本会警告);或改用 SSH 转发(见第 4 节)。

---

## 1. 安装(把项目装进运行环境)

一次性把本项目以**可编辑方式**装进 `dl2025`:

```bash
uv pip install -p /scratch/users/chensj16/venvs/dl2025/.venv/bin/python \
  -e /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
```

验证能 import:

```bash
/scratch/users/chensj16/venvs/dl2025/.venv/bin/python -c "import sc_curation_pipeline; print('ok')"
```

---

## 2. 配置(环境变量)

| 变量 | 默认 | 说明 |
|---|---|---|
| `SC_CURATION_WATCH_DIR` | **(必填)** | 要监控的根目录(放在 `$SCRATCH` 下) |
| `SC_CURATION_OUTPUT_DIR` | **(必填)** | 标准化 `.h5ad` 的输出目录(写入 `$SCRATCH` 下) |
| `SC_CURATION_DONE_MARKER` | `.done` | 上传完成标记文件名 |
| `SC_CURATION_H5AD_GLOB` | `*.h5ad` | 文件夹内匹配 h5ad 的模式 |
| `SC_CURATION_SCAN_INTERVAL_SEC` | `30` | sensor 最小扫描间隔(秒;在 `dg dev` 启动时读取) |
| `SC_CURATION_MIN_CELLS` | `100` | 硬性阈值:细胞数低于此值快速失败(无输出) |
| `SC_CURATION_MIN_GENES` | `5000` | 硬性阈值:检测到的基因总数(在 ≥1 个细胞中 counts>0 的基因数)低于此值快速失败(无输出) |
| `SC_CURATION_MIN_GENES_PER_CELL` | `400` | 细胞级过滤(`cell_filtered` 步骤):每个细胞检出基因数低于此值的细胞被剔除 |

`SC_CURATION_WATCH_DIR` 是必填的——没设会立刻报错(注册资源时就要读它)。可选变量留空或写成非法值会**安全退回默认值**(不会让服务崩溃)。

### 怎么设置这些变量

三种方式,**推荐第 ① 种**:

**① `.env` 文件(推荐 —— `dg` 启动时自动加载)**
在项目根目录放一个 `.env`,`dg dev` / `dg check defs` 会自动把它加载进环境,**不用每次 `export`**。仓库带了模板 `.env.example`:
```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
cp .env.example .env        # 然后编辑 .env,至少填好 SC_CURATION_WATCH_DIR 和 SC_CURATION_OUTPUT_DIR(两者必填)
```
`.env` 已在 `.gitignore` 里、不会被提交。内容示例:
```dotenv
SC_CURATION_WATCH_DIR=/scratch/users/chensj16/sc-curation-watch
SC_CURATION_OUTPUT_DIR=/scratch/users/chensj16/sc-curation-output
SC_CURATION_MIN_CELLS=200
SC_CURATION_MIN_GENES=5000
SC_CURATION_MIN_GENES_PER_CELL=400
# 其余不写就用默认值
```

**② 临时 `export`(只对当前 shell 生效)**
```bash
export SC_CURATION_WATCH_DIR=/scratch/users/chensj16/sc-curation-watch
export SC_CURATION_MIN_CELLS=200
```
在同一个 shell 里再 `dg dev` 即可。

**③ 写进 SLURM 作业脚本(以后跑常驻/批处理时)**
在 sbatch 脚本里 `export` 这些变量,或 `cd` 到项目目录靠 `.env` 自动加载。

> 注:本项目用 `os.getenv` 读取(不是 `dg.EnvVar`),所以 `dg list envs` 不会列出它们——以上面那张表为准。
> 如果以后加了 **GitHub Actions** CI:把值放进仓库 **Settings → Secrets and variables → Actions**,再在 workflow 的 `env:` 里注入(不要把真实路径/密钥写进会提交的文件)。

---

## 3. 目录约定

```
$SC_CURATION_WATCH_DIR/
├── GSE123_sampleA/
│   ├── matrix.h5ad
│   ├── .species.hs    ← 物种声明(人=hs),基因名标准化要用
│   └── .done          ← 上传完成后"再"放这个,sensor 才会处理
├── GSE123_sampleB/
│   └── matrix.h5ad    ← 没有 .done → 暂不处理(视为还在上传)
└── proj/
    └── pbmc/
        ├── pbmc.h5ad  ← 支持任意层级嵌套
        ├── .species.mm
        └── .done
```

**关键:先把 h5ad 传完,再放 `.done`。** 这样 sensor 永远不会去碰一个还没写完的文件。

**物种标记 `.species.<code>`**(基因名标准化需要;缺失/无法识别/多于一个 → run 快速失败、不写输出):

| code | 物种 | code | 物种 |
|---|---|---|---|
| `hs` / `human` | 人 | `cyno` / `cynomolgus` | 食蟹猴 |
| `mm` / `mouse` | 小鼠 | `rhesus` | 恒河猴 |
| `rn` / `rat` | 大鼠 | `marmoset` | 狨猴 |
| `dr` / `zebrafish` | 斑马鱼 | `lemur` / `mouse_lemur` | 鼠狐猴 |
| `dm` / `fruit_fly` | 果蝇 | `ce` / `c_elegans` | 线虫 |

(短码、全名均可;大小写不敏感。)

---

## 4. 启动 Dagster

先在项目目录用 `dl2025` 的 `dg` 把服务跑起来(**两种调试方式共用这一步**):

```bash
export SC_CURATION_WATCH_DIR=/scratch/users/chensj16/<你的watch目录>
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
/scratch/users/chensj16/venvs/dl2025/.venv/bin/dg dev -p 27182
```

> 如果你已经建好 `.env`(见上面「怎么设置这些变量」),上面那行 `export` 就能省掉——`dg dev` 会自动加载 `.env`。

`dg dev` 跑在**计算节点**的 27182 端口。怎么看到 UI,按场景从下面两节里选一种。

### 4.1 本机调试(SSH 端口转发)

自己一个人临时看、不对外暴露,最简单也最安全。从本地电脑做端口转发到那个计算节点:

```bash
ssh -L 27182:<计算节点名, 如 sh02-06n11>:27182 <你的SUNet>@login.sherlock.stanford.edu
# 然后浏览器打开 http://localhost:27182
```

只要 SSH 还连着,本地的 `http://localhost:27182` 就指向计算节点上的 UI;断开转发即收回,不会留下任何对外入口。

### 4.2 ngrok 公网调试(固定域名)

想从任意设备访问、或分享给别人,不想每次都 SSH 转发时用。

**方式一(手动)**:`dg dev` 继续在前台开着,在**同一个计算节点**上另开一个终端跑 ngrok:
```bash
# authtoken 一次性配置(~/.config/ngrok/ngrok.yml 里已有则跳过):
# ngrok config add-authtoken <你的token>

# 给 UI 加一层认证(强烈建议!),再暴露到你的固定域名:
ngrok http 27182 --domain=csj.ngrok.io --basic-auth "你:一个强密码"
```
然后任意地方浏览器打开 https://csj.ngrok.io 。

**方式二(脚本,推荐)**:用仓库里的 `scripts/serve-ui.sh` —— **一键把 `dg dev`(后台)+ ngrok 隧道一起拉起/关掉**,`dg dev` 不再占着终端(配置走 `SC_UI_PORT` / `SC_UI_NGROK_DOMAIN` / `SC_UI_BASIC_AUTH`;run 历史/已登记样本存到项目内 gitignored 的 `.dagster_home/`,重启不丢)。脚本自己会在后台拉起 `dg dev`,所以走这条时**不必**先手动跑上面那条前台 `dg dev`:
```bash
SC_UI_BASIC_AUTH="csj:一个强密码" scripts/serve-ui.sh up   # 启动(后台)
scripts/serve-ui.sh status                                # 看状态 + 公网 URL
scripts/serve-ui.sh down                                  # 断开
```

> ⚠️ 公网暴露注意(仅 4.2 相关):
> - **Dagster UI 默认没有登录认证**,而它能触发/取消 run(等于在集群上跑代码)。公开到公网前**务必加 `--basic-auth`(或 `--oauth`)**,否则拿到 URL 的人就能操作你的 pipeline;用完 `Ctrl-C` 关掉隧道。
> - 从共享 HPC 对公网暴露服务,请确认符合 Stanford SRC 使用规范。
> - 小坑:ngrok 域名 DNS 会先给 IPv6(本节点 IPv6 不通),ngrok 自动回退 IPv4(已验证可连);隧道起得慢等几秒即可。

### 4.3 打开 sensor

不管用哪种方式进 UI,最后都要:**Automation → `watch_h5ad_dir`**,开关拨到 **ON**(它默认是 `STOPPED`,不打开不会扫描)。

---

## 5. 端到端走一遍

```bash
# 造一个样本(注意顺序:先 h5ad 和 .species,最后才放 .done)
mkdir -p "$SC_CURATION_WATCH_DIR/demo_sample"
cp /path/to/your.h5ad "$SC_CURATION_WATCH_DIR/demo_sample/demo.h5ad"
touch "$SC_CURATION_WATCH_DIR/demo_sample/.species.hs"   # 物种:人=hs(必需,见第 3 节)
touch "$SC_CURATION_WATCH_DIR/demo_sample/.done"         # 最后放:它一出现 sensor 就会触发
```

- 一个 tick(≤30s)内,sensor 注册分区 `demo_sample` 并触发一次 `h5ad_qc` run。
- 在 UI **Assets → `h5ad_qc` → 选 `demo_sample` 分区**,可以看到:
  - **Metadata**:`output_path`、`counts_source`、`n_cells` / `n_genes_detected` / `n_vars` / `total_counts` / 每细胞中位 `counts`/`genes` / `sparsity` / `layers` / `obsm`,以及 `qc_plots`(内嵌图)。
  - 不达 `min_cells` / `min_genes` 阈值的样本会**快速失败(红 run)、不写输出**,原因写在 metadata。
  - 通过阈值后,标准化 `.h5ad` 写入 `SC_CURATION_OUTPUT_DIR`。
- 试验失败路径:
  - **忘放 `.species.<code>`**(只 `touch .done`)→ run 变红,原因 `missing or ambiguous .species.<code> marker`,不重试、不写输出。
  - 放一个损坏的 h5ad(+ 两个标记)→ 那个分区的 run 变红,原因写在 metadata。
  - 用低细胞数样本触发 `min_cells` / `min_genes` 硬性阈值快速失败。

---

## 6. QC 指标 & 检查

- **规模 / 结构**:`n_cells`、`n_vars`、`n_genes_detected`(在 ≥1 个细胞中 counts>0 的基因数)、`sparsity`(counts 矩阵零元素占比)、`layers`、`obsm`。
- **计数**:`total_counts`、每细胞中位 `counts` / `genes`。
- **来源 / 输出**:`counts_source`(counts 取自哪)、`output_path`、`source_h5ad`。
- **物种 / 基因名**:`species`(规范名)、`species_code`(标记原码)、`harmonized`、`n_genes_mapped` / `n_unmapped` / `mapping_rate`。
- **图**:`qc_plots` —— counts、genes 的小提琴 + counts×genes、counts×mito 散点。mito 现在**按物种识别**(`stangene.mito_mask`):哺乳类/灵长/斑马鱼用 `MT-` 前缀、灵长另认裸名(`ND1`/`COX1`/`CYTB`…)、果蝇用 `mt:` 前缀、线虫用专名(`nduo-`/`ctc-`/`ctb-1`/`atp-6`)。注:斑马鱼/狨猴参考库未收录 mtDNA 基因,这两种的 mito 可能为空。
- **硬性阈值(fast-fail,非 asset check)**:`min_cells`、`min_genes` —— 不达标直接失败、不写输出。

### 标准化输出(`h5ad_qc` step 新增)

通过 QC 硬性阈值的样本,`h5ad_qc` 会把原始对象标准化后写成一个新 `.h5ad` 到 `SC_CURATION_OUTPUT_DIR`(文件名由样本分区键自动生成)。

**标准化规则:**

- `layers["counts"]` — 原始整数 counts(来源优先顺序:输入 `layers["counts"]` → 矩阵 `X` → `.raw.X` → 由 stancounts `get_counts()` 自动恢复)
- `X` — 对 counts 做 `normalize_total(target_sum=1e4)` + `log1p` 的归一化结果
- velocity 相关 layers(`spliced` / `unspliced` / `ambiguous` 等)原样保留到输出
- `obs` / `var` / `obsm` / `obsp` 保持不变
- **基因名标准化**:由 `.species.<code>` 声明物种,调 stangene 把 `var_names` 统一成规范基因 symbol —— 映射到的换成官方 symbol(如 `p53`→`TP53`、Ensembl ID→symbol),**未映射的保留原名**,重名自动加后缀去重,原名存进 `var["original_feature_name"]`;stangene 的映射列(`gene_id_harmonized` / `mapping_status` / …)并入 `var`。支持 10 个物种(人/鼠/大鼠/斑马鱼/果蝇/线虫 + 食蟹猴/恒河猴/狨猴/鼠狐猴),参考数据离线随包附带。

**硬性快速失败(fast-fail)阈值:**

| 变量 | 默认 | 含义 |
|---|---|---|
| `SC_CURATION_MIN_CELLS` | `100` | 细胞数低于此值 → run 立即失败,**不写输出文件** |
| `SC_CURATION_MIN_GENES` | `5000` | 检测到的基因总数(在 ≥1 个细胞中 counts>0 的基因数)低于此值 → run 立即失败,**不写输出文件** |

未达到阈值的样本以 `dagster.Failure` 快速失败,原因写入 run 日志和 metadata——不会留下半截写好的文件。

**已移除:**`max_mito_pct`、`is_raw_counts`(检查项);以及 `mito_pct` / `ribo_pct` / `density`(QC metadata 数字)。mito 分布仍在 `qc_plots` 图里展示,但不再作为 metadata 数字输出(跨物种不可靠,见上)。

### 细胞级过滤(`cell_filtered` step,自动接在 `h5ad_qc` 之后)

`cell_filtered` 是一个**下游 asset**(`deps=["h5ad_qc"]`),和 `h5ad_qc` 同属一个 job——sensor 发现新样本时,一个 run 里会**先跑 `h5ad_qc`、再自动跑 `cell_filtered`**,无需手动触发。

- **读**:`h5ad_qc` 写到 `SC_CURATION_OUTPUT_DIR` 的标准化 `.h5ad`(用 `layers["counts"]` 算每细胞检出基因数)。
- **过滤**:剔除检出基因数 `< SC_CURATION_MIN_GENES_PER_CELL`(默认 400)的细胞;`X` 与所有 layers 一起按行子集。
- **写**:过滤后的对象写成**单独**的 `*_filtered.h5ad`(同目录、加后缀)——`h5ad_qc` 的全细胞文件**原样保留**,过滤是非破坏性的。
- **硬性快速失败**:过滤后剩余细胞数 `< SC_CURATION_MIN_CELLS` → `dagster.Failure`(红 run)、**不写输出**。
- **metadata**:`filtered_output_path`、`source_standardized`、`min_genes_per_cell`、`n_cells_before` / `n_cells_after` / `n_cells_removed`。

---

## 7. 重新处理某个样本

样本是**写一次**语义:登记后即使文件内容变了,也不会自动重跑。需要重算时,在 UI 选中该分区点 **Materialize** 即可。

---

## 8. 测试 & 校验

```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
# 单元测试(应为 25 passed)
/scratch/users/chensj16/venvs/dl2025/.venv/bin/python -m pytest tests/ -q
# 校验所有 Dagster 定义能正常加载
SC_CURATION_WATCH_DIR=/tmp/x /scratch/users/chensj16/venvs/dl2025/.venv/bin/dg check defs
```

---

## 项目结构

```
src/sc_curation_pipeline/
├── definitions.py            # 入口(load_from_defs_folder 自动发现 defs/)
└── defs/
    ├── settings.py           # CurationSettings(环境变量)+ 可逆的分区键编码
    ├── partitions.py         # h5ad_samples 动态分区
    ├── qc.py                 # compute_count_qc 等纯函数 + h5ad_qc asset + job(选 h5ad_qc + cell_filtered)
    ├── standardize.py        # build_standardized_adata / write_standardized
    ├── harmonize_apply.py    # apply_harmonization(把 stangene 结果写回 var_names)
    ├── filter_cells.py       # filter_cells_by_genes / filtered_path_for 纯函数
    ├── filtering.py          # cell_filtered 下游 asset(deps=["h5ad_qc"])
    ├── sensors.py            # discover_samples 扫描器 + watch_h5ad_dir sensor
    └── registration.py       # 把 asset / job / sensor / resource 打包成 Definitions
tests/                        # pytest(test_settings / test_qc / test_sensor / test_filter_cells / test_filtering / …)
docs/superpowers/             # 设计 spec 与实现 plan
```

---

## 故障排查

- **`dg check defs` 报 venv 不匹配警告**:无害——因为用 `dl2025` 跑、而项目里另有一个闲置的 `.venv`。想消除可在配置里把 `project_and_activated_venv_mismatch` 加进 `cli.suppress_warnings`。
- **`dg check defs` 抱怨 `SC_CURATION_WATCH_DIR` 没设**:它是必填项,临时给个值即可(如上 `/tmp/x`)。
- **sensor 不动**:确认 UI 里它是 **ON**(默认 STOPPED);确认样本文件夹里**有 `.done`** 且**恰好一个** `*.h5ad`。
- **UI 打不开**:确认 `dg dev` 在计算节点跑、并做了 `ssh -L 27182:<节点>:27182` 转发。

---

## 已知限制(开发阶段)

sensor 的去重按"分区是否已注册"判断。daemon 在"注册分区"与"提交 run"之间崩溃这种极端情况,可能漏掉一个样本(UI 里能看到未物化的分区,手动 Materialize 可恢复)。生产化(常驻 daemon + 失败告警)是后续项,见设计 spec 第 10 节。

---

## 设计文档

- 设计 spec:[`docs/superpowers/specs/2026-06-18-sc-curation-pipeline-h5ad-qc-design.md`](docs/superpowers/specs/2026-06-18-sc-curation-pipeline-h5ad-qc-design.md)
- 实现 plan:[`docs/superpowers/plans/2026-06-18-sc-curation-pipeline-h5ad-qc.md`](docs/superpowers/plans/2026-06-18-sc-curation-pipeline-h5ad-qc.md)

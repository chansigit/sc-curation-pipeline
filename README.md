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
                                            asset  standardized_h5ad   (1 分区 = 1 个样本)
                                              ├─ anndata.read_h5ad(backed='r')  省内存
                                              ├─ 恢复 counts + 标准化 + 基因名标准化
                                              ├─ 用 scanpy 算 QC、写进物化 metadata
                                              ├─ 硬性阈值不达标 → 快速失败(红 run)
                                              └─ 通过 → 写标准化 .h5ad 到 OUTPUT_DIR
                                                   │  (同一 run 内自动接力)
                                                   ▼
                                            asset  initially_filtered_h5ad   (deps=["standardized_h5ad"])
                                              ├─ 读上一步的标准化 .h5ad
                                              ├─ 按每细胞检出基因数过滤(默认 ≥400)
                                              └─ 写 *_filtered.h5ad(全细胞文件原样保留)
                                                   │  (同一 run 内自动接力)
                                                   ▼
                                            asset  doublet_scored_h5ad   (deps=["initially_filtered_h5ad"])
                                              ├─ 在 counts 上跑 Scrublet(有 sample 列就按 sample 分批)
                                              └─ 把 doublet_score / predicted_doublet 写回 *_filtered.h5ad
                                                   │  (同一 run 内自动接力)
                                                   ▼
                                            asset  mrvi_leiden_h5ad   (deps=["doublet_scored_h5ad"])
                                              ├─ 通过 Dagster Pipes 提交 Slurm GPU 作业训 MrVI(torch)
                                              ├─ 取 u latent → sc.pp.neighbors + sc.tl.leiden
                                              └─ 把 obsm["X_mrvi_u"] + obs["mrvi_leiden"] 写回 *_filtered.h5ad
                                                   │
                                                   ▼
                                            Dagster UI: 每样本一格 + QC metadata + 绿/红 run
```

- **打不开 / 损坏 / 缺文件** → 该分区的 run **变红**(`dagster.Failure`),失败原因在 metadata 里。
- **未达到硬性阈值**(细胞数 < `SC_CURATION_MIN_CELLS` 或基因数 < `SC_CURATION_MIN_GENES`)→ run **变红**(`dagster.Failure`),**不写输出文件**。
- **通过阈值** → run 变绿,标准化 `.h5ad` 写入 `SC_CURATION_OUTPUT_DIR`,QC metadata 在 Asset UI 里可查。
- **同一个 run 紧接着跑 `initially_filtered_h5ad`**:按每细胞检出基因数(默认 ≥ `SC_CURATION_MIN_GENES_PER_CELL`)过滤,另写一个 `*_filtered.h5ad`;过滤后剩余细胞 < `SC_CURATION_MIN_CELLS` 则该步快速失败、不写过滤文件。
- **再接着跑 `doublet_scored_h5ad`**:在 counts 上跑 Scrublet 算 doublet 分(识别到 `sample` 列就**按 sample 分批**),把 `doublet_score` / `predicted_doublet` 写回 `*_filtered.h5ad`。某个 sample 太小/退化 → 该 sample 打 NaN、非致命。
- **最后跑 `mrvi_leiden_h5ad`**(终端):通过 **Dagster Pipes 提交 Slurm GPU 作业**训 MrVI(torch 后端),取 **u latent** 做 Leiden 聚类,把 `obsm["X_mrvi_u"]` + `obs["mrvi_leiden"]` 写回 `*_filtered.h5ad`。GPU 训练在外部作业里,Dagster 进程只在 CPU 上轮询等待。

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

打开 **https://csj.ngrok.io**,在 UI 里把 `watch_h5ad_dir` sensor 开成 **ON**。往 watch 目录放样本(**先 `*.h5ad`,再 `.done`**),≤30s 后该样本的 QC 就出现在 **Assets → `standardized_h5ad`**。

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
| `SC_CURATION_MIN_GENES_PER_CELL` | `400` | 细胞级过滤(`initially_filtered_h5ad` 步骤):每个细胞检出基因数低于此值的细胞被剔除 |

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

- 一个 tick(≤30s)内,sensor 注册分区 `demo_sample` 并触发一次 `standardized_h5ad` run。
- 在 UI **Assets → `standardized_h5ad` → 选 `demo_sample` 分区**,可以看到:
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
- **污染比例(物种感知)**:`median_pct_counts_mt`、`median_pct_counts_hb` —— 每细胞 `pct_counts_mt` / `pct_counts_hb` 的中位数;per-cell 两列也写进输出 h5ad 的 `obs`(见下)。
- **元数据列识别(stanmetacols)**:`metacols_method`(走了 LLM 还是启发式)、`metacols_result`(markdown 表:全部角色的 top-1 解析 + 哪些被规范化进 obs)。详见下方「元数据列识别」。
- **图**:`qc_plots` —— 第一行 counts / genes / mito_pct 小提琴,第二行 counts×mito、counts×genes 散点 + hb_pct 小提琴。
  - **mito 按物种识别**(`stangene.mito_mask`):哺乳类/灵长/斑马鱼用 `MT-` 前缀、灵长另认裸名(`ND1`/`COX1`/`CYTB`…)、果蝇用 `mt:` 前缀、线虫用专名(`nduo-`/`ctc-`/`ctb-1`/`atp-6`)。注:斑马鱼/狨猴参考库未收录 mtDNA 基因,这两种的 mito 可能为空。
  - **hb(血红蛋白)按物种识别**(`stangene.hb_mask`):人/灵长用 HGNC 符号集(`HBA1`/`HBB`/`HBE1`…),鼠/大鼠用 `Hba-`/`Hbb-` 簇 + `Hbq1*`,斑马鱼用 `hbaa*`/`hbba*`/`hbae*`/`hbbe*`;显式集天然排除 `HBEGF`/`HBP1`/`HBS1L` 等同前缀的非血红蛋白基因。**果蝇/线虫无红细胞血红蛋白 → hb 恒为 0(不适用)**;灵长参考库 hb 收录稀疏(如食蟹猴为空),这些样本 hb 可能偏低。
- **硬性阈值(fast-fail,非 asset check)**:`min_cells`、`min_genes` —— 不达标直接失败、不写输出。

### 标准化输出(`standardized_h5ad` step 新增)

通过 QC 硬性阈值的样本,`standardized_h5ad` 会把原始对象标准化后写成一个新 `.h5ad` 到 `SC_CURATION_OUTPUT_DIR`(文件名由样本分区键自动生成)。

**标准化规则:**

- `layers["counts"]` — 原始整数 counts(来源优先顺序:输入 `layers["counts"]` → 矩阵 `X` → `.raw.X` → 由 stancounts `get_counts()` 自动恢复)
- `X` — 对 counts 做 `normalize_total(target_sum=1e4)` + `log1p` 的归一化结果
- velocity 相关 layers(`spliced` / `unspliced` / `ambiguous` 等)原样保留到输出
- `obs` 新增每细胞 `pct_counts_mt` / `pct_counts_hb`(物种感知污染比例),以及识别到的规范元数据列 `sample` / `cell_type_coarse` / `cell_type_fine` / `organ` / `tissue`(见下「元数据列识别」);`obsm` / `obsp` 不变(`var` 另含基因名标准化的映射列,见下)
- `uns["metacols"]` — stanmetacols 完整排名结果(JSON 字符串,`json.loads` 读回:`method` / `assigned` / `ranking`)
- **基因名标准化**:由 `.species.<code>` 声明物种,调 stangene 把 `var_names` 统一成规范基因 symbol —— 映射到的换成官方 symbol(如 `p53`→`TP53`、Ensembl ID→symbol),**未映射的保留原名**,重名自动加后缀去重,原名存进 `var["original_feature_name"]`;stangene 的映射列(`gene_id_harmonized` / `mapping_status` / …)并入 `var`。支持 10 个物种(人/鼠/大鼠/斑马鱼/果蝇/线虫 + 食蟹猴/恒河猴/狨猴/鼠狐猴),参考数据离线随包附带。

**硬性快速失败(fast-fail)阈值:**

| 变量 | 默认 | 含义 |
|---|---|---|
| `SC_CURATION_MIN_CELLS` | `100` | 细胞数低于此值 → run 立即失败,**不写输出文件** |
| `SC_CURATION_MIN_GENES` | `5000` | 检测到的基因总数(在 ≥1 个细胞中 counts>0 的基因数)低于此值 → run 立即失败,**不写输出文件** |

未达到阈值的样本以 `dagster.Failure` 快速失败,原因写入 run 日志和 metadata——不会留下半截写好的文件。

**已移除:**`max_mito_pct`、`is_raw_counts`(检查项);以及 `ribo_pct` / `density`(QC metadata 数字)。

**已重新加入(物种感知):**`pct_counts_mt` / `pct_counts_hb` —— 之前因"跨物种不可靠"删掉的 mito 数字,现在用 `stangene.mito_mask` / `hb_mask` 按物种识别,作为每细胞 `obs` 列 + `median_pct_counts_mt` / `median_pct_counts_hb` metadata 重新输出。

### 元数据列识别(`standardized_h5ad` step 内,stanmetacols)

`standardized_h5ad` 用 [stanmetacols](https://github.com/chansigit/stanmetacols) 对 `.obs` 做**完整元数据角色解析**(请求**全部角色**,含 organ / tissue 及数值 QC 角色),整体记录下来;但只把**分类/分组角色**规范化进 obs:`sample`、`cell_type_coarse`、`cell_type_fine`、`organ`、`tissue`。数值 QC 角色(`pct_mt`/`pct_hb`/`n_counts`/…)只展示、不写 obs —— 本步自算 `pct_counts_mt`/`pct_counts_hb`。

- **识别 + 规范化**:对每个可规范化角色取 top-1 候选,**仅当它是单列(`single`)、在 obs 里、且分数 ≥ 0.5** 时,把该列复制成同名规范列(`obs["sample"]` / `obs["cell_type_coarse"]` / `obs["cell_type_fine"]` / `obs["organ"]` / `obs["tissue"]`,**原列保留**)。一个源列只归分数最高的那个角色(避免重复)。`composite`(`"a + b"`)/ `barcode` 候选和低分候选**只记录、不自动建列**。
- **完整排名**写进 `uns["metacols"]`(JSON 字符串,含 `method` / `assigned` / `ranking`);Dagster metadata 给出 `metacols_method` 和 **`metacols_result`** —— 一张 markdown 表,逐角色列出 top-1 候选(列名/分数/kind/source)并标出哪些被规范化进 obs。
- **LLM vs 启发式**:`SC_CURATION_METACOLS_USE_LLM`(默认 `1` = LLM 优先);没 key / 无外网 / API 报错时自动回退离线启发式,`metacols_method` 会如实写明走了哪条。
- **LLM 后端可配**:`SC_CURATION_METACOLS_PROVIDER`(`anthropic` 默认,或 `openai` = 任意 OpenAI 兼容端点)、`SC_CURATION_METACOLS_MODEL`、`SC_CURATION_METACOLS_BASE_URL`、`SC_CURATION_METACOLS_API_KEY_ENV`(持有 key 的**环境变量名**,key 本身不进配置)。火山 ARK / 豆包示例:
  ```bash
  SC_CURATION_METACOLS_PROVIDER=openai
  SC_CURATION_METACOLS_BASE_URL=https://ark.cn-beijing.volces.com/api/v3
  SC_CURATION_METACOLS_MODEL=doubao-seed-2-0-pro-260215   # 或 deepseek-v4-pro-260425
  SC_CURATION_METACOLS_API_KEY_ENV=ARK_API_KEY
  ```
  - ⚠️ **Sherlock 计算节点跑批建议设 `SC_CURATION_METACOLS_USE_LLM=0`**:LLM-first 时每个样本都会发一次网络请求;若节点能建 TCP 连接但出网被防火墙/代理黑洞,Anthropic SDK 的读超时默认很长(可达数十分钟)才回退到启发式——非致命包装能保证不报错,但保不住墙钟时间。设 `0` 走离线启发式:确定可复现、零外部调用(测试即用此)。纯连接拒绝/DNS 失败约 15s 返回,属尾部风险。
- **非致命**:整段被 `try/except` 包住——stanmetacols 或 LLM 出问题只记 warning,**绝不阻断标准化写盘**。

### 细胞级过滤(`initially_filtered_h5ad` step,自动接在 `standardized_h5ad` 之后)

`initially_filtered_h5ad` 是一个**下游 asset**(`deps=["standardized_h5ad"]`),和 `standardized_h5ad` 同属一个 job——sensor 发现新样本时,一个 run 里会**先跑 `standardized_h5ad`、再自动跑 `initially_filtered_h5ad`**,无需手动触发。

- **读**:`standardized_h5ad` 写到 `SC_CURATION_OUTPUT_DIR` 的标准化 `.h5ad`(用 `layers["counts"]` 算每细胞检出基因数)。
- **过滤**:剔除检出基因数 `< SC_CURATION_MIN_GENES_PER_CELL`(默认 400)的细胞;`X` 与所有 layers 一起按行子集。
- **写**:过滤后的对象写成**单独**的 `*_filtered.h5ad`(同目录、加后缀)——`standardized_h5ad` 的全细胞文件**原样保留**,过滤是非破坏性的。
- **硬性快速失败**:过滤后剩余细胞数 `< SC_CURATION_MIN_CELLS` → `dagster.Failure`(红 run)、**不写输出**。
- **metadata**:`filtered_output_path`、`source_standardized`、`min_genes_per_cell`、`n_cells_before` / `n_cells_after` / `n_cells_removed`、`adata_info`(`print(adata)` 面板)。

### Doublet 评分(`doublet_scored_h5ad` step,自动接在 `initially_filtered_h5ad` 之后)

`doublet_scored_h5ad`(`deps=["initially_filtered_h5ad"]`),同 job 内自动接力。

- **读**:`*_filtered.h5ad`,在 `layers["counts"]`(原始 counts)上跑 [Scrublet](https://github.com/swolock/scrublet)(`scanpy.pp.scrublet`,`random_state=0`)。
- **按 sample 分批**:Scrublet 靠"随机配对模拟 doublet",必须**在样本内部**做(跨样本混合会造出假 doublet)。识别到的 `sample` 列(来自 metacols)存在时**逐 sample 跑**;否则整份数据当一组。
- **非致命**:某个 sample 太小/退化导致 Scrublet 报错 → 该 sample 的 `doublet_score=NaN`、`predicted_doublet=False` + warning,不影响其它 sample。
- **写**:把 `obs["doublet_score"]`(float)/ `obs["predicted_doublet"]`(bool)写回、**覆盖**同一个 `*_filtered.h5ad`(就地加列)。
- **metadata**:`batch_key`(`sample` 或 `—`)、`n_cells` / `n_scored` / `n_predicted_doublets` / `doublet_rate` / `n_failed_samples`、`adata_info`。

### MrVI + Leiden 聚类(`mrvi_leiden_h5ad` step,终端,Pipes → Slurm GPU)

`mrvi_leiden_h5ad`(`deps=["doublet_scored_h5ad"]`)是**终端 asset**。MrVI 是 GPU 训练的多样本深度生成模型,所以**不在 Dagster 进程里跑**——通过 **Dagster Pipes** 提交一个 **Slurm GPU 作业**(`scripts/mrvi_leiden_job.py`),orchestration 端只在 CPU 上轮询 `sacct` 等它完成。

- **读**:`*_filtered.h5ad`(`layers["counts"]` 作输入)。
- **MrVI**:`MRVI.setup_anndata(layer="counts", sample_key="sample", backend="torch")` → 训练 → `get_latent_representation(give_z=False)` 取 **u latent** → `obsm["X_mrvi_u"]`。没 `sample` 列时建个常量列退化成单样本。
- **Leiden**:`sc.pp.neighbors(use_rep="X_mrvi_u")` + `sc.tl.leiden(flavor="igraph")` → `obs["mrvi_leiden"]`。
- **写**:`obsm["X_mrvi_u"]` + `obs["mrvi_leiden"]` **就地写回** `*_filtered.h5ad`(全部细胞,doublet 不剔)。
- **失败**:Slurm 作业非 `COMPLETED` → `dagster.Failure`(可重试)。
- **sbatch 资源(env 可配,`-G 1` 固定)**:

  | env | 默认 | 说明 |
  |---|---|---|
  | `SC_CURATION_MRVI_PARTITION` | `gpu` | sbatch `-p`;小数据可用 `dev`(排队快) |
  | `SC_CURATION_MRVI_TIME` | `01:00:00` | `--time`(短→调度快;大数据加长) |
  | `SC_CURATION_MRVI_CPUS` | `4` | `--cpus-per-task` |
  | `SC_CURATION_MRVI_MEM` | `32GB` | `--mem` |
  | `SC_CURATION_MRVI_GPU_CONSTRAINT` | 空 | 可选 `-C`(如 `GPU_MEM:24GB`);空=不限 |
  | `SC_CURATION_MRVI_MAX_EPOCHS` | 空 | MrVI 训练轮数(空=scvi 默认) |
  | `SC_CURATION_LEIDEN_RESOLUTION` | `1.0` | Leiden 分辨率 |

- **metadata**(由外部作业经 Pipes 回传):`n_cells` / `n_samples` / `n_clusters` / `latent_dim` / `leiden_resolution` / `accelerator`(gpu/cpu)/ `had_sample_column`。

---

## 7. 重新处理某个样本

样本的"完成"以**终端 asset `mrvi_leiden_h5ad` 对该分区物化**为准:sensor 据此去重,未物化(改名、新增下游步骤、或某步失败)会被重新触发,已物化则写一次不再自动重跑。需要重算某一步时,在 UI 选中该分区点 **Materialize** 即可(各下游步骤都读磁盘上的 `*_filtered.h5ad`,可单独重跑、不必重跑上游)。

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
    ├── qc.py                 # compute_count_qc 等纯函数 + standardized_h5ad asset + job(选标准化/过滤/doublet 三个 asset)
    ├── standardize.py        # build_standardized_adata / write_standardized
    ├── harmonize_apply.py    # apply_harmonization(把 stangene 结果写回 var_names)
    ├── metacols.py           # stanmetacols 元数据列识别 + 规范化 + markdown 渲染
    ├── filter_cells.py       # filter_cells_by_genes / filtered_path_for 纯函数
    ├── filtering.py          # initially_filtered_h5ad 下游 asset(deps=["standardized_h5ad"])
    ├── doublets.py           # doublet_scored_h5ad asset(Scrublet,按 sample 分批)
    ├── mrvi_compute.py       # train_mrvi_u_latent / leiden_on_rep 纯函数(外部作业 + 测试共用)
    ├── slurm_pipes.py        # PipesSlurmClient:文件型 Pipes 通道 + sbatch + sacct 轮询
    ├── mrvi.py               # mrvi_leiden_h5ad 终端 asset(Pipes → Slurm GPU 作业)
    ├── sensors.py            # discover_samples 扫描器 + watch_h5ad_dir sensor(去重看终端 asset 物化)
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

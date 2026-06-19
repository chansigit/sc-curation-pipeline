# sc-curation-pipeline

用 **Dagster** 监控一个目录、对新上传的单细胞 `.h5ad` 文件自动做轻量 **QC**,结果以 **Dagster asset metadata + asset checks** 的形式呈现在 Web UI 里——**不额外落地任何文件**,也不改动源数据。

> 约定:**一个文件夹 = 一个样本 = 一个 `.h5ad`**。上传完成后,在该文件夹里放一个空的 `.done` 标记文件来触发处理。

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
                                              ├─ 用 scanpy 算 QC
                                              ├─ 写进物化 metadata
                                              └─ 跑 3 个阈值 asset check
                                                   │
                                                   ▼
                                            Dagster UI: 每样本一格 + QC metadata + 绿/红检查
```

- **打不开 / 损坏 / 缺文件** → 该分区的 run **变红**(`dagster.Failure`),失败原因在 metadata 里。
- **QC 不达标**(细胞太少 / mito% 过高 / 不是原始 counts)→ 对应 **check 变红,但 run 仍为绿**(方便你照常看指标做分诊)。

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
| `SC_CURATION_DONE_MARKER` | `.done` | 上传完成标记文件名 |
| `SC_CURATION_H5AD_GLOB` | `*.h5ad` | 文件夹内匹配 h5ad 的模式 |
| `SC_CURATION_SCAN_INTERVAL_SEC` | `30` | sensor 最小扫描间隔(秒;在 `dg dev` 启动时读取) |
| `SC_CURATION_MIN_CELLS` | `100` | check:细胞数下限 |
| `SC_CURATION_MAX_MITO_PCT` | `20` | check:中位 mito% 上限 |

`SC_CURATION_WATCH_DIR` 是必填的——没设会立刻报错(注册资源时就要读它)。可选变量留空或写成非法值会**安全退回默认值**(不会让服务崩溃)。

### 怎么设置这些变量

三种方式,**推荐第 ① 种**:

**① `.env` 文件(推荐 —— `dg` 启动时自动加载)**
在项目根目录放一个 `.env`,`dg dev` / `dg check defs` 会自动把它加载进环境,**不用每次 `export`**。仓库带了模板 `.env.example`:
```bash
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
cp .env.example .env        # 然后编辑 .env,至少填好 SC_CURATION_WATCH_DIR
```
`.env` 已在 `.gitignore` 里、不会被提交。内容示例:
```dotenv
SC_CURATION_WATCH_DIR=/scratch/users/chensj16/sc-curation-watch
SC_CURATION_MIN_CELLS=200
SC_CURATION_MAX_MITO_PCT=15
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
│   └── .done          ← 上传完成后"再"放这个,sensor 才会处理
├── GSE123_sampleB/
│   └── matrix.h5ad    ← 没有 .done → 暂不处理(视为还在上传)
└── proj/
    └── pbmc/
        ├── pbmc.h5ad  ← 支持任意层级嵌套
        └── .done
```

**关键:先把 h5ad 传完,再放 `.done`。** 这样 sensor 永远不会去碰一个还没写完的文件。

---

## 4. 启动 Dagster

从项目目录,用 `dl2025` 的 `dg` 启动:

```bash
export SC_CURATION_WATCH_DIR=/scratch/users/chensj16/<你的watch目录>
cd /scratch/users/chensj16/projects/eca-dagster-pipeline/sc-curation-pipeline
/scratch/users/chensj16/venvs/dl2025/.venv/bin/dg dev -p 27182
```

> 如果你已经建好 `.env`(见上面「怎么设置这些变量」),上面那行 `export` 就能省掉——`dg dev` 会自动加载 `.env`。

从本地电脑做端口转发看 UI(`dg dev` 跑在计算节点的 27182 端口):

```bash
ssh -L 27182:<计算节点名, 如 sh02-06n11>:27182 <你的SUNet>@login.sherlock.stanford.edu
# 然后浏览器打开 http://localhost:27182
```

**(或)用 ngrok 暴露到固定域名** —— 不想每次 SSH 转发的话,在**同一个计算节点**上另开一个终端跑(`dg dev` 继续开着):
```bash
# authtoken 一次性配置(~/.config/ngrok/ngrok.yml 里已有则跳过):
# ngrok config add-authtoken <你的token>

# 给 UI 加一层认证(强烈建议!),再暴露到你的固定域名:
ngrok http 27182 --domain=csj.ngrok.io --basic-auth "你:一个强密码"
```
然后任意地方浏览器打开 https://csj.ngrok.io 。

或用仓库里的脚本 `scripts/serve-ui.sh` —— **一键把 `dg dev`(后台)+ ngrok 隧道一起拉起/关掉**,`dg dev` 不再占着终端(配置走 `SC_UI_PORT` / `SC_UI_NGROK_DOMAIN` / `SC_UI_BASIC_AUTH`;run 历史/已登记样本存到项目内 gitignored 的 `.dagster_home/`,重启不丢):
```bash
SC_UI_BASIC_AUTH="csj:一个强密码" scripts/serve-ui.sh up   # 启动(后台)
scripts/serve-ui.sh status                                # 看状态 + 公网 URL
scripts/serve-ui.sh down                                  # 断开
```

> ⚠️ 注意:
> - **Dagster UI 默认没有登录认证**,而它能触发/取消 run(等于在集群上跑代码)。公开到公网前**务必加 `--basic-auth`(或 `--oauth`)**,否则拿到 URL 的人就能操作你的 pipeline;用完 `Ctrl-C` 关掉隧道。
> - 从共享 HPC 对公网暴露服务,请确认符合 Stanford SRC 使用规范。
> - 小坑:ngrok 域名 DNS 会先给 IPv6(本节点 IPv6 不通),ngrok 自动回退 IPv4(已验证可连);隧道起得慢等几秒即可。

**打开 sensor**:UI 里 **Automation → `watch_h5ad_dir`**,开关拨到 **ON**(它默认是 `STOPPED`,不打开不会扫描)。

---

## 5. 端到端走一遍

```bash
# 造一个样本(注意顺序:先 h5ad,后 .done)
mkdir -p "$SC_CURATION_WATCH_DIR/demo_sample"
cp /path/to/your.h5ad "$SC_CURATION_WATCH_DIR/demo_sample/demo.h5ad"
touch "$SC_CURATION_WATCH_DIR/demo_sample/.done"
```

- 一个 tick(≤30s)内,sensor 注册分区 `demo_sample` 并触发一次 `h5ad_qc` run。
- 在 UI **Assets → `h5ad_qc` → 选 `demo_sample` 分区**,可以看到:
  - **Metadata**:`n_cells` / `n_genes` / `mito_pct` / `ribo_pct` / `total_counts` / `sparsity` / `is_raw_counts` / `obs_columns` / 文件大小、路径 等。
  - **Checks**:`min_cells` / `max_mito_pct` / `is_raw_counts` 的绿 / 红。
- 试验失败路径:放一个损坏的 h5ad(+ `.done`)→ 那个分区的 run 变红,原因写在 metadata。

---

## 6. QC 指标 & 检查

- **结构**:`n_cells`、`n_genes`、`X_dtype`、`is_sparse` / `density` / `sparsity`、`has_raw`、`layers` / `obsm` / `obsp`、`obs_columns` / `var_columns`。
- **计数**:`total_counts`、每细胞中位 `counts` / `genes`、`mito_pct`(`MT-` 基因)、`ribo_pct`(`RPS` / `RPL`)。
- **判定**:`is_raw_counts`(X 是否近似整数 → 原始 counts vs 已归一化)。
- **文件**:大小、mtime、路径。
- **asset checks(阈值可配)**:`min_cells`、`max_mito_pct`、`is_raw_counts`。

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
    ├── qc.py                 # compute_qc 纯函数 + h5ad_qc asset + job
    ├── sensors.py            # discover_samples 扫描器 + watch_h5ad_dir sensor
    └── registration.py       # 把 asset / job / sensor / resource 打包成 Definitions
tests/                        # pytest(test_settings / test_qc / test_sensor)
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

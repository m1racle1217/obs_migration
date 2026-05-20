# ☁️ OBS / S3 兼容对象存储迁移工具

面向 TB / PB 级数据、海量小文件、断点续传和离线部署场景的迁移工具。支持：

- `local -> s3`
- `s3 -> local`
- `s3 -> s3`

项目优先优化华为 OBS 场景，也兼容部分 S3 协议对象存储。文档中的 `s3` 表示 S3 协议兼容对象存储，不等于只能连接 AWS S3。

> 当前远端实现基于华为 OBS Python SDK。其他 S3 兼容服务是否可用，取决于服务端对 `HEAD`、`copyObject`、`copyPart`、分片上传等接口的兼容程度。

## ✅ 核心能力

| 能力 | 说明 |
| --- | --- |
| 🔁 多端迁移 | 支持本地和对象存储之间双向迁移，也支持对象存储互转 |
| 📋 源端选择模式 | 支持目录模式和列表模式；列表模式可混合添加多个目录、文件、S3 前缀或对象 |
| 🧩 三段流水线 | `扫描 -> 检查 -> 传输` 解耦，检查和传输并发可单独控制 |
| 🚀 高吞吐传输 | 多线程传输、分片上传、同 endpoint 优先服务端拷贝 |
| 🧠 智能跳过 | 支持 checkpoint、目标端索引、HEAD、ETAG 组合判断 |
| ✅ 传后校验 | 支持 `none / size / etag / head` |
| 🌈 实时仪表盘 | 展示进度、速度、队列、线程状态、扫描状态、错误数，并使用 256 色区分状态 |
| 🔤 路径编码修复 | 扫描时保留原始路径字节，上传对象 key 前做 UTF-8 安全转换，降低历史乱码路径导致的失败率 |
| 💾 断点续传 | SQLite checkpoint 保存任务状态 |
| 📊 审计报告 | 输出成功、跳过、失败、中断、未完成任务明细 |
| 📦 离线部署 | 支持构建 Windows / Linux 离线发布目录 |
| 🔐 配置安全 | `ak/sk` 支持 Fernet 加密；真实配置可放仓库外 |

## 🧱 架构概览

```text
源端扫描 Scanner / S3 Scanner
    ↓
检查队列 check_queue
    ↓
检查器 TaskChecker
    - checkpoint
    - 目标端索引
    - HEAD / ETAG
    ↓
传输队列 transfer_queue
    ↓
传输器 TaskTransfer
    - 本地上传
    - 远端下载
    - S3 -> S3 服务端拷贝
    - 分片上传 / 分片拷贝
    ↓
Checkpoint / Report / Dashboard
```

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

离线环境请参考 [OFFLINE_DEPLOY.md](OFFLINE_DEPLOY.md)。

### 2. 准备配置

首次运行会自动创建配置，也可以复制示例配置：

```powershell
copy config.example.ini config.ini
python obs_migrate.py
```

真实配置不建议提交到仓库，可以放到仓库外，并用环境变量指定：

```powershell
$env:OBS_MIGRATE_CONFIG = 'D:\secure\obs_sync_check\config.ini'
python obs_migrate.py
```

### 3. 启动迁移

```bash
python obs_migrate.py
```

启动后默认进入交互配置菜单。确认配置后按 `Y` 启动迁移。

## 🧭 交互菜单

### 主菜单热键

| 按键 | 作用 |
| --- | --- |
| `S` | 浏览源端目录 / 桶 / 前缀 |
| `T` | 浏览目标端目录 / 桶 / 前缀 |
| `Y` | 按当前配置启动迁移 |
| `Q` | 退出程序 |

热键语义保持一致：

- `Q`：只用于退出程序。
- `B`：返回上一层或取消当前子操作。

### 源端选择模式

源端通过 `SOURCE.selection_mode` 控制选择方式，`local` 和 `s3` 都支持：

| 模式 | 说明 |
| --- | --- |
| `directory` | 目录模式，一对一迁移；本地使用 `SOURCE.path`，S3 使用 `SOURCE.bucket/prefix` |
| `list` | 列表模式，多条目迁移；使用 `[PATH] migration_list_file` 指定的清单文件 |

推荐配置顺序：

```ini
[SOURCE]
type = local
selection_mode = list

[PATH]
migration_list_file = ./migration_list.txt
```

列表模式说明：

- `migration_list_file` 默认是 `./migration_list.txt`，相对 `config.ini` 所在目录解析。
- 列表文件不存在时，第一次通过 UI 添加列表项会自动创建；父目录不存在时也会自动创建。
- 本地源端时，列表文件每行一个本地文件或目录。
- S3 源端时，列表文件每行一个桶内前缀或对象 key，不需要写 `bucket://`。
- 目录 / 前缀会递归扫描，文件 / 对象会作为单项加入迁移。
- 重复路径会自动去重。

### 浏览器热键

#### 普通目录模式

| 按键 | 作用 |
| --- | --- |
| 数字 | 进入对应目录 / 桶 / 前缀 |
| `S` | 保存当前位置到配置 |
| `K` | 筛选当前列表 |
| `B` | 返回上一层；根层时返回配置菜单 |
| `N` | 下一页 |
| `P` | 上一页 |
| `R` | 远端浏览刷新当前页和统计 |

#### 列表模式

| 按键 | 作用 |
| --- | --- |
| 数字 | 进入对应目录 / 桶 / 前缀；文件不会被误添加 |
| `F` | 添加指定项至迁移列表，然后输入序号 |
| `A` | 添加当前目录 / 当前前缀至迁移列表 |
| `K` | 筛选当前列表 |
| `B` | 返回上一层；根层时返回配置菜单 |
| `N` | 下一页 |
| `P` | 上一页 |
| `R` | 远端浏览刷新当前页和统计 |

`F` 添加指定项时支持多选：

```text
1 3 5
1,3,5
2-8
```

`K` 筛选支持多个关键字，空格分隔，全部命中才显示；输入空值会清除筛选。

### 源端列表管理

当源端处于 `selection_mode = list` 时，源端配置界面会显示“源端列表模式清单文件与列表管理”。选择该项会直接打印当前列表并进入管理面板。

目录模式下不显示列表管理项，避免误操作。

| 按键 | 作用 |
| --- | --- |
| `A` | 手工添加路径 / 前缀 / 对象 |
| `D` | 删除列表项，会提示输入要删除的编号 |
| `C` | 清空列表 |
| `B` | 返回上一层 |

`migration_list.txt` 示例：

```text
E:\data\project-a
E:\data\project-b\report.xlsx
```

S3 列表文件示例：

```text
logs/2026/04/
images/logo.png
```

## ⚙️ 配置示例

### local -> s3，目录模式

```ini
[SOURCE]
type = local
selection_mode = directory
path = E:\data\project-a
ak =
sk =
endpoint =
bucket =
prefix =

[TARGET]
type = s3
path =
ak = your-ak
sk = your-sk
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = target-bucket
prefix = backup/project-a/
```

### local -> s3，列表模式

```ini
[SOURCE]
type = local
selection_mode = list
path =
ak =
sk =
endpoint =
bucket =
prefix =

[TARGET]
type = s3
path =
ak = your-ak
sk = your-sk
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = target-bucket
prefix = backup/list/

[PATH]
migration_list_file = ./migration_list.txt
```

对应 `migration_list.txt`：

```text
E:\data\project-a
E:\data\project-b\report.xlsx
```

### s3 -> s3，列表模式

```ini
[SOURCE]
type = s3
selection_mode = list
path =
ak = source-ak
sk = source-sk
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = source-bucket
prefix =

[TARGET]
type = s3
path =
ak = target-ak
sk = target-sk
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = target-bucket
prefix = migrated/

[PATH]
migration_list_file = ./migration_list.txt
```

对应 `migration_list.txt`：

```text
logs/2026/04/
images/logo.png
```

### 完整运行参数

```ini
[UPLOAD]
workers = 64
checkers = 32
part_size = 128M
multipart_threshold = 300M
retry = 3
rate_limit = 10000
rate_limit_burst = 10000
low_level_retries = 5
low_level_retry_sleep = 0.5
max_connections = 256
multipart_concurrency = 4
max_buffer_memory = 512M
request_timeout = 60
worker_stall_timeout = 300

[SCAN]
scan_workers = 128
batch_size = 5000
queue_size = 20000

[CHECK]
enable_etag_check = false
enable_head_check = true
strict_client_check = true
target_compare_mode = auto
verify_after_upload = head

[PATH]
log_dir = ./logs
state_dir = ./state
failed_dir = ./failed
migration_list_file = ./migration_list.txt

[UI]
prompt_config = true
show_dashboard = true
language = zh
```

## 🔧 关键参数说明

### `[SOURCE]`

| 参数 | 说明 |
| --- | --- |
| `type` | 源端类型，支持 `local` / `s3` |
| `selection_mode` | 源端选择模式，支持 `directory` / `list` |
| `path` | 目录模式使用；本地源端可填目录、单文件、通配符 |
| `ak/sk/endpoint/bucket/prefix` | 源端为 S3 时使用 |

通配符只用于本地目录模式的 `SOURCE.path`：

```ini
[SOURCE]
type = local
selection_mode = directory
path = /data/attachments/202604*
```

如果本地 `SOURCE.path` 使用通配符，目标端会保留第一个通配符之前的静态目录层级作为相对根目录。

列表模式的 `migration_list.txt` 建议写明确的文件或目录路径。当前列表扫描会按条目本身处理，不把列表文件里的 `*` 当成批量通配符入口。

### 路径编码与对象 key

本地扫描会尽量保留操作系统返回的原始路径字节，传输前再做安全的 UTF-8 路径转换。遇到历史数据里常见的 GBK / GB18030 路径名、混合编码或 surrogate 转义时，程序会尽量恢复成可上传的对象 key，避免因为文件名乱码导致任务直接失败。

需要注意：这里处理的是“文件名 / 路径 / 对象 key”的编码，不会修改文件内容本身。

### `[TARGET]`

| 参数 | 说明 |
| --- | --- |
| `type` | 目标端类型，支持 `local` / `s3` |
| `path` | 目标端为本地时使用 |
| `ak/sk/endpoint/bucket/prefix` | 目标端为 S3 时使用 |
| `prefix` | 可为空；程序会自动拼接和清洗 |

### `[UPLOAD]`

| 参数 | 说明 |
| --- | --- |
| `workers` | 传输线程数 |
| `checkers` | 检查线程数 |
| `part_size` | 分片大小 |
| `multipart_threshold` | 超过该大小启用分片传输 |
| `retry` | 任务级失败重试次数 |
| `rate_limit` | 目标端 API 基础 QPS 限制，`0` 表示不限制 |
| `rate_limit_burst` | QPS 突发上限 |
| `low_level_retries` | 底层请求重试次数 |
| `low_level_retry_sleep` | 底层请求重试基础等待秒数 |
| `max_connections` | 最大连接数，`0` 表示不限制 |
| `multipart_concurrency` | 单个大文件的分片并发数 |
| `max_buffer_memory` | 流式缓冲总预算，`0` 表示不限制 |
| `request_timeout` | 单次请求超时秒数 |
| `worker_stall_timeout` | worker 心跳超时阈值 |

### `[SCAN]`

| 参数 | 说明 |
| --- | --- |
| `scan_workers` | 扫描线程上限，本地和远端扫描都生效 |
| `batch_size` | 单批扫描入队数量 |
| `queue_size` | 检查队列和传输队列最大长度 |

扫描并发会根据 CPU 和队列压力自动调整。如果配置过高，启动时会提示自动降级。

### `[CHECK]`

| 参数 | 说明 |
| --- | --- |
| `enable_etag_check` | 上传前是否启用 ETAG 比对 |
| `enable_head_check` | 上传前是否启用 HEAD 校验 |
| `strict_client_check` | 客户端未初始化时是否直接报错 |
| `target_compare_mode` | 目标端比较策略，支持 `auto / hybrid / index_only / head_only` |
| `verify_after_upload` | 传输后校验策略，支持 `none / size / etag / head` |

### `[PATH]`

| 参数 | 说明 |
| --- | --- |
| `log_dir` | 日志目录 |
| `state_dir` | 断点数据库目录，`tasks.db` 会写到这里 |
| `failed_dir` | 失败任务目录 |
| `migration_list_file` | 源端列表模式清单文件，默认 `./migration_list.txt`；第一次写入会自动创建 |

`logs`、`state`、`failed`、`check_report`、`migration_list_file` 等路径都会相对配置文件所在目录解析，而不是相对当前终端目录。

### `[UI]`

| 参数 | 说明 |
| --- | --- |
| `prompt_config` | 启动时是否进入交互配置菜单 |
| `show_dashboard` | 是否显示实时仪表盘 |
| `language` | 仪表盘语言，`zh` 为中文 / English 双语，`en` 为英文 |

## 🧪 比较策略与校验

### `target_compare_mode`

| 模式 | 说明 |
| --- | --- |
| `auto` | 默认模式，根据场景自动选择 |
| `hybrid` | 目标索引命中时再补 HEAD，兼顾性能与确认 |
| `index_only` | 只依赖目标端索引判断，适合重跑和减少请求 |
| `head_only` | 每个对象尽量走 HEAD，确认直接但请求量更大 |

### `verify_after_upload`

| 模式 | 说明 |
| --- | --- |
| `none` | 不做传后校验 |
| `size` | 传后校验大小 |
| `etag` | 传后校验大小和 ETAG |
| `head` | 默认推荐；传后 HEAD 校验大小，ETAG 可比较时也会校验 |

`enable_head_check` / `enable_etag_check` 是传输前跳过判断；`verify_after_upload` 是传输完成后的后验校验。

## 🌈 实时仪表盘

仪表盘展示：

- 总进度、百分比、已处理大小、ETA
- 累计处理速度、实时上传速度
- 索引、扫描、检查、上传状态
- 扫描文件数、扫描速度、扫描错误
- 上传错误、跳过数量
- 检查队列、传输队列水位
- 扫描 / 检查 / 上传线程状态

颜色含义：

- 绿色：完成、速度、正常值
- 黄色：运行中、队列较忙、关键数值
- 红色：错误、队列接近满
- 紫色：等待、排队、脉冲进度
- 青色 / 蓝色：路径、模式、标题和边框

关闭仪表盘：

```powershell
$env:OBS_MIGRATE_DASHBOARD = '0'
python obs_migrate.py
```

## 📊 输出目录

| 目录 | 说明 |
| --- | --- |
| `logs/` | 运行日志 |
| `state/tasks.db` | checkpoint 数据库 |
| `failed/` | 失败任务明细 |
| `check_report/` | CSV 明细和 JSON 汇总 |

报告中会保留源端路径、目标路径、大小、状态和错误信息。任务失败、中断或提前停止时，已经扫描到但尚未迁移完成的对象也会写入报告，状态为未完成或中断。

## 🔐 配置安全

- `SOURCE.ak/sk`、`TARGET.ak/sk` 会加密写入配置文件。
- 加密密钥默认保存在配置文件同目录的 `.config.key`。
- `.config.key` 不应提交到仓库。
- 建议用 `OBS_MIGRATE_CONFIG` 指向仓库外的真实配置。

## 🧰 环境变量

| 环境变量 | 作用 |
| --- | --- |
| `OBS_MIGRATE_CONFIG` | 指定配置文件路径 |
| `OBS_MIGRATE_VENDOR` | 指定离线依赖目录 |
| `OBS_MIGRATE_INTERACTIVE` | 控制是否启用交互配置 |
| `OBS_MIGRATE_DASHBOARD` | 控制是否显示实时仪表盘 |
| `OBS_MIGRATE_FORCE_TERMINAL` | 强制以终端模式渲染 Rich UI |

## 📦 离线部署

构建离线发布目录：

```bash
python build_offline_bundle.py
```

指定平台：

```bash
python build_offline_bundle.py --platform windows --python-tag py39
python build_offline_bundle.py --platform linux --python-tag py39 --strict-centos7
```

脚本会生成 `dist/obs_sync_check_<platform>_<machine>_<python-tag>/`，复制源码、整理 `vendor/` 依赖，并生成启动脚本。

## 🗂️ 目录结构

```text
obs_migrate.py              # 命令行入口、交互菜单、配置迁移和任务编排
config.example.ini          # 可提交的配置模板
requirements.txt            # 在线安装依赖清单
bootstrap_runtime.py         # 离线运行时 vendor 加载入口
build_offline_bundle.py      # 离线发布目录构建脚本
OFFLINE_DEPLOY.md            # 离线部署说明
README.txt                   # 兼容保留的纯文本说明
core/
  __init__.py                # core 包导出入口
  build_db.py                # 历史版本上传状态库初始化
  capabilities.py            # 源端 / 目标端能力探测
  checkpoint.py              # SQLite checkpoint 和任务状态
  dashboard.py               # 实时仪表盘
  governor.py                # 连接、限流、缓冲预算治理
  object_browser.py          # 本地 / 远端浏览器
  obs_index.py               # 目标端对象索引构建与缓存
  progress.py                # 运行期进度与速度指标
  ratelimiter.py             # 令牌桶 API 限流
  report.py                  # CSV / JSON 报告输出
  retry.py                   # 底层请求重试策略
  s3_scanner.py              # 远端对象扫描
  scanner.py                 # 本地文件扫描
  scan_control.py            # 自适应扫描并发控制
  scheduler.py               # worker 调度、心跳和卡死检测
  uploader.py                # 传输、检查、目标端初始化
  utils.py                   # 路径、大小、Endpoint、编码和哈希工具
tools/
  prepare_vendor.py          # 从 lib/ 整理离线 vendor 依赖
lib/                         # 离线依赖归档目录
logs/                        # 默认日志目录
state/                       # 默认 checkpoint 状态目录
failed/                      # 默认失败任务目录
check_report/                # 默认审计报告目录
tests/                       # 测试用例
```

## 💡 使用建议

- 海量小文件场景优先使用较高 `checkers`，让检查阶段不要成为瓶颈。
- 如果目标端前缀稳定、常常重跑，可考虑 `target_compare_mode = index_only`。
- 如果需要更谨慎的传后确认，保持 `verify_after_upload = head`。
- 只迁移一个本地目录、单文件或通配符匹配结果时，使用 `selection_mode = directory` + `SOURCE.path`。
- 需要挑选多个目录、文件、S3 前缀或对象时，使用 `selection_mode = list` + `PATH.migration_list_file`。
- 对象很多的 S3 前缀可以先用 `K` 筛选，再用 `F` 批量添加指定对象。

## Web 控制台

可以用配置或命令行开关启动本地 Web 控制台。控制台是无需前端构建工具的静态 Operations Shell，包含“配置”“目录浏览”“任务仪表盘”“日志/报告”四个区域。

```ini
[WEB_UI]
enabled = false
host = 127.0.0.1
port = 8765
require_login = true
username = admin
password = admin
auto_open = false
```

启动方式：

```powershell
python obs_migrate.py --web
```

- `enabled`：设为 `true` 时，普通 `python obs_migrate.py` 也会启动 Web 控制台。
- `--web`：临时启用 Web 控制台，不需要修改 `config.ini`。
- `host` / `port`：监听地址和端口；端口被占用时启动错误会包含当前 host/port。
- `require_login` / `username` / `password`：控制 API 登录；非本机监听建议保持登录开启。
- `auto_open`：设为 `true` 时启动后自动打开浏览器。
- Web 控制台启动后仍会在前台执行原 CLI 迁移流程；结束或中断时会关闭 Web 服务。

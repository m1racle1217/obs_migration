
# ☁️ OBS / S3 兼容对象存储迁移工具

> 一个面向 **TB 级数据**、**海量小文件**、**断点续传**、**离线环境部署** 的迁移工具。  
> 一个面向 **TB / PB 级数据**、**海量小文件**、**断点续传**、**离线环境部署** 的对象迁移工具。  
> 支持 `local -> s3`、`s3 -> local`、`s3 -> s3`，优先优化 **OBS** 场景，也兼顾其他 **S3 协议兼容对象存储**。

## ✨ 亮点速览

- 🧵 **扫描与上传解耦**：扫描线程、上传线程、任务队列独立协作
- 🚀 **高吞吐传输**：多线程、分片上传、同 endpoint 优先服务端拷贝
- 🧠 **智能跳过判定**：索引 + HEAD + checkpoint 多层组合
- 🌈 **实时仪表盘**：进度条、速率、队列、活跃扫描线程一屏可见
- 🪄 **自适应扫描并发**：根据队列压力动态调节有效 `scan_workers`
- 📄 **报告更完整**：成功、跳过、失败、未完成、中断任务都会进入报告
- 🔐 **配置更安全**：支持 Fernet 加密 `ak/sk`，真实配置可放仓库外
- 📦 **适合离线部署**：支持构建 Windows / Linux 离线发布目录

> [!NOTE]
> 文档中的 `s3` 表示 **S3 协议兼容对象存储**。  
> 当前远端实现基于 **华为 OBS Python SDK**，因此 **OBS 支持最完整**。  
> 对于“其他 S3 -> OBS”“OBS -> 其他 S3”“其他 S3 -> 其他 S3”这类场景，只要服务端对 `HEAD`、`copyObject`、`copyPart` 等接口兼容度足够，通常也可以工作；最终效果仍取决于目标服务的兼容程度。
- 🧭 **多模式迁移**：支持本地与对象存储之间双向迁移，也支持对象存储互转
- 🧩 **三段流水线**：`扫描 -> 检查 -> 传输` 解耦，吞吐和稳定性更好
- 🚀 **高吞吐传输**：并发传输、分片上传、服务端拷贝、单文件分片并发都已支持
- 🧠 **智能存在性判断**：支持 `auto / hybrid / index_only / head_only` 多种比较策略
- 🛡️ **传后校验**：支持 `none / size / etag / head`
- 🌈 **实时仪表盘**：进度条、百分比、速率、ETA、检查阶段、卡死探测一屏可见
- 📄 **报告完整**：成功、跳过、失败、中断、未完成任务都会落到报告
- 🔐 **配置更安全**：`ak/sk` 支持 Fernet 加密，真实配置可放仓库外
- 📦 **离线可部署**：支持 Windows / Linux 离线发包，适合内网和 CentOS 7.9

---

| `s3 -> local` | 对象存储下载到本地 | 恢复、回迁、本地审计 |
| `s3 -> s3` | 对象存储之间迁移 | OBS ↔ OBS、其他 S3 ↔ OBS |

---

## 🧩 核心能力
> [!NOTE]
> 本项目里的 `s3` 表示 **S3 协议兼容对象存储**，不等于只能连 AWS S3。  
> 当前远端实现基于 **华为 OBS Python SDK**，因此 **OBS 支持最完整**。  
> 对于“其他 S3 -> OBS”“OBS -> 其他 S3”“其他 S3 -> 其他 S3”，只要目标服务对 `HEAD`、`copyObject`、`copyPart` 等接口兼容度足够，通常也可以工作。

| 能力 | 说明 |
| --- | --- |
| 🔍 多线程扫描 | 支持本地目录扫描与远端对象扫描 |
| 📤 多线程传输 | 支持高并发上传 / 复制 |
| 🧱 分片上传 | 大文件自动走 multipart |
| 🪞 服务端拷贝 | 同 endpoint 场景优先尝试 `copyObject` / `copyPart` |
| 💾 断点续传 | SQLite checkpoint + SDK 级续传能力 |
| ⚡ 索引跳过 | 目标端索引命中时快速跳过已存在对象 |
| 🧪 HEAD / ETAG 校验 | 支持更严格的存在性与一致性检查 |
| 🎨 实时仪表盘 | 展示进度、速度、队列、扫描状态、缓存命中率 |
| 📝 审计报告 | 输出 CSV 明细和 JSON 汇总 |
| 🧰 离线发布 | 自动整理 `vendor/` 依赖并生成发布目录 |
> [!TIP]
> 日志和仪表盘里的路径展示会根据 endpoint 自动识别协议：  
> `obs.cn-*.myhuaweicloud.com` 会优先显示为 `obs://bucket/key`，其他兼容服务默认显示为 `s3://bucket/key`。  
> 这只是展示层优化，配置里的 `type` 仍统一使用 `s3`。

---

## 🏗️ 架构概览

```text
Scanner / Remote Scanner
扫描器（本地 / 远端）
   ↓
检查队列（check_queue）
   ↓
Queue（背压缓冲）
检查器（索引 / HEAD / checkpoint）
   ↓
Scheduler（线程调度）
传输队列（transfer_queue）
   ↓
Decision Engine（索引 / HEAD / checkpoint）
传输器（上传 / 下载 / 服务端拷贝 / 分片）
   ↓
Uploader（上传 / 分片 / 服务端拷贝）
传后校验（size / etag / head）
   ↓
Checkpoint / Report / Dashboard
断点状态 / 报告 / 仪表盘
```

### 🧷 为什么现在是“三段流水”？

- 之前“扫描后直接上传”虽然简单，但检查逻辑、存在性判断、传输重试都挤在一起，稳定性会受影响
- 现在拆成 `扫描 -> 检查 -> 传输` 后：
  - 检查并发可以单独控制
  - 传输并发可以单独控制
  - 队列背压更清晰
  - 心跳与卡死探测更容易做
- 为了避免编码问题，任务里仍会保留原始路径字节信息，**不是只保留清洗后的字符串**

---

## 🚀 快速开始
pip install -r requirements.txt
```

### 2. 启动程序
### 2. 准备配置

推荐先复制模板：

```bash
python obs_migrate.py
copy config.example.ini config.ini
```

首次运行会引导生成配置文件；如果开启交互模式，也可以直接输入配置项编号进行修改。
如果你不希望真实配置出现在仓库里，也可以把真实配置放在仓库外，再通过环境变量指定：

### 3. 非交互运行

```powershell
$env:OBS_MIGRATE_INTERACTIVE = '0'
$env:OBS_MIGRATE_CONFIG = 'D:\secure\obs_sync_check\config.ini'
python obs_migrate.py
```

### 4. 关闭实时仪表盘
### 3. 启动程序

```powershell
$env:OBS_MIGRATE_DASHBOARD = '0'
```bash
python obs_migrate.py
```

### 4. 常用环境变量

| 环境变量 | 作用 |
| --- | --- |
| `OBS_MIGRATE_CONFIG` | 指定配置文件路径 |
| `OBS_MIGRATE_VENDOR` | 指定离线依赖目录 |
| `OBS_MIGRATE_INTERACTIVE` | 控制是否启用交互修改配置 |
| `OBS_MIGRATE_DASHBOARD` | 控制是否显示实时仪表盘 |
| `OBS_MIGRATE_FORCE_TERMINAL` | 强制以终端模式渲染 Rich 界面 |

---

## 📦 离线部署 / 离线发包
## 🧩 交互配置说明

如果目标机器 **没有公网**、**不能 `pip install`**，推荐直接使用离线发布脚本：
程序启动后会先展示当前配置，然后出现下面的提示：

```bash
python build_offline_bundle.py
```text
是否修改配置? (y/N，或直接输入编号):
```

脚本会引导你选择：

- 🪟 `windows`
- 🐧 `linux`（适合 CentOS 7.9 这类离线环境）

然后自动完成：

- 生成 `dist/obs_sync_check_<platform>_<machine>_<python-tag>/`
- 复制运行所需源码
- 从 `lib/` 解包匹配平台的依赖到 `vendor/`
- 生成启动脚本 `run_obs_migrate.bat` / `run_obs_migrate.sh`
- 生成 `bundle_manifest.json`

也可以显式指定参数：
交互规则如下：

```bash
python build_offline_bundle.py --platform windows --python-tag py39
python build_offline_bundle.py --platform linux --python-tag py39 --strict-centos7
```
- 输入 `n` 或直接回车：直接按当前配置开始执行
- 输入 `y`：进入编号修改模式
- 直接输入某个编号：会立刻跳到对应配置项，不会误判成“开始迁移”
- 在编号修改模式里输入 `q`：保存当前修改并退出
- `ak/sk` 这类敏感字段会以**加密后的密文**写回配置文件

> [!TIP]
> 更完整的离线部署说明见 `OFFLINE_DEPLOY.md`。
> `logs`、`state`、`failed`、`check_report` 等目录，都会**相对配置文件所在目录**解析，而不是相对当前终端目录。

---

## ⚙️ 配置结构
## ⚙️ 配置示例

当前配置已经从早期单一 `[OBS] + [TASK]` 结构升级为双端配置：
下面是一份与当前版本能力对齐的完整配置示例：

```ini
[SOURCE]
type = local | s3
type = s3
path =
ak =
sk =
endpoint =
bucket =
prefix =
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = source-bucket
prefix = source/

[TARGET]
type = local | s3
type = s3
path =
ak =
sk =
endpoint =
bucket =
prefix =
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = target-bucket
prefix = target/

[UPLOAD]
workers = 32
part_size = 64M
multipart_threshold = 128M
workers = 64
checkers = 32
part_size = 128M
multipart_threshold = 300M
retry = 3
rate_limit = 200
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
scan_workers = 4
batch_size = 1000
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

[UI]
prompt_config = true
show_dashboard = true
language = zh
```

### 🧾 参数说明
---

#### `[SOURCE]`
## 🧾 关键参数说明

- `type`：源端类型，支持 `local` / `s3`
- `path`：源端为本地时使用，支持目录、单文件、通配符，例如 `/data/attachments/202604*`
- `ak/sk/endpoint/bucket/prefix`：源端为对象存储时使用
### `[SOURCE]` / `[TARGET]`

#### `[TARGET]`
- `type`：`local` 或 `s3`
- `path`：本地模式使用
- `ak / sk / endpoint / bucket / prefix`：对象存储模式使用
- `prefix` 可以为空；程序会自动做前缀拼接和清洗
- 如果源端本地路径使用通配符，目标端会**保留第一个通配符之前的静态目录层级**作为相对根目录  
  例如：源端 `/nfs2/qyk/qyfile/data/attachments/202604*`，目标前缀 `bak/xtbak/`，则会写成 `bak/xtbak/20260401/...`

- `type`：目标端类型，支持 `local` / `s3`
- `path`：目标端为本地时使用
- `ak/sk/endpoint/bucket/prefix`：目标端为对象存储时使用
### `[UPLOAD]`

#### `[UPLOAD]`

- `workers`：上传并发线程数
- `workers`：传输线程数，负责真正的数据搬运
- `checkers`：检查线程数，负责“是否已存在 / 是否需要跳过 / 是否需要上传”
- `part_size`：分片大小
- `multipart_threshold`：超过该阈值启用分片
- `retry`：失败重试次数
- `rate_limit`：API QPS 限制
- `multipart_threshold`：超过该阈值启用分片传输
- `retry`：任务级重试次数
- `rate_limit`：目标端 API 基础 QPS 限速，`0` 表示不限制
- `rate_limit_burst`：QPS 突发上限，建议大于等于 `rate_limit`
- `low_level_retries`：底层请求重试次数，覆盖 HEAD、copy、multipart 等网络调用
- `low_level_retry_sleep`：底层请求重试的基础退避秒数
- `max_connections`：全局最大连接数，`0` 表示不限制
- `multipart_concurrency`：单个大文件内部的分片并发数
- `max_buffer_memory`：流式缓冲总预算，`0` 表示不限制
- `request_timeout`：单次请求超时秒数
- `worker_stall_timeout`：worker 心跳超时阈值，用于卡死探测

#### `[SCAN]`
### `[SCAN]`

- `scan_workers`：扫描线程数上限，本地与远端扫描都生效
- `batch_size`：扫描批次
- `queue_size`：任务队列最大长度
- `scan_workers`：扫描线程上限，本地扫描和远端对象扫描都生效
- `batch_size`：单批扫描入队数量
- `queue_size`：检查队列和传输队列的最大长度

#### `[CHECK]`
### `[CHECK]`

- `enable_etag_check`：是否启用 ETAG 对比
- `enable_head_check`：是否启用 HEAD 校验
- `strict_client_check`：client 未初始化时是否直接报错
- `enable_etag_check`：上传前是否启用 ETAG 比对
- `enable_head_check`：上传前是否启用 HEAD 校验
- `strict_client_check`：客户端初始化失败时是否直接退出
- `target_compare_mode`：目标端比较策略
- `verify_after_upload`：传输完成后的校验策略

#### `[PATH]`
### `[PATH]`

- `log_dir`：日志目录
- `state_dir`：断点数据库目录
- `state_dir`：断点数据库目录，`tasks.db` 会写到这里
- `failed_dir`：失败任务目录

#### `[UI]`
### `[UI]`

- `prompt_config`：启动时是否允许交互修改配置
- `show_dashboard`：是否显示实时仪表盘
- `language`：仪表盘界面语言，`zh` 显示中文 / English 双语指标，`en` 显示英文

> [!TIP]
> `logs`、`state`、`failed`、`check_report` 等运行目录，都会**相对配置文件所在目录**解析，而不是相对当前终端目录。

---

## 🧠 比较策略与传后校验

这部分很重要，尤其是你需要在“性能”和“严格确认”之间做取舍时。

### `target_compare_mode`

#### `auto`
- 默认模式
- 本地单文件场景会自动偏向 `head_only`
- 目标端为对象存储且索引已完成时，大多数场景会自动落到 `hybrid`

#### `hybrid`
- 优先利用目标端索引做快速判断
- 索引未命中时，通常不再额外发 `HEAD`
- 索引命中时，会再补一次 `HEAD`
- 适合想兼顾吞吐与谨慎确认的场景

#### `index_only`
- 优先利用目标端索引做快速判断
- 索引命中且 `size` 一致时，直接按索引跳过，不再补 `HEAD`
- 索引未命中时，直接进入传输
- 适合目标端前缀稳定、重跑较多、希望尽量减少重复请求的场景

#### `head_only`
- 每个对象都尽量走 `HEAD`
- 一致性确认最直接，但请求量更大
- 适合对“外部并发写入目标前缀”比较敏感的场景

### `verify_after_upload`

#### `none`

## 🌈 运行界面与体验优化
- 不做传后校验

当前版本已经补上了不少体验细节：
#### `size`

- 🎛️ 仪表盘支持进度条、百分比、大小、速度、队列、线程状态展示
- 🧵 `Scan Status` 会显示类似 `running (49 active, target 49)`
- 📉 扫描并发会根据队列压力自动收缩 / 放大，避免队列长期打满
- ✍️ 启动时可以直接输入编号修改配置，不会再把 `7` 误判成直接开始迁移
- 🔗 路径展示会自动识别协议，OBS endpoint 显示为 `obs://bucket/key`，其他场景默认显示为 `s3://bucket/key`
- 传后只校验大小

---
#### `etag`

## ⚡ 已做的性能优化
- 传后校验大小和 ETAG
- 如果源端 / 目标端 ETAG 不可简单比较，会直接报校验不可用

### 1. 远端扫描不再固定单线程
#### `head`

- `scan_workers` 同时作用于本地扫描与远端对象扫描
- 远端扫描基于前缀并发列举
- 引入自适应扫描控制，避免队列过满时继续盲目堆任务
- 默认推荐模式
- 传后一定做一次目标端 `HEAD`
- 一定校验大小
- 如果 ETAG 可比较，还会顺带校验 ETAG

### 2. 同 endpoint 的远端迁移优先服务端拷贝

- 小文件优先尝试 `copyObject`
- 大文件优先尝试 `copyPart`
- 命中时数据不再经过本机中转，吞吐通常会明显提升
> [!IMPORTANT]
> `enable_head_check` / `enable_etag_check` 是**传输前判断是否需要跳过**；  
> `verify_after_upload` 是**传输完成后的后验校验**。  
> 这两组参数作用阶段不同，不要混用。

### 3. 索引命中后减少不必要 HEAD

- 当目标端索引已构建完成，且明确索引 miss 时，会跳过部分预上传 HEAD
- 对海量小文件场景尤其有效

### 4. 运行目录相对配置文件解析

- 避免因为“从不同目录启动程序”导致日志、状态库、报告目录跑偏

---

## 🧠 一致性与跳过判定逻辑

上传前主要有三层信息来源：

### 1. 目标端索引

- 启动时先构建目标前缀索引
- 缓存 `key -> size / etag`
- 命中时优先快速判定

### 2. HEAD 校验

- 当开启 `enable_head_check` 时，会对目标端做元数据确认
- 可与 ETAG 联合使用

### 3. 本地 checkpoint

- 当 HEAD 异常时，checkpoint 用作兜底
- 用于断点续跑、异常中断恢复

简化逻辑如下：

- `EXIST_SAME`：跳过
- `EXIST_DIFF`：覆盖上传
- `NOT_EXIST`：上传
- `ERROR`：尝试 checkpoint 兜底

### 关于“索引 miss 时跳过 HEAD”

这是当前版本的重要性能优化策略之一。

**适用前提：**

- 目标端索引是当前任务启动时刚构建出来的
- 迁移过程中，没有其他程序并发写入同一目标前缀

**优点：**

- 减少大量 HEAD 请求
- 对海量小文件场景提升明显
## 🌈 仪表盘与运行状态

**边界：**
当前仪表盘会展示：

- 如果索引构建后，目标端又被外部程序并发写入，同名对象可能在索引 miss 后已存在
- 这种情况下，跳过预上传 HEAD 会降低上传前存在性确认的严格程度
- `Transfer`：总进度条、百分比、已传输大小、速率、ETA
- `Index Status`：目标端索引构建状态
- `Scan Status`：扫描状态和活跃扫描线程
- `Check Status`：检查阶段状态和活跃检查线程
- `Upload Status`：传输阶段状态和活跃传输线程
- `Check Queue` / `Queue Size`：检查队列与传输队列水位
- `Scan Workers`：当前有效扫描线程 / 上限扫描线程
- `Upload Workers`：传输线程数
- `stalled`：如果某阶段 worker 长时间没有心跳，会显示卡死数量

**建议：**
### 仪表盘读法建议

- 常规迁移：保持当前默认策略，优先性能
- 高严格校验场景：开启 `enable_head_check = true`，并在迁移完成后做一次独立复核
- `Queue Size` 长期打满：通常是传输阶段更慢
- `Check Queue` 长期堆积：通常是检查阶段更慢，考虑增加 `checkers`
- `Queue Size` 长期偏空：通常是扫描或检查阶段跟不上
- `stalled` 出现并长期不下降：建议重点查看网络、服务端接口响应和日志

---

## 📊 仪表盘与报告
## 📊 报告与审计

### 实时仪表盘

仪表盘会展示这些核心指标：

- `Files Done`
- `Upload Skip`
- `Scan Skip`
- `Index Status`
- `Scan Status`
- `Check Status`
- `Upload Status`
- `Cache Hit / Hit Rate`
- `Progress`
- `Scan Files / Scan Speed`
- `Process Speed`（累计处理速度）
- `Net Upload Speed`（最近 5 秒实时上传速度）
- `Check Queue / Transfer Queue`
- `Check Workers / Upload Workers / Scan Workers`

> [!TIP]
> `UI.language = zh` 时，仪表盘指标会显示“中文 / English”双语对照；  
> `UI.language = en` 时，仪表盘只显示英文名称。

### 报告目录
### 输出目录

```text
check_report/
### 常见状态

- `SUCCESS`
- `UPLOAD_SKIP`
- `SCAN_SKIP`
- `SKIP`
- `FAILED`
- `ERROR`
- `INTERRUPTED`
- `UNFINISHED`
- `UNKNOWN`

### 未完成任务也会写入报告
### 为什么现在报告更完整？

如果任务失败、中断、提前停止，已经扫描到但尚未真正迁移完成的对象，也会补写进 CSV：
即使任务失败、中断或提前停止，已经扫描到但还没真正完成迁移的对象，也会补写到报告里：

- `source_path`：有值
- `target_path`：通常为空
- `source_path`：保留源端路径
- `target_path`：未完成时允许为空
- `size`：记录源对象大小
- `message`：默认会写入 `detected_but_not_migrated`

这样排查问题时，不会再出现“源端明明识别到了，但报告里完全看不到”的情况。
这样排查时不会再出现“源端明明已经识别到了，但报告里完全没有”的问题。

---

## 🔐 配置安全

当前版本支持以下安全机制：
## ⚡ 性能调优建议

### 1. 敏感配置加密
### 本地 -> 对象存储

- `ak/sk` 使用 Fernet 加密后写入配置文件
- 明文不会直接落盘
- `workers = 16 ~ 64`
- `checkers = 8 ~ 32`
- `scan_workers = 4 ~ 16`
- `part_size = 64M ~ 128M`
- `verify_after_upload = head` 适合作为默认安全值

### 2. 密钥独立存放
### 对象存储 -> 对象存储

- `.config.key` 与配置文件放在同一目录
- 配置密文无法脱离对应密钥单独解密

### 3. 真实配置可放仓库外
- 源和目标是同 endpoint 时，优先命中服务端拷贝
- 大文件可以适当提高 `multipart_concurrency`
- 如果 `Check Queue` 长期堆积，优先提高 `checkers`
- 如果 `Queue Size` 长期打满，优先排查传输侧或降低扫描压力

- 通过环境变量 `OBS_MIGRATE_CONFIG` 指定真实配置文件路径
- 适合把真实 `config.ini` 放到 Git 仓库之外
### 海量小文件

```powershell
$env:OBS_MIGRATE_CONFIG = 'D:\secure\obs_sync_check\config.ini'
python obs_migrate.py
```
- 更推荐 `target_compare_mode = hybrid`
- 结合目标端索引可以显著减少不必要的 `HEAD`
- 如果目标端前缀稳定、重跑较多、主要是跳过已存在对象，更推荐 `target_compare_mode = index_only`
- 如果对目标端外部写入特别敏感，再切到 `head_only`

### 4. 推荐的 Git 策略
### 内网离线环境

- ✅ 提交 `config.example.ini`
- ✅ 提交文档、脚本、源码
- ❌ 不要提交真实 `config.ini`
- ❌ 不要提交 `.config.key`
- 🔁 如果历史上已经提交过敏感文件，建议尽快轮换 AK / SK
- 建议结合 `max_connections`、`rate_limit`、`max_buffer_memory` 做资源治理
- 不建议只靠盲目拉高线程数来追求吞吐

---

## 🧪 环境变量速查
## 📦 离线部署 / 离线发包

| 环境变量 | 作用 |
| --- | --- |
| `OBS_MIGRATE_CONFIG` | 指定配置文件路径 |
| `OBS_MIGRATE_VENDOR` | 指定离线依赖目录 |
| `OBS_MIGRATE_INTERACTIVE` | 控制是否启用交互修改配置 |
| `OBS_MIGRATE_DASHBOARD` | 控制是否显示实时仪表盘 |
| `OBS_MIGRATE_FORCE_TERMINAL` | 强制以终端模式渲染 Rich 界面 |
如果目标机器 **没有公网**、**不能 `pip install`**，可以直接使用离线发包脚本：

---

## 📁 目录结构

```text
core/
├── build_db.py
├── checkpoint.py
├── dashboard.py
├── obs_index.py
├── progress.py
├── ratelimiter.py
├── report.py
├── s3_scanner.py
├── scan_control.py
├── scanner.py
├── scheduler.py
├── uploader.py
├── utils.py
tools/
├── prepare_vendor.py
bootstrap_runtime.py
build_offline_bundle.py
config.example.ini
obs_migrate.py
OFFLINE_DEPLOY.md
README.md
```bash
python build_offline_bundle.py
```

---
脚本会引导你选择：

## ⚙️ 性能调优建议
- 🪟 `windows`
- 🐧 `linux`

### 本地 -> 对象存储
然后自动完成：

- `workers = 16 ~ 64`
- `scan_workers = 4 ~ 16`
- `part_size = 64M ~ 128M`
- `rate_limit` 结合服务端限额调节
- 生成发布目录
- 复制运行所需源码
- 从 `lib/` 解包匹配平台的依赖到 `vendor/`
- 生成启动脚本
- 生成 `bundle_manifest.json`

### 对象存储 -> 对象存储
也可以显式传参：

- 如果源和目标是同 endpoint，优先利用服务端拷贝
- 此时 `workers` 往往比 `scan_workers` 更关键
- 若 `Queue Size` 长期打满，通常说明上传侧是瓶颈
- 若 `Queue Size` 长期偏空，通常说明扫描侧是瓶颈
```bash
python build_offline_bundle.py --platform windows --python-tag py39
python build_offline_bundle.py --platform linux --python-tag py39 --strict-centos7
```

### 海量小文件
更多说明见 `OFFLINE_DEPLOY.md`。

- 提高 `workers`
- 保持较大的 `queue_size`
- 保持目标索引开启
- 在可接受前提下减少不必要 HEAD
---

### 大文件
## 🔐 配置安全建议

- 增大 `part_size`
- 关注 `multipart_threshold`
- 观察是否命中服务端分片拷贝
- ✅ 提交 `config.example.ini`
- ✅ 提交源码、脚本、文档
- ❌ 不要提交真实 `config.ini`
- ❌ 不要提交 `.config.key`
- 🔁 如果历史上提交过真实 AK / SK，请尽快轮换

### 遇到 `403 / 503`
推荐做法：

- 降低 `workers`
- 降低 `rate_limit`
- 必要时降低 `scan_workers`
1. 仓库里只保留 `config.example.ini`
2. 把真实配置放到仓库外
3. 用 `OBS_MIGRATE_CONFIG` 指向真实配置

---

## 🧱 当前边界与建议
## 📁 项目结构

### 关于其他 S3 兼容对象存储

- 配置层已经支持 `type = s3`
- 展示层已支持 `obs://` / `s3://` 自动识别
- 但远端实现仍基于 OBS SDK
- 因此“是否完全支持某家 S3 兼容服务”，最终取决于该服务对 OBS SDK 所调用接口的兼容程度

### 关于最终校验

- 当前版本已经有索引、HEAD、checkpoint、报告
- 但还没有独立的“迁移完成后二次全量比对器”
- 如果是高审计要求场景，建议迁移完成后再做一次源 / 目标清单复核
```text
core/
├── capabilities.py      # 后端能力探测
├── checkpoint.py        # 断点状态
├── dashboard.py         # 实时仪表盘
├── governor.py          # 统一资源治理
├── obs_index.py         # 目标端索引
├── progress.py          # 进度统计
├── ratelimiter.py       # 限速器
├── report.py            # 报告输出
├── retry.py             # 底层重试
├── s3_scanner.py        # 远端对象扫描
├── scan_control.py      # 自适应扫描控制
├── scanner.py           # 本地扫描
├── scheduler.py         # worker 调度、心跳、卡死探测
├── uploader.py          # 检查 / 传输主逻辑
└── utils.py             # 通用工具
tools/
├── prepare_vendor.py    # 依赖整理脚本
bootstrap_runtime.py     # 启动时加载本地 vendor
build_offline_bundle.py  # 离线发包脚本
config.example.ini       # 提交到仓库的配置模板
obs_migrate.py           # 命令行入口
OFFLINE_DEPLOY.md        # 离线部署说明
README.md                # 项目说明
```

---

## 🗺️ 后续可扩展方向

- ✅ 独立最终校验阶段
- ✅ 更通用的原生 S3 backend
- ✅ 删除同步
- ✅ 多桶批量迁移
- ✅ 分布式断点共享
- ✅ Prometheus / 可视化监控
## ❤️ 适合谁用

---
如果你的场景里同时有这些要求，这个工具会比较合适：

## 💡 相关文件
- 要跑在 **内网 / 离线环境**
- 对 **OBS** 支持要求高
- 有 **大量小文件**
- 需要 **断点续传**
- 需要 **迁移报告**
- 希望看到实时的 **仪表盘 / 线程状态 / 队列水位**

- `config.example.ini`：示例配置模板
- `OFFLINE_DEPLOY.md`：离线部署详解
- `build_offline_bundle.py`：离线发布目录构建脚本
- `bootstrap_runtime.py`：本地 `vendor` 依赖自动加载入口

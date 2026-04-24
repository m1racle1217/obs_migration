# ☁️ OBS / S3 兼容对象存储迁移工具

> 一个面向 **TB 级数据**、**海量小文件**、**断点续传**、**离线环境部署** 的迁移工具。  
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

---

## 🧭 支持的迁移模式

| 模式 | 说明 | 典型场景 |
| --- | --- | --- |
| `local -> s3` | 本地文件上传到对象存储 | 备份、归档、上云 |
| `s3 -> local` | 对象存储下载到本地 | 恢复、回迁、本地审计 |
| `s3 -> s3` | 对象存储之间迁移 | OBS ↔ OBS、其他 S3 ↔ OBS |

---

## 🧩 核心能力

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

---

## 🏗️ 架构概览

```text
Scanner / Remote Scanner
   ↓
Queue（背压缓冲）
   ↓
Scheduler（线程调度）
   ↓
Decision Engine（索引 / HEAD / checkpoint）
   ↓
Uploader（上传 / 分片 / 服务端拷贝）
   ↓
Checkpoint / Report / Dashboard
```

---

## 🚀 快速开始

### 1. 在线安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动程序

```bash
python obs_migrate.py
```

首次运行会引导生成配置文件；如果开启交互模式，也可以直接输入配置项编号进行修改。

### 3. 非交互运行

```powershell
$env:OBS_MIGRATE_INTERACTIVE = '0'
python obs_migrate.py
```

### 4. 关闭实时仪表盘

```powershell
$env:OBS_MIGRATE_DASHBOARD = '0'
python obs_migrate.py
```

---

## 📦 离线部署 / 离线发包

如果目标机器 **没有公网**、**不能 `pip install`**，推荐直接使用离线发布脚本：

```bash
python build_offline_bundle.py
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

```bash
python build_offline_bundle.py --platform windows --python-tag py39
python build_offline_bundle.py --platform linux --python-tag py39 --strict-centos7
```

> [!TIP]
> 更完整的离线部署说明见 `OFFLINE_DEPLOY.md`。

---

## ⚙️ 配置结构

当前配置已经从早期单一 `[OBS] + [TASK]` 结构升级为双端配置：

```ini
[SOURCE]
type = local | s3
path =
ak =
sk =
endpoint =
bucket =
prefix =

[TARGET]
type = local | s3
path =
ak =
sk =
endpoint =
bucket =
prefix =

[UPLOAD]
workers = 32
part_size = 64M
multipart_threshold = 128M
retry = 3
rate_limit = 200

[SCAN]
scan_workers = 4
batch_size = 1000
queue_size = 20000

[CHECK]
enable_etag_check = false
enable_head_check = true
strict_client_check = true

[PATH]
log_dir = ./logs
state_dir = ./state
failed_dir = ./failed

[UI]
prompt_config = true
show_dashboard = true
```

### 🧾 参数说明

#### `[SOURCE]`

- `type`：源端类型，支持 `local` / `s3`
- `path`：源端为本地时使用
- `ak/sk/endpoint/bucket/prefix`：源端为对象存储时使用

#### `[TARGET]`

- `type`：目标端类型，支持 `local` / `s3`
- `path`：目标端为本地时使用
- `ak/sk/endpoint/bucket/prefix`：目标端为对象存储时使用

#### `[UPLOAD]`

- `workers`：上传并发线程数
- `part_size`：分片大小
- `multipart_threshold`：超过该阈值启用分片
- `retry`：失败重试次数
- `rate_limit`：API QPS 限制

#### `[SCAN]`

- `scan_workers`：扫描线程数上限，本地与远端扫描都生效
- `batch_size`：扫描批次
- `queue_size`：任务队列最大长度

#### `[CHECK]`

- `enable_etag_check`：是否启用 ETAG 对比
- `enable_head_check`：是否启用 HEAD 校验
- `strict_client_check`：client 未初始化时是否直接报错

#### `[PATH]`

- `log_dir`：日志目录
- `state_dir`：断点数据库目录
- `failed_dir`：失败任务目录

#### `[UI]`

- `prompt_config`：启动时是否允许交互修改配置
- `show_dashboard`：是否显示实时仪表盘

> [!TIP]
> `logs`、`state`、`failed`、`check_report` 等运行目录，都会**相对配置文件所在目录**解析，而不是相对当前终端目录。

---

## 🌰 典型配置示例

### 1. 本地目录上传到 OBS

```ini
[SOURCE]
type = local
path = /data/source

[TARGET]
type = s3
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = your-bucket
prefix = backup/
```

### 2. OBS 下载到本地

```ini
[SOURCE]
type = s3
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = your-bucket
prefix = backup/

[TARGET]
type = local
path = /data/restore
```

### 3. 对象存储到对象存储

```ini
[SOURCE]
type = s3
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = source-bucket
prefix = source/

[TARGET]
type = s3
endpoint = obs.cn-south-1.myhuaweicloud.com
bucket = target-bucket
prefix = target/
```

---

## 🌈 运行界面与体验优化

当前版本已经补上了不少体验细节：

- 🎛️ 仪表盘支持进度条、百分比、大小、速度、队列、线程状态展示
- 🧵 `Scan Status` 会显示类似 `running (49 active, target 49)`
- 📉 扫描并发会根据队列压力自动收缩 / 放大，避免队列长期打满
- ✍️ 启动时可以直接输入编号修改配置，不会再把 `7` 误判成直接开始迁移
- 🔗 路径展示会自动识别协议，OBS endpoint 显示为 `obs://bucket/key`，其他场景默认显示为 `s3://bucket/key`

---

## ⚡ 已做的性能优化

### 1. 远端扫描不再固定单线程

- `scan_workers` 同时作用于本地扫描与远端对象扫描
- 远端扫描基于前缀并发列举
- 引入自适应扫描控制，避免队列过满时继续盲目堆任务

### 2. 同 endpoint 的远端迁移优先服务端拷贝

- 小文件优先尝试 `copyObject`
- 大文件优先尝试 `copyPart`
- 命中时数据不再经过本机中转，吞吐通常会明显提升

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

**边界：**

- 如果索引构建后，目标端又被外部程序并发写入，同名对象可能在索引 miss 后已存在
- 这种情况下，跳过预上传 HEAD 会降低上传前存在性确认的严格程度

**建议：**

- 常规迁移：保持当前默认策略，优先性能
- 高严格校验场景：开启 `enable_head_check = true`，并在迁移完成后做一次独立复核

---

## 📊 仪表盘与报告

### 实时仪表盘

仪表盘会展示这些核心指标：

- `Files Done`
- `Upload Skip`
- `Scan Skip`
- `Index Status`
- `Scan Status`
- `Upload Status`
- `Cache Hit / Hit Rate`
- `Progress`
- `Scan Files / Scan Speed`
- `Upload Speed`
- `Queue Size`
- `Upload Workers / Scan Workers`

### 报告目录

```text
check_report/
├── xxx.csv
└── xxx_summary.json
```

### CSV 字段

- `source_path`
- `target_path`
- `size`
- `status`
- `message`

### 常见状态

- `SUCCESS`
- `UPLOAD_SKIP`
- `SCAN_SKIP`
- `FAILED`
- `ERROR`
- `INTERRUPTED`
- `UNFINISHED`
- `UNKNOWN`

### 未完成任务也会写入报告

如果任务失败、中断、提前停止，已经扫描到但尚未真正迁移完成的对象，也会补写进 CSV：

- `source_path`：有值
- `target_path`：通常为空
- `size`：记录源对象大小
- `message`：默认会写入 `detected_but_not_migrated`

这样排查问题时，不会再出现“源端明明识别到了，但报告里完全看不到”的情况。

---

## 🔐 配置安全

当前版本支持以下安全机制：

### 1. 敏感配置加密

- `ak/sk` 使用 Fernet 加密后写入配置文件
- 明文不会直接落盘

### 2. 密钥独立存放

- `.config.key` 与配置文件放在同一目录
- 配置密文无法脱离对应密钥单独解密

### 3. 真实配置可放仓库外

- 通过环境变量 `OBS_MIGRATE_CONFIG` 指定真实配置文件路径
- 适合把真实 `config.ini` 放到 Git 仓库之外

```powershell
$env:OBS_MIGRATE_CONFIG = 'D:\secure\obs_sync_check\config.ini'
python obs_migrate.py
```

### 4. 推荐的 Git 策略

- ✅ 提交 `config.example.ini`
- ✅ 提交文档、脚本、源码
- ❌ 不要提交真实 `config.ini`
- ❌ 不要提交 `.config.key`
- 🔁 如果历史上已经提交过敏感文件，建议尽快轮换 AK / SK

---

## 🧪 环境变量速查

| 环境变量 | 作用 |
| --- | --- |
| `OBS_MIGRATE_CONFIG` | 指定配置文件路径 |
| `OBS_MIGRATE_VENDOR` | 指定离线依赖目录 |
| `OBS_MIGRATE_INTERACTIVE` | 控制是否启用交互修改配置 |
| `OBS_MIGRATE_DASHBOARD` | 控制是否显示实时仪表盘 |
| `OBS_MIGRATE_FORCE_TERMINAL` | 强制以终端模式渲染 Rich 界面 |

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
```

---

## ⚙️ 性能调优建议

### 本地 -> 对象存储

- `workers = 16 ~ 64`
- `scan_workers = 4 ~ 16`
- `part_size = 64M ~ 128M`
- `rate_limit` 结合服务端限额调节

### 对象存储 -> 对象存储

- 如果源和目标是同 endpoint，优先利用服务端拷贝
- 此时 `workers` 往往比 `scan_workers` 更关键
- 若 `Queue Size` 长期打满，通常说明上传侧是瓶颈
- 若 `Queue Size` 长期偏空，通常说明扫描侧是瓶颈

### 海量小文件

- 提高 `workers`
- 保持较大的 `queue_size`
- 保持目标索引开启
- 在可接受前提下减少不必要 HEAD

### 大文件

- 增大 `part_size`
- 关注 `multipart_threshold`
- 观察是否命中服务端分片拷贝

### 遇到 `403 / 503`

- 降低 `workers`
- 降低 `rate_limit`
- 必要时降低 `scan_workers`

---

## 🧱 当前边界与建议

### 关于其他 S3 兼容对象存储

- 配置层已经支持 `type = s3`
- 展示层已支持 `obs://` / `s3://` 自动识别
- 但远端实现仍基于 OBS SDK
- 因此“是否完全支持某家 S3 兼容服务”，最终取决于该服务对 OBS SDK 所调用接口的兼容程度

### 关于最终校验

- 当前版本已经有索引、HEAD、checkpoint、报告
- 但还没有独立的“迁移完成后二次全量比对器”
- 如果是高审计要求场景，建议迁移完成后再做一次源 / 目标清单复核

---

## 🗺️ 后续可扩展方向

- ✅ 独立最终校验阶段
- ✅ 更通用的原生 S3 backend
- ✅ 删除同步
- ✅ 多桶批量迁移
- ✅ 分布式断点共享
- ✅ Prometheus / 可视化监控

---

## 💡 相关文件

- `config.example.ini`：示例配置模板
- `OFFLINE_DEPLOY.md`：离线部署详解
- `build_offline_bundle.py`：离线发布目录构建脚本
- `bootstrap_runtime.py`：本地 `vendor` 依赖自动加载入口

如果你还想继续，我下一步可以帮你把 `OFFLINE_DEPLOY.md` 也同步成同一套 emoji 风格。  

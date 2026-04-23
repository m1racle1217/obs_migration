🚀 OBS / S3 兼容对象存储迁移工具

一个面向 TB 级、海量小文件、断点续传场景的数据迁移工具，重点关注：

- 稳定性：失败重试、限流、断点恢复
- 一致性：索引 + HEAD + checkpoint 多层判定
- 吞吐：扫描与上传解耦、多线程、分片、服务端拷贝
- 可审计：日志、失败清单、CSV/JSON 报告


✨ 当前能力概览

1. 迁移模式
- `local -> s3`
- `s3 -> local`
- `s3 -> s3`
- 这里的 `s3` 表示 S3 协议兼容对象存储
- 当前远端实现基于华为 OBS Python SDK，OBS 为优先支持场景；其他 S3 兼容服务是否可用，取决于其对相关接口的兼容程度

2. 扫描能力
- 本地目录多线程扫描
- 远端对象存储多线程扫描
- 扫描与上传解耦，使用队列做背压控制
- 自动忽略隐藏文件、临时文件、常见系统垃圾文件

3. 传输能力
- 普通上传
- 分片上传
- 同 endpoint/bucket 场景优先尝试服务端拷贝
- 失败自动重试
- 内置 QPS 限流

4. 一致性与跳过判定
- 本地 checkpoint 断点续传
- 目标端对象索引缓存
- 可选 HEAD 校验
- 可选 ETAG 校验
- 命中缓存时优先快速跳过

5. 可观测性
- Rich 实时仪表盘
- 详细日志
- 失败任务文件
- `check_report` 审计报告


🧱 架构概览

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


📦 配置结构

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


🧭 参数说明

`[SOURCE]`
- `type`：源端类型，支持 `local` / `s3`
- `path`：源端为本地时使用
- `ak/sk/endpoint/bucket/prefix`：源端为对象存储时使用

`[TARGET]`
- `type`：目标端类型，支持 `local` / `s3`
- `path`：目标端为本地时使用
- `ak/sk/endpoint/bucket/prefix`：目标端为对象存储时使用

`[UPLOAD]`
- `workers`：上传并发线程数
- `part_size`：分片大小
- `multipart_threshold`：超过该阈值走分片
- `retry`：失败重试次数
- `rate_limit`：API QPS 限制

`[SCAN]`
- `scan_workers`：扫描线程数，本地和远端扫描都生效
- `batch_size`：扫描批次
- `queue_size`：任务队列最大长度

`[CHECK]`
- `enable_etag_check`：是否启用 ETAG 对比
- `enable_head_check`：是否启用 HEAD 校验
- `strict_client_check`：client 未初始化时是否直接报错

`[PATH]`
- `log_dir`：日志目录
- `state_dir`：断点数据库目录
- `failed_dir`：失败任务目录

`[UI]`
- `prompt_config`：启动时是否允许交互修改配置
- `show_dashboard`：是否显示实时仪表盘


🎯 典型场景

1. 本地目录上传到 OBS
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

2. OBS 下载到本地
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

3. 对象存储到对象存储
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


⚡ 新版性能优化点

1. 远端扫描不再固定单线程
- `scan_workers` 现在同时作用于本地扫描与远端对象扫描
- 远端扫描会基于前缀做并发列举

2. 同 endpoint 的远端迁移优先服务端拷贝
- 小文件优先尝试 `copyObject`
- 大文件分片优先尝试 `copyPart`
- 命中时数据不再经本机中转，吞吐会明显提升
- 日志中会出现 `[SERVER_COPY_ENABLED]`

3. 索引命中后减少不必要 HEAD
- 当目标索引已经构建完成，且明确索引 miss 时，会跳过部分预上传 HEAD
- 这样可以显著减少大量小文件迁移时的元数据请求

4. 运行路径改为相对配置文件解析
- `logs`、`state`、`failed`、`check_report`
- 都会相对当前配置文件所在目录解析
- 不再依赖进程启动目录


🧠 一致性判定逻辑

上传前主要有三层信息来源：

1. 目标端索引
- 启动时先构建目标前缀索引
- 缓存 `key -> size / etag`
- 命中时优先快速判定

2. HEAD 校验
- 当开启 `enable_head_check` 时，会对目标端做元数据确认
- 可与 ETAG 联合使用

3. 本地 checkpoint
- 当 HEAD 异常时，checkpoint 用作兜底
- 用于断点续跑、异常中断恢复

简化逻辑：
- `EXIST_SAME`：跳过
- `EXIST_DIFF`：覆盖上传
- `NOT_EXIST`：上传
- `ERROR`：尝试 checkpoint 兜底


⚠️ 关于“索引 miss 时跳过 HEAD”的说明

这是当前版本的性能优化策略之一。

适用前提：
- 目标端索引是刚刚构建出来的
- 迁移过程中，没有其他外部程序同时往同一目标前缀写入对象

优点：
- 少一次甚至大量 HEAD 请求
- 对海量小文件吞吐帮助非常明显

边界：
- 如果目标端在索引构建后被其他程序并发写入，同名对象有可能在索引 miss 后已存在
- 这种情况下，跳过预上传 HEAD 会降低“上传前存在性确认”的严格程度

建议：
- 常规迁移：保持当前默认策略，优先性能
- 高严格校验场景：开启 `enable_head_check = true`，并在迁移结束后做一次独立复核


📊 仪表盘与报告

实时仪表盘会展示：
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

报告目录：

```text
check_report/
├── xxx.csv
└── xxx_summary.json
```

CSV 字段：
- `local_path`
- `obs_key`
- `size`
- `status`
- `message`

常见状态：
- `SUCCESS`
- `UPLOAD_SKIP`
- `SCAN_SKIP`
- `FAILED`
- `MISSING`


🔐 配置安全

当前版本支持以下安全机制：

1. 配置加密
- `ak/sk` 使用 Fernet 加密后写入配置文件
- 明文不会直接落盘

2. 密钥独立存放
- `.config.key` 与配置文件放在同一目录
- 配置文件密文离不开对应密钥文件

3. 支持把真实配置放到仓库外
- 通过环境变量 `OBS_MIGRATE_CONFIG` 指定配置文件路径
- 适合把真实 `config.ini` 放到 Git 仓库之外

PowerShell 示例：
```powershell
$env:OBS_MIGRATE_CONFIG = 'D:\secure\obs_sync_check\config.ini'
python obs_migrate.py
```

4. 建议的 Git 策略
- 仓库只提交 `config.example.ini`
- 本地真实 `config.ini` 和 `.config.key` 加入 `.gitignore`
- 如果历史上已经提交过密钥文件，建议及时轮换 AK/SK


🖥️ 交互体验优化

1. 启动时可以直接输入编号修改配置
- 例如在 `是否修改配置?` 处直接输入 `7`
- 会直接进入对应配置项编辑
- 不会再误判为“不修改并直接开始迁移”

2. 路径展示自动识别协议
- OBS endpoint 日志展示为 `obs://bucket/key`
- 其他场景默认展示为 `s3://bucket/key`
- 仅影响显示与日志，不影响内部兼容逻辑


📂 目录结构

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
├── scanner.py
├── scheduler.py
├── uploader.py
├── utils.py
config.example.ini
obs_migrate.py
logs/
state/
failed/
check_report/
```


🚀 使用方式

1. 安装依赖
```bash
pip install -r requirements.txt
```

离线安装：
```bash
pip install --no-index --find-links=./lib -r requirements.txt
```

2. 启动
```bash
python obs_migrate.py
```

首次运行会引导生成配置文件。

3. 非交互运行
- 设置 `UI.prompt_config = false`
- 或设置环境变量关闭交互：

```powershell
$env:OBS_MIGRATE_INTERACTIVE = '0'
python obs_migrate.py
```

4. 关闭实时仪表盘
```powershell
$env:OBS_MIGRATE_DASHBOARD = '0'
python obs_migrate.py
```


🧪 性能调优建议

1. 本地 -> 对象存储
- `workers = 16 ~ 64`
- `scan_workers = 4 ~ 16`
- `part_size = 64M ~ 128M`
- `rate_limit` 结合服务端限额调节

2. 对象存储 -> 对象存储
- 如果源和目标是同 endpoint，优先利用服务端拷贝
- 此时 `workers` 比 `scan_workers` 更关键
- 若 `Queue Size` 长期满，说明上传侧是瓶颈
- 若 `Queue Size` 长期空，说明扫描侧是瓶颈

3. 海量小文件
- 提高 `workers`
- 保持较大的 `queue_size`
- 保持目标索引开启
- 在可接受前提下减少不必要 HEAD

4. 大文件
- 增大 `part_size`
- 关注 `multipart_threshold`
- 观察是否命中服务端分片拷贝

5. 遇到 403 / 503
- 降低 `workers`
- 降低 `rate_limit`
- 必要时减小 `scan_workers`


🧭 当前边界与建议

1. 关于其他 S3 兼容对象存储
- 配置层已经支持 `type = s3`
- 展示层已支持 `obs://` / `s3://` 自动识别
- 但远端实现仍基于 OBS SDK
- 因此“是否完全支持某家 S3 兼容服务”取决于该服务对 OBS SDK 所调用接口的兼容程度

2. 关于最终校验
- 当前版本已经有索引、HEAD、checkpoint、报告
- 但还没有独立的“迁移完成后二次全量比对器”
- 如果是高审计要求场景，建议迁移完成后再做一次源/目标清单复核


🔮 后续可扩展方向

- 独立最终校验阶段
- 更通用的原生 S3 backend
- 删除同步
- 多桶批量迁移
- 分布式断点共享
- Prometheus / 可视化监控

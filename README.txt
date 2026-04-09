🚀 OBS 数据迁移工具（Production Enhanced Edition）
一个面向 TB / 亿级文件迁移场景 的高性能工具，专为 稳定性 / 一致性 / 可审计性 设计。

🧱 架构图
                +----------------------+
                |     Scanner          |
                | (多线程扫描文件)      |
                +----------+-----------+
                           |
                           v
                +----------------------+
                |     Queue            |
                | (任务缓冲 / 背压控制) |
                +----------+-----------+
                           |
                           v
                +----------------------+
                |    Scheduler         |
                | (限流 + 线程池调度)  |
                +----------+-----------+
                           |
        +------------------+------------------+
        |                                     |
        v                                     v
+---------------------+            +----------------------+
|    HEAD Checker     |            |      Uploader        |
| (远端一致性判断)     |            | (多线程 + 分片上传)   |
+----------+----------+            +----------+-----------+
           |                                  |
           +-------------+--------------------+
                         |
                         v
                +----------------------+
                |     Reporter         |
                | (日志 / 报告 / 审计) |
                +----------------------+

✨ 核心能力
🧠 一致性优先：HEAD + Checkpoint 双层判定
判定优先级：
HEAD（远端真实状态）
   ↓
失败时 fallback
   ↓
Checkpoint（本地状态）
判定逻辑
场景	行为
远端存在且 size 相同	✅ 直接跳过
远端存在但 size 不同	🔁 强制覆盖
HEAD 失败	⚠️ 使用 checkpoint 判断
checkpoint 已完成	⏭ 跳过
其他情况	⬆️ 上传
✔ 避免误判
✔ 不依赖本地状态
✔ 支持远端被删除后自动补传


🔁 断点续传
两层断点：
文件级 checkpoint（SQLite）
OBS SDK 分片断点（大文件）
checkpoint 特点（来自代码实现）
SQLite + WAL 模式（高并发）
内存缓存 + 批量刷盘（性能优化）
判定维度：path + size + mtime
✔ 精确恢复
✔ 支持进程异常退出
✔ 支持增量同步


⚡ 高性能架构（解耦 + 并发）
架构模型
Scanner（生产者）
   ↓
Queue（缓冲）
   ↓
Scheduler（线程池）
   ↓
Uploader（消费者）
特点
扫描 / 上传完全解耦
队列背压控制（queue_size）
多线程上传（workers）
扫描线程池（scan_workers）
✔ 适合百万级文件
✔ 不会阻塞扫描
✔ CPU / IO 利用率高


🛡️ 限流保护（Token Bucket 实现）
内置 令牌桶算法：
tokens += delta * rate
tokens >= 请求量 才允许发送
特点
基于文件大小动态消耗 token
防止：
OBS 403
OBS 503
API 被封
✔ 比固定 sleep 更智能
✔ 自适应请求速率

📂 扫描系统（生产级增强）
扫描特性（来自 scanner.py）
bytes 级路径处理（避免乱码）
自动编码修复（utf-8 / gbk / gb18030）
多线程目录遍历
自动忽略
隐藏文件（.xxx）
临时文件（.tmp / .part）
系统文件（.DS_Store / Thumbs.db）
扫描阶段 skip 分类（重要）
类型	说明
SKIP_SCAN	扫描阶段过滤
scan_error	stat / IO 异常
✔ 可审计
✔ 不污染上传逻辑

⬆上传系统（核心逻辑）
上传策略
文件大小	方式
< threshold	putFile
≥ threshold	分片上传
重试机制
最大 3 次
指数退避（2^n + 随机）
特殊处理
文件不存在 → 标记 MISSING
编码异常 → 自动识别
Windows 长路径支持

📊 实时进度（Rich UI）
基于 rich 实现：
传输进度条
实时速度
剩余时间
文件统计
✔ 实时可视化
✔ 大任务可控

📄 报告系统（企业级审计）
输出文件
check_report/
├── xxx.csv              # 明细
└── xxx_summary.json     # 汇总
CSV 字段
local_path, obs_key, size, status, message
状态分类（代码真实定义）
状态	说明
SUCCESS	上传成功
UPLOAD_SKIP	上传阶段跳过
SCAN_SKIP	扫描阶段跳过
FAILED	上传失败
MISSING	文件不存在
summary.json（核心指标）
{
  "TOTAL_FILES": 1000000,
  "SUCCESS": 800000,
  "UPLOAD_SKIP": 150000,
  "FAILED": 1000,
  "SCAN_SKIP": 49000,
  "TOTAL_SIZE": 1234567890
}
✔ 可审计
✔ 可对账
✔ 可用于报表

🔐 配置安全（内置加密）
AK/SK 自动加密存储（Fernet）：
首次运行自动生成密钥
config.ini 存储密文
✔ 避免明文泄露

🛠️ 环境要求
Python 3.8+
Linux / macOS（推荐）
Windows（支持）
安装依赖：
pip install -r requirements.txt

⚙️ 配置说明（config.ini）
[OBS]
ak = 加密存储
sk = 加密存储
endpoint = obs.xxx.myhuaweicloud.com
bucket = your-bucket
[TASK]
local_dir = /data/source
obs_prefix = backup/data
[UPLOAD]
workers = 32
part_size = 64M
multipart_threshold = 128M
retry = 3
rate_limit = 200
[SCAN]
scan_workers = 4
queue_size = 20000
batch_size = 1000
[CHECK]
enable_head_check = true
strict_client_check = true
[PATH]
log_dir = ./logs
state_dir = ./state
failed_dir = ./failed

🚀 使用方法
python obs_migrate.py
首次运行会引导配置。

📂 目录结构
core/
├── scanner.py        # 扫描器
├── uploader.py       # 上传核心
├── scheduler.py      # 调度器
├── checkpoint.py     # 断点数据库
├── rate_limiter.py   # 限流器
├── progress.py       # 进度展示
├── reporter.py       # 报告系统
├── utils.py          # 工具函数
logs/                 # 日志
state/                # checkpoint
failed/               # 失败列表
check_report/         # 审计报告

🧠 工作流程（真实执行路径）
扫描目录
   ↓
过滤无效文件（SCAN_SKIP）
   ↓
生成任务（含 size）
   ↓
HEAD 校验
   ↓
决策：
   - EXIST_SAME → 跳过
   - EXIST_DIFF → 上传
   - NOT_EXIST → 上传
   - ERROR → checkpoint 判断
   ↓
上传（限流 + 重试）
   ↓
记录 checkpoint
   ↓
写入报告
⚠️ 性能调优建议（实战）
推荐配置
workers = 32
scan_workers = 4
queue_size = 20000
rate_limit = 200
出现 403 / 503
👉 原因：请求过快
解决：
workers ↓
rate_limit ↓
海量文件（千万级）
建议：
scan_workers = 8
queue_size = 50000

❗ 设计原则
✅ 远端优先（HEAD）
保证一致性
✅ 本地兜底（checkpoint）
保证性能
✅ 扫描 / 上传解耦
保证吞吐
✅ 全链路可审计
保证可控

🔮 后续可扩展 （待优化）
 MD5 / ETag 强一致校验
 删除同步（--delete）
 多桶迁移
 断点分布式共享（多机器协同）
 Prometheus / 可视化监控
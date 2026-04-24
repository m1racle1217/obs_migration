# 📦 离线部署说明（Windows / Linux / CentOS 7.9）

> 这份文档专门说明：**目标机器不能联网、不能执行 `pip install`** 时，怎么把这个项目稳定跑起来。  
> 当前项目已经支持通过 `vendor/` 目录加载本地依赖，并提供了**自动构建离线发布目录**的脚本。

---

## ✨ 先说结论

对于你的场景，最推荐的不是让目标机器临时安装依赖，而是：

1. 在一台“准备机”上把依赖整理到 `vendor/`
2. 把源码和 `vendor/` 一起拷到目标机器
3. 在目标机器直接运行 `python obs_migrate.py`

如果你不想手动整理，直接用：

```bash
python build_offline_bundle.py
```

它会自动帮你生成适合 Windows 或 Linux 的离线发布目录。

---

## 🧭 两种离线部署方式

### 方式 A：自动构建离线发布目录（推荐）

适合大多数场景，省事、稳定、适合发版。

脚本：

- `build_offline_bundle.py`

它会自动完成：

- 复制运行所需源码
- 从 `lib/` 解包匹配平台的依赖到 `vendor/`
- 生成启动脚本
- 生成 `bundle_manifest.json`
- 输出一个可直接拷贝到目标机器的发布目录

### 方式 B：手动准备 `vendor/`

适合你想完全控制目录结构，或者已经有自己的发布流程。

脚本：

- `tools/prepare_vendor.py`

它只负责把离线依赖解包到指定 `vendor/` 目录，不会帮你复制整个项目。

---

## ✅ 离线部署前提

离线依赖想稳定运行，至少要满足这几个前提：

- 🖥️ **操作系统一致**：Windows 包给 Windows 用，Linux 包给 Linux 用
- 🐍 **Python 主次版本一致**：比如准备机用 `Python 3.9`，目标机最好也用 `Python 3.9`
- 🧱 **CPU 架构一致**：例如 `x86_64`
- 🔐 **如果配置启用了加密**，目标机器必须能使用 `cryptography`，并且要带上 `.config.key`

> [!TIP]
> 最稳的做法是：  
> **Windows 包在 Windows 上准备，CentOS 7.9 包在 CentOS 7.9 上准备。**

---

## 🚀 方式 A：使用 `build_offline_bundle.py`

### 1. 交互式构建

```bash
python build_offline_bundle.py
```

脚本会提示你选择：

- `windows`
- `linux`

也会提示你确认目标 Python 标签，例如：

- `py39`
- `py310`

### 2. 非交互构建示例

#### Windows

```bash
python build_offline_bundle.py --platform windows --python-tag py39
```

#### Linux / CentOS 7.9

```bash
python build_offline_bundle.py --platform linux --python-tag py39 --strict-centos7
```

### 3. 常用参数

| 参数 | 说明 |
| --- | --- |
| `--platform` | 目标平台：`windows` / `linux` |
| `--python-tag` | 目标 Python 标签，如 `py39` |
| `--machine` | 目标架构，默认 `x86_64` |
| `--output` | 自定义输出目录 |
| `--source` | 依赖归档目录，默认 `lib` |
| `--strict-centos7` | Linux 下严格检查 CentOS 7.9 兼容性 |
| `--no-strict-centos7` | 关闭 CentOS 7.9 严格检查 |

### 4. 构建产物示例

生成后目录大致如下：

```text
dist/
└── obs_sync_check_windows_x86_64_py39/
    ├── obs_migrate.py
    ├── bootstrap_runtime.py
    ├── config.example.ini
    ├── README.md
    ├── OFFLINE_DEPLOY.md
    ├── bundle_manifest.json
    ├── run_obs_migrate.bat
    ├── core/
    └── vendor/
        └── windows-x86_64-py39/
```

Linux 版本类似，只是启动脚本变成 `run_obs_migrate.sh`。

### 5. 目标机器怎么运行

把整个发布目录拷到目标机器后，直接执行：

#### Windows

```powershell
python obs_migrate.py
```

或者：

```powershell
run_obs_migrate.bat
```

#### Linux

```bash
python3 obs_migrate.py
```

或者：

```bash
./run_obs_migrate.sh
```

> [!NOTE]
> 离线发布目录**不包含 Python 解释器本身**。  
> 目标机器仍然需要预先安装一个兼容版本的 Python。

---

## 🧰 方式 B：手动准备 `vendor/`

如果你不想生成完整发布目录，也可以只准备依赖目录。

### Windows 示例

```powershell
cd E:\PythonProject\obs_sync_check
python tools\prepare_vendor.py --clean --tag windows-x86_64-py39
```

准备完成后，把整个项目目录复制到目标 Windows 机器即可。

如果需要手动指定依赖目录：

```powershell
$env:OBS_MIGRATE_VENDOR = ".\vendor\windows-x86_64-py39"
python .\obs_migrate.py
```

### Linux / CentOS 7.9 示例

```bash
cd /path/to/obs_sync_check
python3 tools/prepare_vendor.py --clean --tag linux-x86_64-py39 --strict-centos7
```

如果需要手动指定依赖目录：

```bash
export OBS_MIGRATE_VENDOR=./vendor/linux-x86_64-py39
python3 obs_migrate.py
```

### `prepare_vendor.py` 常用参数

| 参数 | 说明 |
| --- | --- |
| `--source` | 离线依赖归档目录，默认 `lib` |
| `--vendor-root` | `vendor` 根目录，默认 `vendor` |
| `--tag` | 目标目录名，如 `linux-x86_64-py39` |
| `--platform` | 目标平台：`windows` / `linux` |
| `--machine` | 目标架构 |
| `--python-tag` | 目标 Python 标签，如 `py39` |
| `--clean` | 先删除旧目录再重新解包 |
| `--strict-centos7` | Linux 下严格检查 CentOS 7.9 兼容性 |

---

## 🧠 运行时是怎么找到离线依赖的

项目启动时会先执行：

- `bootstrap_runtime.py`

它会把本地 `vendor/` 目录注入到 `sys.path`。

默认搜索顺序大致是：

1. 环境变量 `OBS_MIGRATE_VENDOR` 指定的目录
2. `vendor/<system>-<machine>-<python-tag>`
3. `vendor/<system>-<machine>`
4. `vendor/<system>-<python-tag>`
5. `vendor/<system>`
6. `vendor/common`

例如在 `Windows + Python 3.9 + x86_64` 下，优先会找：

```text
vendor/windows-x86_64-py39
```

如果你想完全手动接管，直接设置环境变量就行：

#### Windows

```powershell
$env:OBS_MIGRATE_VENDOR = "D:\deploy\obs_sync_check\vendor\windows-x86_64-py39"
python obs_migrate.py
```

#### Linux

```bash
export OBS_MIGRATE_VENDOR=/opt/obs_sync_check/vendor/linux-x86_64-py39
python3 obs_migrate.py
```

---

## 🐧 CentOS 7.9 重点注意事项

CentOS 7.9 的 `glibc` 通常是：

- `2.17`

所以 Linux 二进制依赖要尽量选：

- `manylinux2014`
- `manylinux_2_17`

不建议直接使用这些标签更新于 `glibc 2.17` 之后的 wheel，比如：

- `manylinux_2_28`
- `manylinux_2_34`

### 为什么要特别注意

有些 wheel 在新 Linux 上可以运行，但在 CentOS 7.9 上会直接导入失败，常见报错包括：

- `GLIBC_2.28 not found`
- `GLIBC_2.34 not found`
- 启动时 `ImportError`

### 典型风险包

如果你的 `lib/` 里有类似：

```text
cryptography-46.0.7-cp38-abi3-manylinux_2_34_x86_64.whl
```

那它**很可能不适合 CentOS 7.9**。

### 正确做法

给 CentOS 7.9 准备离线包时，优先使用：

- 与目标 Python 版本匹配的 wheel
- 标签为 `manylinux2014` 或 `manylinux_2_17` 的 wheel

并建议直接启用：

```bash
python3 tools/prepare_vendor.py --clean --tag linux-x86_64-py39 --strict-centos7
```

或者：

```bash
python build_offline_bundle.py --platform linux --python-tag py39 --strict-centos7
```

这样遇到明显不兼容的 wheel，会直接停止，而不是带着隐患发版。

---

## 🔑 哪些依赖最关键

当前项目依赖：

```text
esdk-obs-python
chardet
rich
cryptography
colorama
```

### 关键性说明

| 依赖 | 作用 | 是否关键 |
| --- | --- | --- |
| `esdk-obs-python` | 核心对象存储 SDK | 必须 |
| `chardet` | 编码识别辅助 | 建议保留 |
| `rich` | 仪表盘与终端渲染 | 建议保留 |
| `cryptography` | 加解密 `ak/sk` | 加密配置场景必须 |
| `colorama` | Windows 终端颜色支持 | 建议保留 |

### 关于 `cryptography`

如果出现下面任一情况，目标机器就**必须**能正常导入 `cryptography`：

- `config.ini` 里的 `ak/sk` 是加密值
- 目标机器上还要交互修改配置并重新加密保存

如果只是完全固定的明文测试配置，`cryptography` 的刚性没那么高；  
但从安全角度，**仍然建议准备好它的离线包**。

---

## 🔐 离线环境下的配置文件建议

推荐做法：

- 仓库里只放 `config.example.ini`
- 真正的 `config.ini` 放在仓库外
- `.config.key` 与真实配置文件放在同目录
- 用环境变量 `OBS_MIGRATE_CONFIG` 指向真实配置

例如：

#### Windows

```powershell
$env:OBS_MIGRATE_CONFIG = 'D:\secure\obs_sync_check\config.ini'
python obs_migrate.py
```

#### Linux

```bash
export OBS_MIGRATE_CONFIG=/opt/secure/obs_sync_check/config.ini
python3 obs_migrate.py
```

---

## 🧪 常见问题排查

### 1. 启动时报 `ImportError`

优先检查：

- 目标机器 Python 版本是否和离线包匹配
- `vendor` 目录标签是否正确
- 是否误用了 Windows 包到 Linux，或者 Linux 包到 Windows
- 是否需要显式设置 `OBS_MIGRATE_VENDOR`

### 2. 能启动，但解密配置失败

优先检查：

- `.config.key` 是否和 `config.ini` 配套
- 是否把密钥文件一起带到了目标机器
- `cryptography` 是否可正常导入

### 3. Linux 上提示 `glibc` 版本过低

说明 wheel 太新了，重新准备：

- `manylinux2014`
- `manylinux_2_17`

并建议开启 `--strict-centos7`。

### 4. 目标机器找不到 `vendor`

可以手动指定：

#### Windows

```powershell
$env:OBS_MIGRATE_VENDOR = ".\vendor\windows-x86_64-py39"
```

#### Linux

```bash
export OBS_MIGRATE_VENDOR=./vendor/linux-x86_64-py39
```

---

## 🗂️ 推荐发布方式

如果后续你要长期在纯离线环境发版，推荐按平台分别出包：

### Windows 发布包

```text
obs_sync_check_windows_py39.zip
```

建议包含：

- 源码
- `vendor/windows-x86_64-py39`
- `config.example.ini`
- `README.md`
- `OFFLINE_DEPLOY.md`

### CentOS 7.9 发布包

```text
obs_sync_check_centos7_py39.tar.gz
```

建议包含：

- 源码
- `vendor/linux-x86_64-py39`
- `config.example.ini`
- `README.md`
- `OFFLINE_DEPLOY.md`

---

## 🏁 一句话总结

离线环境下，最稳的方案不是让目标机器临时装依赖，而是：

> **在同平台准备机上把依赖整理进 `vendor/`，再把源码和依赖一起发到目标机器。**

---

## 💡 相关文件

- `README.md`：功能概览与使用说明
- `build_offline_bundle.py`：自动构建离线发布目录
- `tools/prepare_vendor.py`：手动准备 `vendor/`
- `bootstrap_runtime.py`：运行时自动加载本地离线依赖


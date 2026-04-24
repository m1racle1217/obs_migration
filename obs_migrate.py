#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""提供 OBS / S3 兼容对象存储迁移工具的命令行入口。"""

import configparser
import logging
import os
import queue
import shutil
import sys
import threading
import unicodedata
from datetime import datetime

from bootstrap_runtime import bootstrap_local_deps

bootstrap_local_deps()

import core.uploader as uploader_module
try:
    from colorama import Fore, Style, init as colorama_init
except ImportError:
    # ================================
    # 提供无颜色回退类
    # ================================
    class _PlainColor:
        BLACK = ""
        BLUE = ""
        CYAN = ""
        GREEN = ""
        LIGHTBLACK_EX = ""
        MAGENTA = ""
        RED = ""
        RESET = ""
        RESET_ALL = ""
        WHITE = ""
        YELLOW = ""

    Fore = _PlainColor()
    Style = _PlainColor()

    # ================================
    # 提供空实现的颜色初始化
    # ================================
    def colorama_init(*args, **kwargs):
        return None

from core import (
    AdaptiveScanController,
    Checkpoint,
    Dashboard,
    OBSUploader,
    Progress,
    Reporter,
    Scheduler,
    TaskChecker,
    TaskTransfer,
    init_source_client,
    init_target,
    scan_directory,
    scan_s3_objects,
)
from core.obs_index import build_obs_index
from core.utils import build_object_uri, detect_storage_scheme, parse_size, sanitize_key, setup_logger

colorama_init(autoreset=True)


CONFIG_FILE = "config.ini"
CONFIG_ENV_VAR = "OBS_MIGRATE_CONFIG"
KEY_FILE = ".config.key"
APP_DIR = os.path.dirname(os.path.abspath(__file__))

SOURCE_SECTION = "SOURCE"
TARGET_SECTION = "TARGET"
LEGACY_TARGET_SECTION = "OBS"
LEGACY_TASK_SECTION = "TASK"

MODE_LOCAL = "local"
MODE_S3 = "s3"

SENSITIVE_FIELDS = {
    (SOURCE_SECTION, "ak"),
    (SOURCE_SECTION, "sk"),
    (TARGET_SECTION, "ak"),
    (TARGET_SECTION, "sk"),
}

CONFIG_DESC = {
    "SOURCE.type": "源端模式：local 或 s3",
    "SOURCE.path": "源端本地目录或单文件路径（source.type=local 时使用）",
    "SOURCE.ak": "源端对象存储 AccessKey（source.type=s3 时使用）",
    "SOURCE.sk": "源端对象存储 SecretKey（source.type=s3 时使用）",
    "SOURCE.endpoint": "源端对象存储 Endpoint（source.type=s3 时使用，支持 OBS / 其他 S3 兼容服务）",
    "SOURCE.bucket": "源端桶名称（source.type=s3 时使用）",
    "SOURCE.prefix": "源端前缀（可为空，source.type=s3 时使用）",
    "TARGET.type": "目标端模式：local 或 s3",
    "TARGET.path": "目标端本地根目录（target.type=local 时使用）",
    "TARGET.ak": "目标端对象存储 AccessKey（target.type=s3 时使用）",
    "TARGET.sk": "目标端对象存储 SecretKey（target.type=s3 时使用）",
    "TARGET.endpoint": "目标端对象存储 Endpoint（target.type=s3 时使用，支持 OBS / 其他 S3 兼容服务）",
    "TARGET.bucket": "目标端桶名称（target.type=s3 时使用）",
    "TARGET.prefix": "目标端前缀（可为空，target.type=s3 时使用）",
    "UPLOAD.workers": "传输并发线程数（上传 / 下载 / 复制，推荐 16-64）",
    "UPLOAD.checkers": "检查阶段并发线程数（扫描后、传输前的存在性判断）",
    "UPLOAD.part_size": "分片大小（例如 64M / 128M）",
    "UPLOAD.multipart_threshold": "超过该大小启用分片传输",
    "UPLOAD.retry": "任务级失败重试次数",
    "UPLOAD.rate_limit": "目标端 API 基础 QPS 限制（0 表示不限制）",
    "UPLOAD.rate_limit_burst": "目标端 API 突发 QPS 上限（建议大于等于 rate_limit）",
    "UPLOAD.low_level_retries": "底层请求重试次数（HEAD / copy / multipart 等）",
    "UPLOAD.low_level_retry_sleep": "底层请求重试基础等待秒数",
    "UPLOAD.max_connections": "最大网络连接数上限（0 表示不限制）",
    "UPLOAD.multipart_concurrency": "单个大文件的分片并发数",
    "UPLOAD.max_buffer_memory": "分片流式缓冲总预算（例如 512M，0 表示不限制）",
    "UPLOAD.request_timeout": "单次请求超时秒数",
    "UPLOAD.worker_stall_timeout": "worker 无心跳判定秒数（用于卡死探测）",
    "SCAN.scan_workers": "扫描线程数上限（本地/对象存储通用，会按队列压力自适应调整）",
    "SCAN.batch_size": "单批扫描入队数量",
    "SCAN.queue_size": "检查队列与传输队列的最大长度",
    "CHECK.enable_etag_check": "上传前是否启用 ETAG 比对",
    "CHECK.enable_head_check": "上传前是否启用 HEAD 校验",
    "CHECK.strict_client_check": "客户端未初始化时是否直接报错退出",
    "CHECK.target_compare_mode": "目标端比较模式：auto / hybrid / head_only",
    "CHECK.verify_after_upload": "传输后校验模式：none / size / etag / head",
    "PATH.log_dir": "日志目录（相对配置文件目录解析）",
    "PATH.state_dir": "断点数据库目录（tasks.db 会写到这里）",
    "PATH.failed_dir": "失败任务目录（失败明细 / 补偿任务）",
    "UI.prompt_config": "启动时是否允许交互修改配置（支持直接输入编号修改）",
    "UI.show_dashboard": "是否显示实时仪表盘",
}

SECTION_TITLES = {
    SOURCE_SECTION: "源端配置",
    TARGET_SECTION: "目标端配置",
    "UPLOAD": "上传器配置",
    "SCAN": "扫描器配置",
    "CHECK": "校验器配置",
    "PATH": "运行目录配置",
    "UI": "界面配置",
}

REMOTE_ENDPOINT_KEYS = ("ak", "sk", "endpoint", "bucket", "prefix")

CONFIG_MENU_GROUPS = [
    {"id": "source", "title": "源端配置", "sections": [SOURCE_SECTION]},
    {"id": "target", "title": "目标端配置", "sections": [TARGET_SECTION]},
    {"id": "scanner", "title": "扫描器配置", "sections": ["SCAN"]},
    {
        "id": "transfer",
        "title": "传输器配置",
        "sections": ["UPLOAD"],
        "keys": [
            "workers",
            "part_size",
            "multipart_threshold",
            "retry",
            "multipart_concurrency",
            "request_timeout",
        ],
    },
    {
        "id": "scheduler",
        "title": "调度器配置",
        "sections": ["UPLOAD"],
        "keys": [
            "checkers",
            "rate_limit",
            "rate_limit_burst",
            "low_level_retries",
            "low_level_retry_sleep",
            "max_connections",
            "max_buffer_memory",
            "worker_stall_timeout",
        ],
    },
    {"id": "check", "title": "校验与比对配置", "sections": ["CHECK"]},
    {"id": "path", "title": "运行目录配置", "sections": ["PATH"]},
    {"id": "ui", "title": "UI 界面配置", "sections": ["UI"]},
]


# ================================
# 构建菜单分组索引
# ================================
def _build_config_menu_group_index(groups):
    index = {}
    for item in groups:
        group_id = item.get("id")
        if not group_id:
            continue
        index[group_id] = item
    return index


CONFIG_MENU_GROUP_INDEX = _build_config_menu_group_index(CONFIG_MENU_GROUPS)

DEFAULT_CONFIG = {
    SOURCE_SECTION: {
        "type": MODE_LOCAL,
        "path": "",
        "ak": "",
        "sk": "",
        "endpoint": "",
        "bucket": "",
        "prefix": "",
    },
    TARGET_SECTION: {
        "type": MODE_S3,
        "path": "",
        "ak": "",
        "sk": "",
        "endpoint": "",
        "bucket": "",
        "prefix": "",
    },
    "UPLOAD": {
        "workers": "32",
        "checkers": "16",
        "part_size": "64M",
        "multipart_threshold": "128M",
        "retry": "3",
        "rate_limit": "200",
        "rate_limit_burst": "400",
        "low_level_retries": "5",
        "low_level_retry_sleep": "0.5",
        "max_connections": "256",
        "multipart_concurrency": "4",
        "max_buffer_memory": "512M",
        "request_timeout": "60",
        "worker_stall_timeout": "300",
    },
    "SCAN": {
        "batch_size": "1000",
        "queue_size": "20000",
        "scan_workers": "4",
    },
    "CHECK": {
        "enable_etag_check": "false",
        "enable_head_check": "true",
        "strict_client_check": "true",
        "target_compare_mode": "auto",
        "verify_after_upload": "head",
    },
    "PATH": {
        "log_dir": "./logs",
        "state_dir": "./state",
        "failed_dir": "./failed",
    },
    "UI": {
        "prompt_config": "true",
        "show_dashboard": "true",
    },
}


# ================================
# 定位配置文件
# ================================
def resolve_config_file():
    config_from_env = (os.getenv(CONFIG_ENV_VAR) or "").strip()
    if config_from_env:
        return os.path.abspath(os.path.expanduser(config_from_env))
    if os.path.isabs(CONFIG_FILE):
        return CONFIG_FILE
    return os.path.join(APP_DIR, CONFIG_FILE)


# ================================
# 获取配置基目录
# ================================
def config_base_dir():
    return os.path.dirname(os.path.abspath(resolve_config_file()))


# ================================
# 定位密钥文件
# ================================
def resolve_key_file():
    if os.path.isabs(KEY_FILE):
        return KEY_FILE
    return os.path.join(config_base_dir(), KEY_FILE)


# ================================
# 解析运行期目录
# ================================
def resolve_runtime_path(path_value):
    raw_value = (path_value or "").strip()
    if not raw_value:
        return config_base_dir()
    if os.path.isabs(raw_value):
        return raw_value
    return os.path.abspath(os.path.join(config_base_dir(), raw_value))


_cipher = None
_fernet_cls = None
_fernet_import_error = None


# ================================
# 按需加载 Fernet
# ================================
def _load_fernet_class(required=False):
    global _fernet_cls, _fernet_import_error

    if _fernet_cls is not None:
        return _fernet_cls

    if _fernet_import_error is not None:
        if required:
            raise RuntimeError(
                "encrypted config requires cryptography; please prepare vendor dependencies or use trusted plaintext config"
            ) from _fernet_import_error
        return None

    try:
        from cryptography.fernet import Fernet
    except Exception as exc:
        _fernet_import_error = exc
        if required:
            raise RuntimeError(
                "encrypted config requires cryptography; please prepare vendor dependencies or use trusted plaintext config"
            ) from exc
        return None

    _fernet_cls = Fernet
    return _fernet_cls


# ================================
# 加载加密器
# ================================
def load_cipher(required=False):
    global _cipher

    if _cipher is not None:
        return _cipher

    fernet_cls = _load_fernet_class(required=required)
    if fernet_cls is None:
        return None

    key_file = resolve_key_file()

    if not os.path.exists(key_file):
        key = fernet_cls.generate_key()
        with open(key_file, "wb") as f:
            f.write(key)
    else:
        with open(key_file, "rb") as f:
            key = f.read()

    _cipher = fernet_cls(key)
    return _cipher


# ================================
# 加密敏感值
# ================================
def encrypt_value(value):
    return load_cipher(required=True).encrypt(value.encode()).decode()


# ================================
# 解密敏感值
# ================================
def decrypt_value(value):
    if not value:
        return ""

    if not value.startswith("gAAAA"):
        return value

    cipher = load_cipher(required=False)
    if cipher is None:
        raise RuntimeError(
            "encrypted config detected but cryptography is unavailable; please prepare vendor dependencies and .config.key"
        )

    try:
        return cipher.decrypt(value.encode()).decode()
    except Exception as exc:
        raise RuntimeError(
            "failed to decrypt sensitive config value; please verify .config.key matches the config"
        ) from exc


# ================================
# 脱敏显示
# ================================
def mask_secret(value):
    if not value:
        return ""
    return "*" * 8


# ================================
# 交互读取输入
# ================================
def _read_input(prompt):
    if not (
        sys.stdin
        and sys.stdout
        and hasattr(sys.stdin, "isatty")
        and hasattr(sys.stdout, "isatty")
        and sys.stdin.isatty()
        and sys.stdout.isatty()
    ):
        return input(prompt)

    if os.name == "nt":
        return _read_input_windows(prompt)

    return _read_input_posix(prompt)


# ================================
# Windows 交互读取输入
# ================================
def _read_input_windows(prompt):
    import msvcrt

    sys.stdout.write(prompt)
    sys.stdout.flush()
    chars = []

    while True:
        ch = msvcrt.getwch()

        if ch in {"\r", "\n"}:
            sys.stdout.write("\n")
            sys.stdout.flush()
            return "".join(chars)

        if ch == "\x03":
            raise KeyboardInterrupt

        if ch in {"\b", "\x7f"}:
            if chars:
                chars.pop()
                sys.stdout.write("\b \b")
                sys.stdout.flush()
            continue

        if ch in {"\x00", "\xe0"}:
            msvcrt.getwch()
            continue

        if ch.isprintable():
            chars.append(ch)
            sys.stdout.write(ch)
            sys.stdout.flush()


# ================================
# Linux / Unix 交互读取输入
# ================================
def _read_input_posix(prompt):
    import termios
    import tty

    fd = sys.stdin.fileno()
    original = termios.tcgetattr(fd)
    chars = []

    sys.stdout.write(prompt)
    sys.stdout.flush()

    try:
        tty.setraw(fd)
        while True:
            ch = sys.stdin.read(1)

            if ch in {"\r", "\n"}:
                sys.stdout.write("\n")
                sys.stdout.flush()
                return "".join(chars)

            if ch == "\x03":
                raise KeyboardInterrupt

            if ch in {"\b", "\x7f"}:
                if chars:
                    chars.pop()
                    sys.stdout.write("\b \b")
                    sys.stdout.flush()
                continue

            if ch == "\x1b":
                continue

            if ch.isprintable():
                chars.append(ch)
                sys.stdout.write(ch)
                sys.stdout.flush()
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, original)


# ================================
# 创建运行目录
# ================================
def ensure_dirs():
    for directory in ("./logs", "./state", "./failed"):
        os.makedirs(resolve_runtime_path(directory), exist_ok=True)


# ================================
# 解析布尔环境变量
# ================================
def parse_env_bool(name):
    value = os.getenv(name)
    if value is None:
        return None

    value = value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return None


# ================================
# 判断是否允许交互改配置
# ================================
def should_prompt_config(cfg=None):
    env_value = parse_env_bool("OBS_MIGRATE_INTERACTIVE")
    if env_value is not None:
        return env_value

    if os.getenv("CI"):
        return False

    if cfg is not None:
        return cfg.getboolean("UI", "prompt_config", fallback=True)

    return True


# ================================
# 判断是否启用仪表盘
# ================================
def should_enable_dashboard(cfg=None):
    env_value = parse_env_bool("OBS_MIGRATE_DASHBOARD")
    if env_value is not None:
        return env_value

    if os.getenv("CI"):
        return False

    if cfg is not None:
        return cfg.getboolean("UI", "show_dashboard", fallback=True)

    return True


# ================================
# 判断是否强制终端渲染
# ================================
def should_force_terminal():
    env_value = parse_env_bool("OBS_MIGRATE_FORCE_TERMINAL")
    if env_value is not None:
        return env_value

    if os.getenv("CI"):
        return False

    return os.name == "nt" or os.getenv("PYCHARM_HOSTED") == "1"


# ================================
# 计算本地扫描线程数
# ================================
def resolve_scan_workers(requested):
    requested = max(1, int(requested or 1))
    cpu_count = os.cpu_count() or 4
    recommended = max(4, min(64, cpu_count * 4))
    return max(1, min(requested, recommended))


# ================================
# 计算远端扫描线程数
# ================================
def resolve_remote_scan_workers(requested):
    return max(1, min(int(requested), 128))


# ================================
# 计算最小扫描线程数
# ================================
def resolve_min_scan_workers(requested):
    requested = max(1, int(requested or 1))
    return max(1, min(4, requested // 8 or 1))


# ================================
# 判断是否为敏感字段
# ================================
def _is_sensitive(section, key):
    return (section, key) in SENSITIVE_FIELDS


# ================================
# 归一化模式输入
# ================================
def _normalize_mode(value, default=None):
    if value is None:
        return default

    text = str(value).strip().lower()
    if not text:
        return default

    mapping = {
        "1": MODE_LOCAL,
        "local": MODE_LOCAL,
        "2": MODE_S3,
        "s3": MODE_S3,
    }
    return mapping.get(text)


# ================================
# 交互选择模式
# ================================
def _prompt_mode(section_label, current_value, allow_empty=False):
    current_value = _normalize_mode(current_value, default=MODE_LOCAL)

    while True:
        print(f"\n请选择 {section_label} 模式：")
        print("1. local")
        print("2. s3")
        raw = _read_input(f"{section_label}.type [{current_value}]: ").strip()

        if not raw and allow_empty:
            return current_value

        normalized = _normalize_mode(raw, default=current_value if allow_empty else None)
        if normalized in {MODE_LOCAL, MODE_S3}:
            return normalized

        print("请输入 local / s3，或者输入 1 / 2。")


# ================================
# 按需加密配置值
# ================================
def _maybe_encrypt_for_store(section, key, value):
    if not value:
        return value
    if not _is_sensitive(section, key):
        return value
    if value.startswith("gAAAA"):
        return value
    return encrypt_value(value)


# ================================
# 从配置中解密取值
# ================================
def _decrypt_from_config(cfg, section, key):
    return decrypt_value(cfg.get(section, key, fallback="").strip())


# ================================
# 获取有序配置分组
# ================================
def _ordered_sections(cfg):
    ordered = []
    for section in DEFAULT_CONFIG:
        if cfg.has_section(section):
            ordered.append(section)

    for section in cfg.sections():
        if section not in ordered:
            ordered.append(section)

    return ordered


# ================================
# 获取配置分组标题
# ================================
def _section_title(section):
    return SECTION_TITLES.get(section, section)


# ================================
# 获取当前模式下隐藏的配置键
# ================================
def _hidden_keys_for_section(cfg, section):
    if section not in {SOURCE_SECTION, TARGET_SECTION} or not cfg.has_section(section):
        return set()

    default_mode = MODE_LOCAL if section == SOURCE_SECTION else MODE_S3
    current_mode = _normalize_mode(cfg.get(section, "type", fallback=default_mode), default=default_mode)

    if current_mode == MODE_LOCAL:
        return {key for key in REMOTE_ENDPOINT_KEYS if key in cfg[section]}

    if current_mode == MODE_S3:
        return {"path"} if "path" in cfg[section] else set()

    return set()


# ================================
# 获取当前展示的配置项
# ================================
def _visible_items_for_section(cfg, section):
    hidden_keys = _hidden_keys_for_section(cfg, section)
    return [(key, value) for key, value in cfg[section].items() if key not in hidden_keys]


# ================================
# 生成配置分组说明
# ================================
def _section_display_hint(cfg, section):
    if section not in {SOURCE_SECTION, TARGET_SECTION} or not cfg.has_section(section):
        return ""

    default_mode = MODE_LOCAL if section == SOURCE_SECTION else MODE_S3
    current_mode = _normalize_mode(cfg.get(section, "type", fallback=default_mode), default=default_mode)
    hidden_count = len(_hidden_keys_for_section(cfg, section))

    if hidden_count > 0:
        return f"当前模式：{current_mode}，仅显示生效项，已折叠 {hidden_count} 项"

    return f"当前模式：{current_mode}"


# ================================
# 获取菜单分组定义
# ================================
def _get_config_menu_group(group_id):
    return CONFIG_MENU_GROUP_INDEX[group_id]


# ================================
# 计算显示宽度
# ================================
def _display_width(text):
    width = 0
    for char in str(text or ""):
        if unicodedata.combining(char):
            continue
        width += 2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1
    return width


# ================================
# 按显示宽度截断文本
# ================================
def _truncate_display(text, max_width):
    text = str(text or "")
    if max_width <= 0:
        return ""

    chars = []
    width = 0
    for char in text:
        char_width = 0 if unicodedata.combining(char) else (2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1)
        if width + char_width > max_width:
            break
        chars.append(char)
        width += char_width
    return "".join(chars)


# ================================
# 从尾部按显示宽度截断文本
# ================================
def _truncate_display_from_end(text, max_width):
    text = str(text or "")
    if max_width <= 0:
        return ""

    chars = []
    width = 0
    for char in reversed(text):
        char_width = 0 if unicodedata.combining(char) else (2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1)
        if width + char_width > max_width:
            break
        chars.append(char)
        width += char_width
    return "".join(reversed(chars))


# ================================
# 截断过长文本
# ================================
def _shorten_text(text, max_length=48):
    text = str(text or "").strip()
    if not text:
        return "-"

    if _display_width(text) <= max_length:
        return text

    if max_length <= 4:
        return _truncate_display(text, max_length)

    keep = max_length - 1
    head_width = max(1, keep // 2)
    tail_width = max(1, keep - head_width)
    head_text = _truncate_display(text, head_width)
    tail_text = _truncate_display_from_end(text, tail_width)
    return f"{head_text}…{tail_text}"


# ================================
# 适配文本宽度
# ================================
def _fit_display(text, width):
    normalized = _truncate_display(text, width)
    return normalized + " " * max(width - _display_width(normalized), 0)


# ================================
# 格式化布尔值
# ================================
def _format_bool_text(value):
    return "true" if str(value).strip().lower() in {"1", "true", "yes", "on"} else "false"


# ================================
# 获取终端菜单宽度
# ================================
def _menu_terminal_width():
    try:
        columns = shutil.get_terminal_size((88, 20)).columns
    except Exception:
        columns = 88
    return max(56, min(columns - 2 if columns > 2 else columns, 104))


# ================================
# 解析盒子宽度
# ================================
def _resolve_box_width(title, lines):
    content_width = _display_width(title) + 2
    for line in lines:
        content_width = max(content_width, _display_width(line))

    desired = max(content_width + 4, 56)
    return min(desired, _menu_terminal_width())


# ================================
# 构建盒子边框
# ================================
def _box_border(title, width, top=True):
    inside_width = max(width - 2, 0)
    label = f" {title} "
    trimmed = _truncate_display(label, inside_width)
    fill = "─" * max(inside_width - _display_width(trimmed), 0)
    if top:
        return f"╭{trimmed}{fill}╮"
    return f"╰{'─' * inside_width}╯"


# ================================
# 构建盒子分隔线
# ================================
def _box_separator(width):
    inside_width = max(width - 2, 0)
    return f"├{'─' * inside_width}┤"


# ================================
# 构建盒子内容行
# ================================
def _box_line(text, width):
    inside_width = max(width - 4, 0)
    return f"│ {_fit_display(text, inside_width)} │"


# ================================
# 渲染文本盒子
# ================================
def _print_box(title, body_lines, footer_lines=None, subtitle_lines=None):
    lines = []
    if subtitle_lines:
        lines.extend(subtitle_lines)
    lines.extend(body_lines)
    if footer_lines:
        lines.extend(footer_lines)

    width = _resolve_box_width(title, lines)
    print(f"{Fore.CYAN}{_box_border(title, width, top=True)}{Style.RESET_ALL}")

    if subtitle_lines:
        for line in subtitle_lines:
            print(f"{Fore.LIGHTBLACK_EX}{_box_line(line, width)}{Style.RESET_ALL}")
        if body_lines or footer_lines:
            print(f"{Fore.CYAN}{_box_separator(width)}{Style.RESET_ALL}")

    for line in body_lines:
        print(f"{Fore.WHITE}{_box_line(line, width)}{Style.RESET_ALL}")

    if footer_lines:
        if body_lines:
            print(f"{Fore.CYAN}{_box_separator(width)}{Style.RESET_ALL}")
        for line in footer_lines:
            print(f"{Fore.GREEN}{_box_line(line, width)}{Style.RESET_ALL}")

    print(f"{Fore.CYAN}{_box_border(title, width, top=False)}{Style.RESET_ALL}")


# ================================
# 获取菜单分组内的配置项
# ================================
def _group_items(cfg, group_id):
    group = _get_config_menu_group(group_id)
    rows = []

    for section in group.get("sections", []):
        if not cfg.has_section(section):
            continue

        visible_map = dict(_visible_items_for_section(cfg, section))
        allowed_keys = group.get("keys")
        if allowed_keys:
            for key in allowed_keys:
                if key in visible_map:
                    rows.append((section, key, visible_map[key]))
            continue

        for key, value in _visible_items_for_section(cfg, section):
            rows.append((section, key, value))

    return rows


# ================================
# 生成存储端简要摘要
# ================================
def _storage_summary(mode, path_value, endpoint, bucket, prefix):
    normalized_mode = _normalize_mode(mode, default=MODE_LOCAL) or MODE_LOCAL
    if normalized_mode == MODE_LOCAL:
        return f"local | {_shorten_text(path_value or '-', 56)}"

    scheme = detect_storage_scheme(endpoint, fallback="s3")
    uri = build_object_uri(bucket, prefix, scheme=scheme)
    return f"{normalized_mode} | {_shorten_text(uri, 56)}"


# ================================
# 生成分组摘要
# ================================
def _group_summary(cfg, group_id):
    if group_id == "source":
        return _storage_summary(
            cfg.get(SOURCE_SECTION, "type", fallback=MODE_LOCAL),
            cfg.get(SOURCE_SECTION, "path", fallback=""),
            cfg.get(SOURCE_SECTION, "endpoint", fallback=""),
            cfg.get(SOURCE_SECTION, "bucket", fallback=""),
            cfg.get(SOURCE_SECTION, "prefix", fallback=""),
        )

    if group_id == "target":
        return _storage_summary(
            cfg.get(TARGET_SECTION, "type", fallback=MODE_S3),
            cfg.get(TARGET_SECTION, "path", fallback=""),
            cfg.get(TARGET_SECTION, "endpoint", fallback=""),
            cfg.get(TARGET_SECTION, "bucket", fallback=""),
            cfg.get(TARGET_SECTION, "prefix", fallback=""),
        )

    if group_id == "scanner":
        return (
            f"workers={cfg.get('SCAN', 'scan_workers', fallback='-')} | "
            f"batch={cfg.get('SCAN', 'batch_size', fallback='-')} | "
            f"queue={cfg.get('SCAN', 'queue_size', fallback='-')}"
        )

    if group_id == "transfer":
        return (
            f"workers={cfg.get('UPLOAD', 'workers', fallback='-')} | "
            f"part={cfg.get('UPLOAD', 'part_size', fallback='-')} | "
            f"threshold={cfg.get('UPLOAD', 'multipart_threshold', fallback='-')}"
        )

    if group_id == "scheduler":
        return (
            f"checkers={cfg.get('UPLOAD', 'checkers', fallback='-')} | "
            f"qps={cfg.get('UPLOAD', 'rate_limit', fallback='-')}/{cfg.get('UPLOAD', 'rate_limit_burst', fallback='-')} | "
            f"conn={cfg.get('UPLOAD', 'max_connections', fallback='-')}"
        )

    if group_id == "check":
        return (
            f"compare={cfg.get('CHECK', 'target_compare_mode', fallback='-')} | "
            f"verify={cfg.get('CHECK', 'verify_after_upload', fallback='-')} | "
            f"head={_format_bool_text(cfg.get('CHECK', 'enable_head_check', fallback='true'))}"
        )

    if group_id == "path":
        return (
            f"logs={_shorten_text(cfg.get('PATH', 'log_dir', fallback='-'), 18)} | "
            f"state={_shorten_text(cfg.get('PATH', 'state_dir', fallback='-'), 18)} | "
            f"failed={_shorten_text(cfg.get('PATH', 'failed_dir', fallback='-'), 18)}"
        )

    if group_id == "ui":
        return (
            f"prompt={_format_bool_text(cfg.get('UI', 'prompt_config', fallback='true'))} | "
            f"dashboard={_format_bool_text(cfg.get('UI', 'show_dashboard', fallback='true'))}"
        )

    return ""


# ================================
# 展示折叠菜单
# ================================
def show_config_menu(cfg):
    mapping = {}
    title_width = max(_display_width(group["title"]) for group in CONFIG_MENU_GROUPS) + 2
    body_lines = []
    for index, group in enumerate(CONFIG_MENU_GROUPS, start=1):
        title = group["title"]
        summary = _group_summary(cfg, group["id"])
        line = f"{index}. {_fit_display(f'[{title}]', title_width + 2)}  {_shorten_text(summary, 56)}"
        body_lines.append(line)
        mapping[str(index)] = group["id"]

    subtitle_lines = [
        f"配置文件：{_shorten_text(resolve_config_file(), 62)}",
        "提示：先选分组进入详情，修改后会自动保存。",
    ]
    footer_lines = ["[Y] 启动迁移程序    [Q] 退出程序"]
    print()
    _print_box("配置菜单", body_lines, footer_lines=footer_lines, subtitle_lines=subtitle_lines)
    return mapping


# ================================
# 展示单个分组详情
# ================================
def show_config_group(cfg, group_id):
    group = _get_config_menu_group(group_id)
    section_hint = ""

    if group_id == "source":
        section_hint = _section_display_hint(cfg, SOURCE_SECTION)
    elif group_id == "target":
        section_hint = _section_display_hint(cfg, TARGET_SECTION)

    mapping = {}
    body_lines = []
    for index, (section, key, value) in enumerate(_group_items(cfg, group_id), start=1):
        shown_value = mask_secret(value) if _is_sensitive(section, key) else value
        if key in {"rate_limit", "rate_limit_burst"} and shown_value not in {"", None}:
            shown_value = f"{shown_value} req/s"

        desc = CONFIG_DESC.get(f"{section}.{key}", "")
        if desc:
            body_lines.append(f"{index}. {desc}")
            body_lines.append(f"   └─ {key} = {shown_value}")
        else:
            body_lines.append(f"{index}. {key} = {shown_value}")

        mapping[str(index)] = (section, key)

    subtitle_lines = [_group_summary(cfg, group_id)]
    if section_hint:
        subtitle_lines.append(section_hint)

    footer_lines = ["[B] 返回上一级    [Y] 启动迁移程序    [Q] 退出程序"]
    print()
    _print_box(group["title"], body_lines, footer_lines=footer_lines, subtitle_lines=subtitle_lines)
    return mapping


# ================================
# 编辑单个分组
# ================================
def edit_config_group(cfg, group_id):
    while True:
        mapping = show_config_group(cfg, group_id)
        answer = _read_input("\n请选择编号，或输入 B/Y/Q: ").strip()
        lowered = answer.lower()

        if lowered == "b":
            return "back"

        if lowered == "y":
            return "start"

        if lowered == "q":
            return "quit"

        if answer not in mapping:
            print("请输入有效编号，或输入 B / Y / Q。")
            continue

        section, key = mapping[answer]
        desc = CONFIG_DESC.get(f"{section}.{key}", "")
        if desc:
            print(f"\n{desc}")

        if _is_sensitive(section, key):
            print(f"注意：敏感信息会以加密形式写入 {resolve_config_file()}")

        if key == "type" and section in {SOURCE_SECTION, TARGET_SECTION}:
            section_label = "source" if section == SOURCE_SECTION else "target"
            new_value = _prompt_mode(section_label, cfg.get(section, key, fallback=MODE_LOCAL), allow_empty=True)
        else:
            new_value = _read_input("新值: ").strip()

        cfg[section][key] = _maybe_encrypt_for_store(section, key, new_value)
        write_config_with_comments(cfg)
        print(f"\n已保存到 {resolve_config_file()}\n")


# ================================
# 运行折叠菜单交互
# ================================
def run_config_menu(cfg):
    while True:
        mapping = show_config_menu(cfg)
        answer = _read_input("\n请选择分组编号，或输入 Y/Q: ").strip()
        lowered = answer.lower()

        if lowered == "y":
            print()
            return cfg

        if lowered == "q":
            print("\n已退出。\n")
            raise SystemExit(0)

        group_id = mapping.get(answer)
        if group_id is None:
            print("请输入有效分组编号，或输入 Y / Q。")
            continue

        action = edit_config_group(cfg, group_id)
        if action == "start":
            print()
            return cfg
        if action == "quit":
            print("\n已退出。\n")
            raise SystemExit(0)


# ================================
# 生成源端标签
# ================================
def _source_label(source_type, source_path, source_bucket, source_prefix):
    if source_type == MODE_LOCAL:
        return source_path

    prefix = source_prefix.strip("/") or "_root_"
    return f"{source_bucket}/{prefix}"


# ================================
# 清洗名称用于文件命名
# ================================
def _sanitize_name(name):
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in name)
    return cleaned or "root"


# ================================
# 迁移旧版目标配置
# ================================
def _migrate_legacy_target_section(cfg):
    if not cfg.has_section(LEGACY_TARGET_SECTION):
        return False

    if not cfg.has_section(TARGET_SECTION):
        cfg.add_section(TARGET_SECTION)

    cfg.set(TARGET_SECTION, "type", cfg.get(TARGET_SECTION, "type", fallback=MODE_S3) or MODE_S3)
    for key in ("ak", "sk", "endpoint", "bucket"):
        if cfg.has_option(LEGACY_TARGET_SECTION, key) and not cfg.has_option(TARGET_SECTION, key):
            cfg.set(TARGET_SECTION, key, cfg.get(LEGACY_TARGET_SECTION, key))

    cfg.remove_section(LEGACY_TARGET_SECTION)
    return True


# ================================
# 迁移旧版任务配置
# ================================
def _migrate_legacy_task_section(cfg):
    if not cfg.has_section(LEGACY_TASK_SECTION):
        return False

    updated = False
    if not cfg.has_section(SOURCE_SECTION):
        cfg.add_section(SOURCE_SECTION)
        updated = True
    if not cfg.has_section(TARGET_SECTION):
        cfg.add_section(TARGET_SECTION)
        updated = True

    if cfg.has_option(LEGACY_TASK_SECTION, "local_dir") and not cfg.has_option(SOURCE_SECTION, "path"):
        cfg.set(SOURCE_SECTION, "path", cfg.get(LEGACY_TASK_SECTION, "local_dir"))
        if not cfg.has_option(SOURCE_SECTION, "type"):
            cfg.set(SOURCE_SECTION, "type", MODE_LOCAL)
        updated = True

    if cfg.has_option(LEGACY_TASK_SECTION, "obs_prefix") and not cfg.has_option(TARGET_SECTION, "prefix"):
        cfg.set(TARGET_SECTION, "prefix", cfg.get(LEGACY_TASK_SECTION, "obs_prefix"))
        if not cfg.has_option(TARGET_SECTION, "type"):
            cfg.set(TARGET_SECTION, "type", MODE_S3)
        updated = True

    cfg.remove_section(LEGACY_TASK_SECTION)
    return True or updated


# ================================
# 初始化配置
# ================================
def init_config():
    print("\n首次运行，初始化配置\n")

    cfg = configparser.ConfigParser()
    for section, items in DEFAULT_CONFIG.items():
        cfg[section] = {}
        for key, default_value in items.items():
            desc = CONFIG_DESC.get(f"{section}.{key}", "")
            if desc:
                print(f"\n{desc}")

            if _is_sensitive(section, key):
                print(f"注意：敏感信息会以加密形式写入 {resolve_config_file()}")

            if key == "type":
                section_label = "source" if section == SOURCE_SECTION else "target"
                value = _prompt_mode(section_label, default_value, allow_empty=True)
            else:
                value = _read_input(f"{key}: ").strip()
                if not value:
                    value = default_value

            cfg[section][key] = _maybe_encrypt_for_store(section, key, value)

    write_config_with_comments(cfg)
    print(f"\n配置文件已生成：{resolve_config_file()}\n")
    return cfg


# ================================
# 展示当前配置
# ================================
def show_config(cfg):
    print("\n当前配置（源端 / 目标端仅显示当前模式下会生效的配置项）\n")

    mapping = {}
    index = 1

    for section in _ordered_sections(cfg):
        title = _section_title(section)
        hint = _section_display_hint(cfg, section)
        if hint:
            print(f"{Fore.CYAN}[{title}]{Style.RESET_ALL} {Fore.MAGENTA}({hint}){Style.RESET_ALL}")
        else:
            print(f"{Fore.CYAN}[{title}]{Style.RESET_ALL}")

        for key, value in _visible_items_for_section(cfg, section):
            shown_value = mask_secret(value) if _is_sensitive(section, key) else value
            if key in {"rate_limit", "rate_limit_burst"} and shown_value not in {"", None}:
                shown_value = f"{shown_value} req/s"

            desc = CONFIG_DESC.get(f"{section}.{key}", "")
            if desc:
                print(f"{index}. {Fore.YELLOW}{desc}{Style.RESET_ALL}")
                print(f"    {Fore.GREEN}{key}{Style.RESET_ALL} = {shown_value}")
            else:
                print(f"{index}. {Fore.GREEN}{key}{Style.RESET_ALL} = {shown_value}")

            mapping[str(index)] = (section, key)
            index += 1

        print()

    return mapping


# ================================
# 交互修改配置
# ================================
def modify_config(cfg, initial_choice=None, mapping=None):
    if mapping is None:
        mapping = show_config(cfg)
    print("\n输入编号修改，q 退出\n")

    choice = initial_choice
    while True:
        if choice is None:
            choice = _read_input("选择编号: ").strip()
        if choice.lower() == "q":
            break
        if choice not in mapping:
            print("未找到该编号，请重新输入。")
            choice = None
            continue

        section, key = mapping[choice]
        desc = CONFIG_DESC.get(f"{section}.{key}", "")
        if desc:
            print(desc)

        if _is_sensitive(section, key):
            print(f"注意：敏感信息会以加密形式写入 {resolve_config_file()}")

        if key == "type" and section in {SOURCE_SECTION, TARGET_SECTION}:
            section_label = "source" if section == SOURCE_SECTION else "target"
            new_value = _prompt_mode(section_label, cfg.get(section, key, fallback=MODE_LOCAL), allow_empty=True)
        else:
            new_value = _read_input("新值: ").strip()

        cfg[section][key] = _maybe_encrypt_for_store(section, key, new_value)
        print("\n已更新，当前生效配置如下：")
        mapping = show_config(cfg)
        print("\n输入编号继续修改，q 退出\n")
        choice = None

    write_config_with_comments(cfg)
    print("\n配置已更新\n")


# ================================
# 获取配置操作输入
# ================================
def _prompt_config_action(mapping):
    while True:
        answer = _read_input("\n是否修改配置? (y/N，或直接输入编号): ").strip()
        lowered = answer.lower()

        if not answer or lowered in {"n", "no"}:
            return None

        if lowered in {"y", "yes"}:
            return "modify"

        if answer in mapping:
            return answer

        print("请输入 y / n，或直接输入上面的配置编号。")


# ================================
# 加载配置
# ================================
def load_config():
    config_file = resolve_config_file()
    if not os.path.exists(config_file):
        return init_config()

    cfg = configparser.ConfigParser()
    cfg.read(config_file, encoding="utf-8")

    updated = False
    if _migrate_legacy_target_section(cfg):
        updated = True
    if _migrate_legacy_task_section(cfg):
        updated = True

    for section, items in DEFAULT_CONFIG.items():
        if not cfg.has_section(section):
            cfg.add_section(section)
            updated = True

        for key, default_value in items.items():
            if not cfg.has_option(section, key):
                cfg.set(section, key, default_value)
                updated = True

    for section, default_mode in ((SOURCE_SECTION, MODE_LOCAL), (TARGET_SECTION, MODE_S3)):
        raw_mode = cfg.get(section, "type", fallback=default_mode)
        normalized = _normalize_mode(raw_mode, default=default_mode)
        if normalized is None:
            normalized = default_mode
        if raw_mode != normalized:
            cfg.set(section, "type", normalized)
            updated = True

    if updated:
        write_config_with_comments(cfg)
        print(f"\n检测到新配置项，已自动更新 {resolve_config_file()}\n")

    if should_prompt_config(cfg):
        run_config_menu(cfg)

    return cfg


# ================================
# 写回带注释的配置
# ================================
def write_config_with_comments(cfg):
    with open(resolve_config_file(), "w", encoding="utf-8") as f:
        for section in _ordered_sections(cfg):
            f.write("# ------------------------------\n")
            f.write(f"# {section}\n")
            f.write("# ------------------------------\n")
            f.write(f"[{section}]\n\n")

            for key, value in cfg[section].items():
                desc = CONFIG_DESC.get(f"{section}.{key}", "")
                if desc:
                    f.write(f"# {desc}\n")
                f.write(f"{key} = {value}\n\n")

            f.write("\n")


# ================================
# 校验配置有效性
# ================================
def validate_config(cfg):
    source_type = _normalize_mode(cfg.get(SOURCE_SECTION, "type", fallback=MODE_LOCAL), default=MODE_LOCAL)
    target_type = _normalize_mode(cfg.get(TARGET_SECTION, "type", fallback=MODE_S3), default=MODE_S3)

    source_path = cfg.get(SOURCE_SECTION, "path", fallback="").strip()
    source_endpoint = cfg.get(SOURCE_SECTION, "endpoint", fallback="").strip()
    source_bucket = cfg.get(SOURCE_SECTION, "bucket", fallback="").strip()
    source_ak = cfg.get(SOURCE_SECTION, "ak", fallback="").strip()
    source_sk = cfg.get(SOURCE_SECTION, "sk", fallback="").strip()

    target_path = cfg.get(TARGET_SECTION, "path", fallback="").strip()
    target_endpoint = cfg.get(TARGET_SECTION, "endpoint", fallback="").strip()
    target_bucket = cfg.get(TARGET_SECTION, "bucket", fallback="").strip()
    target_ak = cfg.get(TARGET_SECTION, "ak", fallback="").strip()
    target_sk = cfg.get(TARGET_SECTION, "sk", fallback="").strip()

    if source_type not in {MODE_LOCAL, MODE_S3}:
        print("❌ SOURCE.type 仅支持 local 或 s3")
        sys.exit(1)

    if target_type not in {MODE_LOCAL, MODE_S3}:
        print("❌ TARGET.type 仅支持 local 或 s3")
        sys.exit(1)

    if source_type == MODE_LOCAL:
        if not source_path or not os.path.exists(source_path):
            print("❌ SOURCE.path 不存在")
            sys.exit(1)
    else:
        if not source_ak or not source_sk:
            print("❌ SOURCE.ak / SOURCE.sk 未配置")
            sys.exit(1)
        if not source_endpoint:
            print("❌ SOURCE.endpoint 未配置")
            sys.exit(1)
        if not source_bucket:
            print("❌ SOURCE.bucket 未配置")
            sys.exit(1)

    if target_type == MODE_LOCAL:
        if not target_path:
            print("❌ TARGET.path 未配置")
            sys.exit(1)
    else:
        if not target_ak or not target_sk:
            print("❌ TARGET.ak / TARGET.sk 未配置")
            sys.exit(1)
        if not target_endpoint:
            print("❌ TARGET.endpoint 未配置")
            sys.exit(1)
        if not target_bucket:
            print("❌ TARGET.bucket 未配置")
            sys.exit(1)

    numeric_fields = [
        ("UPLOAD", "workers"),
        ("UPLOAD", "retry"),
        ("UPLOAD", "rate_limit"),
        ("SCAN", "scan_workers"),
        ("SCAN", "queue_size"),
        ("SCAN", "batch_size"),
    ]
    for section, key in numeric_fields:
        value = cfg.getint(section, key, fallback=0)
        if value <= 0:
            print(f"❌ {section}.{key} 必须大于 0")
            sys.exit(1)

    size_fields = [
        ("UPLOAD", "part_size"),
        ("UPLOAD", "multipart_threshold"),
    ]
    for section, key in size_fields:
        try:
            value = parse_size(cfg.get(section, key))
        except Exception:
            print(f"❌ {section}.{key} 不是合法大小")
            sys.exit(1)

        if value <= 0:
            print(f"❌ {section}.{key} 必须大于 0")
            sys.exit(1)


# ================================
# 生成日志文件名
# ================================
def build_log_file(log_dir, source_name):
    os.makedirs(log_dir, exist_ok=True)

    name = _sanitize_name(os.path.basename(os.path.normpath(source_name)))
    date = datetime.now().strftime("%Y%m%d")

    index = 1
    while True:
        path = os.path.join(log_dir, f"{name}_{date}_{index}.log")
        if not os.path.exists(path):
            return path
        index += 1


# ================================
# 确保敏感字段已加密
# ================================
def _ensure_secret_fields_encrypted(cfg):
    changed = False
    for section, key in SENSITIVE_FIELDS:
        value = cfg.get(section, key, fallback="").strip()
        if value and not value.startswith("gAAAA"):
            cfg.set(section, key, encrypt_value(value))
            changed = True

    if changed:
        write_config_with_comments(cfg)


# ================================
# 主流程
# ================================
def main():
    ensure_dirs()

    cfg = load_config()
    validate_config(cfg)
    _ensure_secret_fields_encrypted(cfg)

    source_type = _normalize_mode(cfg.get(SOURCE_SECTION, "type", fallback=MODE_LOCAL), default=MODE_LOCAL)
    target_type = _normalize_mode(cfg.get(TARGET_SECTION, "type", fallback=MODE_S3), default=MODE_S3)

    source_path = cfg.get(SOURCE_SECTION, "path", fallback="").strip()
    source_ak = _decrypt_from_config(cfg, SOURCE_SECTION, "ak")
    source_sk = _decrypt_from_config(cfg, SOURCE_SECTION, "sk")
    source_endpoint = cfg.get(SOURCE_SECTION, "endpoint", fallback="").strip()
    source_bucket = cfg.get(SOURCE_SECTION, "bucket", fallback="").strip()
    source_prefix = sanitize_key(cfg.get(SOURCE_SECTION, "prefix", fallback="")).strip("/")

    target_path = cfg.get(TARGET_SECTION, "path", fallback="").strip()
    target_ak = _decrypt_from_config(cfg, TARGET_SECTION, "ak")
    target_sk = _decrypt_from_config(cfg, TARGET_SECTION, "sk")
    target_endpoint = cfg.get(TARGET_SECTION, "endpoint", fallback="").strip()
    target_bucket = cfg.get(TARGET_SECTION, "bucket", fallback="").strip()
    target_prefix = sanitize_key(cfg.get(TARGET_SECTION, "prefix", fallback="")).strip("/")

    source_label = _source_label(source_type, source_path, source_bucket, source_prefix)

    workers = cfg.getint("UPLOAD", "workers")
    retry_limit = cfg.getint("UPLOAD", "retry")
    rate_limit = cfg.getint("UPLOAD", "rate_limit")

    log_dir = resolve_runtime_path(cfg.get("PATH", "log_dir"))
    state_dir = resolve_runtime_path(cfg.get("PATH", "state_dir"))
    failed_dir = resolve_runtime_path(cfg.get("PATH", "failed_dir"))

    requested_scan_workers = cfg.getint("SCAN", "scan_workers", fallback=4)
    if source_type == MODE_LOCAL:
        scan_workers = resolve_scan_workers(requested_scan_workers)
    else:
        scan_workers = resolve_remote_scan_workers(requested_scan_workers)

    enable_head = cfg.getboolean("CHECK", "enable_head_check", fallback=True)
    strict_check = cfg.getboolean("CHECK", "strict_client_check", fallback=True)
    enable_etag = cfg.getboolean("CHECK", "enable_etag_check", fallback=False)

    report_dir = resolve_runtime_path("./check_report")
    os.makedirs(report_dir, exist_ok=True)

    log_file = build_log_file(log_dir, source_label)
    setup_logger(log_file)
    logging.getLogger().propagate = False

    if scan_workers != requested_scan_workers:
        if source_type == MODE_LOCAL:
            adjust_reason = f"本地扫描按 CPU 自适应限流（当前上限 {scan_workers}）"
        else:
            adjust_reason = f"远端扫描线程上限为 {scan_workers}"
        print(
            f"\n⚠️ 扫描线程配置过高，已从 {requested_scan_workers} 自动调整为 {scan_workers}（{adjust_reason}）\n"
        )
        logging.warning(
            "[SCAN] requested workers=%s adjusted to %s for source_type=%s reason=%s",
            requested_scan_workers,
            scan_workers,
            source_type,
            adjust_reason,
        )

    db_path = os.path.join(state_dir, "tasks.db")
    checkpoint = Checkpoint(db_path)
    checkpoint.reset_obs_index()

    is_local_single_file = source_type == MODE_LOCAL and os.path.isfile(source_path)
    progress = Progress()
    task_queue = queue.Queue(maxsize=cfg.getint("SCAN", "queue_size"))
    scan_controller = None
    if not is_local_single_file and scan_workers > 1:
        scan_controller = AdaptiveScanController(
            task_queue,
            max_workers=scan_workers,
            min_workers=resolve_min_scan_workers(scan_workers),
        )

    init_target(
        target_type,
        parse_size(cfg.get("UPLOAD", "part_size")),
        parse_size(cfg.get("UPLOAD", "multipart_threshold")),
        rate_limit=rate_limit,
        ak=target_ak,
        sk=target_sk,
        endpoint=target_endpoint,
        bucket=target_bucket,
        path=target_path,
        prefix=target_prefix,
    )
    init_source_client(
        source_ak,
        source_sk,
        source_endpoint,
        source_bucket,
    )

    reporter = Reporter(report_dir, source_label)
    uploader = OBSUploader(
        progress,
        checkpoint,
        reporter=reporter,
        failed_dir=failed_dir,
        enable_head_check=enable_head,
        strict_client_check=strict_check,
        enable_etag_check=enable_etag,
        retry_limit=retry_limit,
    )
    scheduler = Scheduler(task_queue, uploader, workers=workers)

    pipeline_status = {
        "index": "pending" if target_type == MODE_S3 else "n/a",
        "scan": "n/a" if is_local_single_file else "pending",
    }
    pipeline_status_lock = threading.Lock()
    interrupted = False

    # ================================
    # 更新流水线状态
    # ================================
    def set_status(name, value):
        with pipeline_status_lock:
            pipeline_status[name] = value

    # ================================
    # 获取流水线状态快照
    # ================================
    def get_status():
        with pipeline_status_lock:
            return dict(pipeline_status)

    dashboard = Dashboard(
        progress,
        task_queue,
        scheduler,
        scan_workers=scan_workers,
        enabled=should_enable_dashboard(cfg),
        force_terminal=should_force_terminal(),
        status_provider=get_status,
        scan_controller=scan_controller,
    )

    # ================================
    # 构建目标端对象索引
    # ================================
    def run_index():
        if target_type != MODE_S3:
            set_status("index", "n/a")
            return

        set_status("index", "running")
        try:
            build_obs_index(
                target_ak,
                target_sk,
                target_endpoint,
                target_bucket,
                target_prefix,
                checkpoint,
            )
        except Exception:
            set_status("index", "error")
            raise
        else:
            set_status("index", "done")

    index_thread = threading.Thread(target=run_index, daemon=True) if target_type == MODE_S3 else None
    scan_thread = None
    scan_done_event = threading.Event()

    # ================================
    # 启动索引、扫描与上传流程
    # ================================
    def start_work():
        nonlocal scan_thread

        progress.start()
        scheduler.start()
        if index_thread is not None:
            index_thread.start()

        if is_local_single_file:
            st = os.stat(source_path)
            filename = os.path.basename(source_path)
            if hasattr(reporter, "track_task"):
                reporter.track_task(
                    source_path,
                    size=st.st_size,
                )
            task_queue.put(
                {
                    "source_type": MODE_LOCAL,
                    "local": source_path,
                    "source_path": source_path,
                    "relative_path": filename,
                    "size": st.st_size,
                    "mtime": st.st_mtime,
                }
            )
            progress.add_total(st.st_size)
            scan_done_event.set()
            return

        if source_type == MODE_LOCAL:
            # ================================
            # 执行本地目录扫描
            # ================================
            def run_local_scan():
                set_status("scan", "running")
                try:
                    scan_directory(
                        source_path,
                        task_queue,
                        progress,
                        checkpoint,
                        reporter,
                        scan_workers,
                        scan_done_event,
                        scan_controller=scan_controller,
                    )
                except Exception:
                    set_status("scan", "error")
                    scan_done_event.set()
                    raise
                else:
                    set_status("scan", "done")

            scan_thread = threading.Thread(target=run_local_scan, daemon=True)
            scan_thread.start()
            return

        # ================================
        # 执行对象存储扫描
        # ================================
        def run_s3_scan():
            set_status("scan", "running")
            try:
                scan_s3_objects(
                    uploader_module._source_client,
                    source_bucket,
                    source_prefix,
                    task_queue,
                    progress,
                    reporter,
                    scan_workers=scan_workers,
                    scan_done_event=scan_done_event,
                    source_scheme=uploader_module._source_uri_scheme,
                    scan_controller=scan_controller,
                )
            except Exception:
                set_status("scan", "error")
                scan_done_event.set()
                raise
            else:
                set_status("scan", "done")

        scan_thread = threading.Thread(target=run_s3_scan, daemon=True)
        scan_thread.start()

    try:
        logging.info("Task Started. Log: %s", log_file)

        # ================================
        # 判断整体任务是否完成
        # ================================
        def work_finished():
            if is_local_single_file:
                return task_queue.unfinished_tasks == 0

            return scan_done_event.is_set() and task_queue.unfinished_tasks == 0

        dashboard.run_until(
            work_finished,
            poll_interval=0.2,
            start_fn=start_work,
        )
    except KeyboardInterrupt:
        interrupted = True
        logging.warning("用户手动停止任务")
    finally:
        if scan_thread is not None:
            scan_thread.join(timeout=5)

        scheduler.stop()
        progress.stop()
        checkpoint.close()
        dashboard.stop()
        reporter.close(
            pending_status="INTERRUPTED" if interrupted else None,
            pending_message="detected_but_not_migrated",
        )

        print("\n" + "=" * 50)
        print("✨ 任务结束")
        print("日志:", log_file)
        print("数据库:", db_path)
        print("对比报告:", reporter.file)
        print("=" * 50)


def main():
    ensure_dirs()

    cfg = load_config()
    validate_config(cfg)
    _ensure_secret_fields_encrypted(cfg)

    source_type = _normalize_mode(cfg.get(SOURCE_SECTION, "type", fallback=MODE_LOCAL), default=MODE_LOCAL)
    target_type = _normalize_mode(cfg.get(TARGET_SECTION, "type", fallback=MODE_S3), default=MODE_S3)

    source_path = cfg.get(SOURCE_SECTION, "path", fallback="").strip()
    source_ak = _decrypt_from_config(cfg, SOURCE_SECTION, "ak")
    source_sk = _decrypt_from_config(cfg, SOURCE_SECTION, "sk")
    source_endpoint = cfg.get(SOURCE_SECTION, "endpoint", fallback="").strip()
    source_bucket = cfg.get(SOURCE_SECTION, "bucket", fallback="").strip()
    source_prefix = sanitize_key(cfg.get(SOURCE_SECTION, "prefix", fallback="")).strip("/")

    target_path = cfg.get(TARGET_SECTION, "path", fallback="").strip()
    target_ak = _decrypt_from_config(cfg, TARGET_SECTION, "ak")
    target_sk = _decrypt_from_config(cfg, TARGET_SECTION, "sk")
    target_endpoint = cfg.get(TARGET_SECTION, "endpoint", fallback="").strip()
    target_bucket = cfg.get(TARGET_SECTION, "bucket", fallback="").strip()
    target_prefix = sanitize_key(cfg.get(TARGET_SECTION, "prefix", fallback="")).strip("/")

    source_label = _source_label(source_type, source_path, source_bucket, source_prefix)

    workers = cfg.getint("UPLOAD", "workers")
    checker_workers = cfg.getint("UPLOAD", "checkers", fallback=max(1, workers // 2))
    retry_limit = cfg.getint("UPLOAD", "retry")
    rate_limit = cfg.getint("UPLOAD", "rate_limit")
    rate_limit_burst = cfg.getint("UPLOAD", "rate_limit_burst", fallback=rate_limit)
    low_level_retries = cfg.getint("UPLOAD", "low_level_retries", fallback=5)
    low_level_retry_sleep = cfg.getfloat("UPLOAD", "low_level_retry_sleep", fallback=0.5)
    max_connections = cfg.getint("UPLOAD", "max_connections", fallback=256)
    multipart_concurrency = cfg.getint("UPLOAD", "multipart_concurrency", fallback=4)
    max_buffer_bytes = parse_size(cfg.get("UPLOAD", "max_buffer_memory", fallback="0") or "0")
    request_timeout = cfg.getint("UPLOAD", "request_timeout", fallback=60)
    worker_stall_timeout = cfg.getint("UPLOAD", "worker_stall_timeout", fallback=300)

    log_dir = resolve_runtime_path(cfg.get("PATH", "log_dir"))
    state_dir = resolve_runtime_path(cfg.get("PATH", "state_dir"))
    failed_dir = resolve_runtime_path(cfg.get("PATH", "failed_dir"))

    requested_scan_workers = cfg.getint("SCAN", "scan_workers", fallback=4)
    if source_type == MODE_LOCAL:
        scan_workers = resolve_scan_workers(requested_scan_workers)
    else:
        scan_workers = resolve_remote_scan_workers(requested_scan_workers)

    enable_head = cfg.getboolean("CHECK", "enable_head_check", fallback=True)
    strict_check = cfg.getboolean("CHECK", "strict_client_check", fallback=True)
    enable_etag = cfg.getboolean("CHECK", "enable_etag_check", fallback=False)
    compare_mode = cfg.get("CHECK", "target_compare_mode", fallback="auto")
    verify_after_upload = cfg.get("CHECK", "verify_after_upload", fallback="head")

    report_dir = resolve_runtime_path("./check_report")
    os.makedirs(report_dir, exist_ok=True)

    log_file = build_log_file(log_dir, source_label)
    setup_logger(log_file)
    logging.getLogger().propagate = False

    if scan_workers != requested_scan_workers:
        if source_type == MODE_LOCAL:
            adjust_reason = f"本地扫描按 CPU 自适应限流（当前上限 {scan_workers}）"
        else:
            adjust_reason = f"远端扫描线程上限为 {scan_workers}"
        print(
            f"\n⚠️ 扫描线程配置过高，已从 {requested_scan_workers} 自动调整为 {scan_workers}（{adjust_reason}）\n"
        )
        logging.warning(
            "[SCAN] requested workers=%s adjusted to %s for source_type=%s reason=%s",
            requested_scan_workers,
            scan_workers,
            source_type,
            adjust_reason,
        )

    db_path = os.path.join(state_dir, "tasks.db")
    checkpoint = Checkpoint(db_path)
    checkpoint.reset_obs_index()

    is_local_single_file = source_type == MODE_LOCAL and os.path.isfile(source_path)
    progress = Progress()
    check_queue = queue.Queue(maxsize=cfg.getint("SCAN", "queue_size"))
    task_queue = queue.Queue(maxsize=cfg.getint("SCAN", "queue_size"))

    scan_controller = None
    if not is_local_single_file and scan_workers > 1:
        scan_controller = AdaptiveScanController(
            check_queue,
            max_workers=scan_workers,
            min_workers=resolve_min_scan_workers(scan_workers),
        )

    init_target(
        target_type,
        parse_size(cfg.get("UPLOAD", "part_size")),
        parse_size(cfg.get("UPLOAD", "multipart_threshold")),
        rate_limit=rate_limit,
        ak=target_ak,
        sk=target_sk,
        endpoint=target_endpoint,
        bucket=target_bucket,
        path=target_path,
        prefix=target_prefix,
        rate_limit_burst=rate_limit_burst,
        max_connections=max_connections,
        max_buffer_bytes=max_buffer_bytes,
        multipart_concurrency=multipart_concurrency,
        low_level_retries=low_level_retries,
        low_level_retry_sleep=low_level_retry_sleep,
        compare_mode=compare_mode,
        verify_after_upload=verify_after_upload,
        request_timeout=request_timeout,
    )
    init_source_client(
        source_ak,
        source_sk,
        source_endpoint,
        source_bucket,
        request_timeout=request_timeout,
    )

    reporter = Reporter(report_dir, source_label)
    uploader = OBSUploader(
        progress,
        checkpoint,
        reporter=reporter,
        failed_dir=failed_dir,
        enable_head_check=enable_head,
        strict_client_check=strict_check,
        enable_etag_check=enable_etag,
        retry_limit=retry_limit,
        compare_mode=compare_mode,
        verify_after_upload=verify_after_upload,
        low_level_retries=low_level_retries,
        low_level_retry_sleep=low_level_retry_sleep,
        multipart_concurrency=multipart_concurrency,
    )
    checker_handler = TaskChecker(uploader, task_queue)
    transfer_handler = TaskTransfer(uploader)
    checker_scheduler = Scheduler(
        check_queue,
        checker_handler,
        workers=checker_workers,
        stage_name="check",
        stall_timeout=worker_stall_timeout,
    )
    scheduler = Scheduler(
        task_queue,
        transfer_handler,
        workers=workers,
        stage_name="upload",
        stall_timeout=worker_stall_timeout,
    )

    pipeline_status = {
        "index": "pending" if target_type == MODE_S3 else "n/a",
        "scan": "n/a" if is_local_single_file else "pending",
        "check": "pending",
    }
    pipeline_status_lock = threading.Lock()
    background_error = [None]
    background_error_lock = threading.Lock()
    interrupted = False

    def set_status(name, value):
        with pipeline_status_lock:
            pipeline_status[name] = value

    def get_status():
        with pipeline_status_lock:
            return dict(pipeline_status)

    def record_background_error(exc):
        with background_error_lock:
            if background_error[0] is None:
                background_error[0] = exc

    dashboard = Dashboard(
        progress,
        task_queue,
        scheduler,
        scan_workers=scan_workers,
        checker_queue=check_queue,
        checker_scheduler=checker_scheduler,
        enabled=should_enable_dashboard(cfg),
        force_terminal=should_force_terminal(),
        status_provider=get_status,
        scan_controller=scan_controller,
    )

    def run_index():
        if target_type != MODE_S3:
            set_status("index", "n/a")
            return

        set_status("index", "running")
        try:
            build_obs_index(
                target_ak,
                target_sk,
                target_endpoint,
                target_bucket,
                target_prefix,
                checkpoint,
                low_level_retries=low_level_retries,
                low_level_retry_sleep=low_level_retry_sleep,
                request_timeout=request_timeout,
            )
        except Exception as exc:
            set_status("index", "error")
            record_background_error(exc)
            raise
        else:
            set_status("index", "done")

    index_thread = threading.Thread(target=run_index, daemon=True) if target_type == MODE_S3 else None
    scan_thread = None
    scan_done_event = threading.Event()

    def start_work():
        nonlocal scan_thread

        progress.start()
        checker_scheduler.start()
        scheduler.start()
        set_status("check", "running")

        if index_thread is not None:
            index_thread.start()

        if is_local_single_file:
            stat_result = os.stat(source_path)
            filename = os.path.basename(source_path)
            if hasattr(reporter, "track_task"):
                reporter.track_task(source_path, size=stat_result.st_size)
            check_queue.put(
                {
                    "source_type": MODE_LOCAL,
                    "local": source_path,
                    "source_path": source_path,
                    "relative_path": filename,
                    "size": stat_result.st_size,
                    "mtime": stat_result.st_mtime,
                }
            )
            progress.add_total(stat_result.st_size)
            scan_done_event.set()
            set_status("scan", "done")
            return

        if source_type == MODE_LOCAL:
            def run_local_scan():
                set_status("scan", "running")
                try:
                    scan_directory(
                        source_path,
                        check_queue,
                        progress,
                        checkpoint,
                        reporter,
                        scan_workers,
                        scan_done_event,
                        scan_controller=scan_controller,
                    )
                except Exception as exc:
                    set_status("scan", "error")
                    scan_done_event.set()
                    record_background_error(exc)
                    raise
                else:
                    set_status("scan", "done")

            scan_thread = threading.Thread(target=run_local_scan, daemon=True)
            scan_thread.start()
            return

        def run_s3_scan():
            set_status("scan", "running")
            try:
                scan_s3_objects(
                    uploader_module._source_client,
                    source_bucket,
                    source_prefix,
                    check_queue,
                    progress,
                    reporter,
                    scan_workers=scan_workers,
                    scan_done_event=scan_done_event,
                    source_scheme=uploader_module._source_uri_scheme,
                    scan_controller=scan_controller,
                    low_level_retries=low_level_retries,
                    low_level_retry_sleep=low_level_retry_sleep,
                )
            except Exception as exc:
                set_status("scan", "error")
                scan_done_event.set()
                record_background_error(exc)
                raise
            else:
                set_status("scan", "done")

        scan_thread = threading.Thread(target=run_s3_scan, daemon=True)
        scan_thread.start()

    try:
        logging.info("Task Started. Log: %s", log_file)

        def work_finished():
            with background_error_lock:
                if background_error[0] is not None:
                    raise background_error[0]

            index_finished = index_thread is None or not index_thread.is_alive()
            queues_finished = (
                scan_done_event.is_set()
                and check_queue.unfinished_tasks == 0
                and task_queue.unfinished_tasks == 0
                and checker_scheduler.get_active_workers() == 0
                and scheduler.get_active_workers() == 0
            )
            if queues_finished and index_finished:
                set_status("check", "done")
                return True
            return False

        dashboard.run_until(
            work_finished,
            poll_interval=0.2,
            start_fn=start_work,
        )
    except KeyboardInterrupt:
        interrupted = True
        logging.warning("用户手动停止任务")
    finally:
        if scan_thread is not None:
            scan_thread.join(timeout=5)
        if index_thread is not None:
            index_thread.join(timeout=5)

        checker_scheduler.stop()
        scheduler.stop()
        progress.stop()
        checkpoint.close()
        dashboard.stop()
        reporter.close(
            pending_status="INTERRUPTED" if interrupted else None,
            pending_message="detected_but_not_migrated",
        )

        print("\n" + "=" * 50)
        print("✅ 任务结束")
        print("日志:", log_file)
        print("数据库:", db_path)
        print("对比报告:", reporter.file)
        print("=" * 50)


if __name__ == "__main__":
    main()

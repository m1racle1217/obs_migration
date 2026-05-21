#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""提供 OBS / S3 兼容对象存储迁移工具的命令行入口。"""

import argparse
import configparser
import glob
import importlib
import logging
import os
import queue
import re
import shutil
import sys
import threading
import time
import unicodedata
import webbrowser
from datetime import datetime

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(errors="replace")
    except Exception:
        pass

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
        BRIGHT = ""
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
    count_remote_prefix_items,
    create_obs_client,
    init_source_client,
    init_target,
    list_local_path,
    list_remote_buckets,
    list_remote_prefix,
    parent_prefix,
    scan_local_sources,
    scan_s3_objects,
    scan_s3_sources,
)
from core.obs_index import build_obs_index
from core.task_manager import MultiTaskManager as TaskManager
from core.utils import build_object_uri, detect_storage_scheme, parse_size, sanitize_key, setup_logger
from core.web_ui import WebConsoleServer

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
SOURCE_MODE_DIRECTORY = "directory"
SOURCE_MODE_LIST = "list"

SENSITIVE_FIELDS = {
    (SOURCE_SECTION, "ak"),
    (SOURCE_SECTION, "sk"),
    (TARGET_SECTION, "ak"),
    (TARGET_SECTION, "sk"),
    ("WEB_UI", "password"),
}

CONFIG_DESC = {
    "SOURCE.type": "源端模式：local 或 s3",
    "SOURCE.selection_mode": "源端选择模式：directory 目录模式（一对一）或 list 列表模式（多目录/文件/对象）",
    "SOURCE.path": "源端本地目录、单文件路径或通配符（source.type=local 时使用，例如 /data/202604*）",
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
    "CHECK.target_compare_mode": "目标端比较模式：auto / hybrid / index_only / head_only",
    "CHECK.verify_after_upload": "传输后校验模式：none / size / etag / head",
    "PATH.log_dir": "日志目录（相对配置文件目录解析）",
    "PATH.state_dir": "断点数据库目录（tasks.db 会写到这里）",
    "PATH.failed_dir": "失败任务目录（失败明细 / 补偿任务）",
    "PATH.migration_list_file": "源端列表模式清单文件（相对配置文件目录解析，默认 migration_list.txt）",
    "UI.prompt_config": "启动时是否允许交互修改配置（支持直接输入编号修改）",
    "UI.show_dashboard": "是否显示实时仪表盘",
    "WEB_UI.enabled": "是否启用 Web 控制台",
    "WEB_UI.host": "Web 控制台监听地址",
    "WEB_UI.port": "Web 控制台监听端口",
    "WEB_UI.require_login": "Web 控制台是否要求登录",
    "WEB_UI.username": "Web 控制台登录用户名",
    "WEB_UI.password": "Web 控制台登录密码",
    "WEB_UI.auto_open": "启动 Web 控制台后是否自动打开浏览器",
}

SECTION_TITLES = {
    SOURCE_SECTION: "源端配置",
    TARGET_SECTION: "目标端配置",
    "UPLOAD": "上传器配置",
    "SCAN": "扫描器配置",
    "CHECK": "校验器配置",
    "PATH": "运行目录配置",
    "UI": "界面配置",
    "WEB_UI": "Web 控制台配置",
}

REMOTE_ENDPOINT_KEYS = ("ak", "sk", "endpoint", "bucket", "prefix")
SOURCE_KEY_ORDER = ("type", "selection_mode", "path", "ak", "sk", "endpoint", "bucket", "prefix")

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
    {"id": "web_ui", "title": "Web 控制台配置", "sections": ["WEB_UI"]},
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
        "selection_mode": SOURCE_MODE_DIRECTORY,
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
        "migration_list_file": "./migration_list.txt",
    },
    "UI": {
        "prompt_config": "true",
        "show_dashboard": "true",
    },
    "WEB_UI": {
        "enabled": "false",
        "host": "127.0.0.1",
        "port": "8765",
        "require_login": "true",
        "username": "admin",
        "password": "admin",
        "auto_open": "false",
    },
    "BROWSER_PROFILES": {
        "profiles": "[]",
    },
}

CONFIG_DESC["UI.language"] = "界面语言：zh（中文）或 en（English）"
DEFAULT_CONFIG["UI"]["language"] = "zh"


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
def _read_menu_input(prompt, hotkeys=None, max_number=None):
    hotkeys = {str(key).lower() for key in (hotkeys or [])}
    max_number = int(max_number or 0)

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
        return _read_menu_input_windows(prompt, hotkeys, max_number)

    return _read_menu_input_posix(prompt, hotkeys, max_number)


def _read_menu_input_windows(prompt, hotkeys, max_number):
    import msvcrt

    sys.stdout.write(prompt)
    sys.stdout.flush()
    chars = []

    while True:
        ch = msvcrt.getwch()
        result = _handle_menu_char(ch, chars, hotkeys, max_number)
        if result is None:
            continue
        if result == "__SKIP_NEXT__":
            msvcrt.getwch()
            continue
        sys.stdout.write("\n")
        sys.stdout.flush()
        return result


def _read_menu_input_posix(prompt, hotkeys, max_number):
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
            result = _handle_menu_char(ch, chars, hotkeys, max_number)
            if result is None:
                continue
            if result == "__SKIP_NEXT__":
                continue
            sys.stdout.write("\n")
            sys.stdout.flush()
            return result
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, original)


def _handle_menu_char(ch, chars, hotkeys, max_number):
    if ch in {"\x00", "\xe0"}:
        return "__SKIP_NEXT__"

    if ch in {"\r", "\n"}:
        return "".join(chars)

    if ch == "\x03":
        raise KeyboardInterrupt

    if ch in {"\b", "\x7f"}:
        if chars:
            chars.pop()
            sys.stdout.write("\b \b")
            sys.stdout.flush()
        return None

    if ch == "\x1b":
        return None

    if not ch.isprintable():
        return None

    lowered = ch.lower()
    if not chars and lowered in hotkeys:
        sys.stdout.write(ch)
        sys.stdout.flush()
        return ch

    if ch.isdigit():
        chars.append(ch)
        sys.stdout.write(ch)
        sys.stdout.flush()
        if max_number > 0 and max_number <= 9:
            return "".join(chars)
        return None

    chars.append(ch)
    sys.stdout.write(ch)
    sys.stdout.flush()
    return None


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


def get_ui_language(cfg=None):
    if cfg is None:
        cfg = load_config()
    return Dashboard.normalize_language(cfg.get("UI", "language", fallback="zh"))


# ================================
# 判断是否强制终端渲染
# ================================
def should_force_terminal():
    env_value = parse_env_bool("OBS_MIGRATE_FORCE_TERMINAL")
    if env_value is not None:
        return env_value

    if os.getenv("CI"):
        return False

    if os.name == "nt" or os.getenv("PYCHARM_HOSTED") == "1":
        return True

    try:
        if sys.stdout.isatty() or sys.stderr.isatty() or sys.stdin.isatty():
            return True
    except Exception:
        pass

    term_name = (os.getenv("TERM") or "").strip().lower()
    if term_name and term_name != "dumb":
        return True

    if os.getenv("SSH_TTY") or os.getenv("COLORTERM"):
        return True

    return False


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
# 归一化源端选择模式
# ================================
def _normalize_source_selection_mode(value, default=SOURCE_MODE_DIRECTORY):
    if value is None:
        return default

    text = str(value).strip().lower()
    if not text:
        return default

    mapping = {
        "1": SOURCE_MODE_DIRECTORY,
        "dir": SOURCE_MODE_DIRECTORY,
        "directory": SOURCE_MODE_DIRECTORY,
        "目录": SOURCE_MODE_DIRECTORY,
        "目录模式": SOURCE_MODE_DIRECTORY,
        "2": SOURCE_MODE_LIST,
        "list": SOURCE_MODE_LIST,
        "列表": SOURCE_MODE_LIST,
        "列表模式": SOURCE_MODE_LIST,
    }
    return mapping.get(text)


# ================================
# 交互选择模式
# ================================
def _prompt_mode(section_label, current_value, allow_empty=False):
    current_value = _normalize_mode(current_value, default=MODE_LOCAL)

    while True:
        title = "源端模式" if section_label == "source" else "目标端模式"
        print()
        _print_box(
            title,
            [
                [
                    ("1. ", Fore.WHITE),
                    ("local", _bright(MENU_COLOR_MODE)),
                    ("  本地目录 / 文件", MENU_COLOR_DESC),
                ],
                [
                    ("2. ", Fore.WHITE),
                    ("s3", _bright(MENU_COLOR_LIST)),
                    ("     S3 / OBS 兼容对象存储", MENU_COLOR_DESC),
                ],
            ],
            footer_lines=[f"当前值：{current_value}    [1] local    [2] s3    [L] local    [S] s3"],
        )
        raw = _read_menu_input("请选择 1/2/L/S，或回车保持当前值: ", hotkeys={"l", "s"}, max_number=2).strip()

        if not raw and allow_empty:
            return current_value
        if raw.lower() == "l":
            return MODE_LOCAL
        if raw.lower() == "s":
            return MODE_S3

        normalized = _normalize_mode(raw, default=current_value if allow_empty else None)
        if normalized in {MODE_LOCAL, MODE_S3}:
            return normalized

        print("请输入 local / s3，或者输入 1 / 2。")


# ================================
# 交互选择源端选择模式
# ================================
def _prompt_source_selection_mode(current_value, allow_empty=False):
    current_value = _normalize_source_selection_mode(current_value, default=SOURCE_MODE_DIRECTORY)

    while True:
        print()
        _print_box(
            "源端选择模式",
            [
                [
                    ("1. ", Fore.WHITE),
                    ("directory", _bright(MENU_COLOR_MODE)),
                    ("  目录模式（一对一迁移）", MENU_COLOR_DESC),
                ],
                [
                    ("2. ", Fore.WHITE),
                    ("list", _bright(MENU_COLOR_LIST)),
                    ("       列表模式（多目录/文件/对象）", MENU_COLOR_DESC),
                ],
            ],
            footer_lines=[f"当前值：{current_value}    [1] directory    [2] list    [D] directory    [L] list"],
        )
        raw = _read_menu_input("请选择 1/2/D/L，或回车保持当前值: ", hotkeys={"d", "l"}, max_number=2).strip()

        if not raw and allow_empty:
            return current_value
        if raw.lower() == "d":
            return SOURCE_MODE_DIRECTORY
        if raw.lower() == "l":
            return SOURCE_MODE_LIST

        normalized = _normalize_source_selection_mode(raw, default=current_value if allow_empty else None)
        if normalized in {SOURCE_MODE_DIRECTORY, SOURCE_MODE_LIST}:
            return normalized

        print("请输入 directory / list，或者输入 1 / 2。")


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
        hidden = {key for key in REMOTE_ENDPOINT_KEYS if key in cfg[section]}
        if section == SOURCE_SECTION:
            source_selection_mode = _normalize_source_selection_mode(
                cfg.get(section, "selection_mode", fallback=SOURCE_MODE_DIRECTORY),
                default=SOURCE_MODE_DIRECTORY,
            )
            if source_selection_mode == SOURCE_MODE_LIST:
                hidden.add("path")
            hidden.add("paths")
        return hidden

    if current_mode == MODE_S3:
        hidden = {"path"} if "path" in cfg[section] else set()
        if section == SOURCE_SECTION:
            selection_mode = _normalize_source_selection_mode(
                cfg.get(section, "selection_mode", fallback=SOURCE_MODE_DIRECTORY),
                default=SOURCE_MODE_DIRECTORY,
            )
            if selection_mode == SOURCE_MODE_LIST:
                hidden.add("prefix")
            hidden.add("paths")
        return hidden

    return set()


# ================================
# 获取当前展示的配置项
# ================================
def _visible_items_for_section(cfg, section):
    hidden_keys = _hidden_keys_for_section(cfg, section)
    items = [(key, value) for key, value in cfg[section].items() if key not in hidden_keys]
    if section != SOURCE_SECTION:
        return items

    order_map = {key: index for index, key in enumerate(SOURCE_KEY_ORDER)}
    return sorted(items, key=lambda item: (order_map.get(item[0], len(order_map)), item[0]))


# ================================
# 获取写回配置项顺序
# ================================
def _ordered_items_for_section(cfg, section):
    items = list(cfg[section].items())
    if section != SOURCE_SECTION:
        return items
    order_map = {key: index for index, key in enumerate(SOURCE_KEY_ORDER)}
    return sorted(items, key=lambda item: (order_map.get(item[0], len(order_map)), item[0]))


# ================================
# 生成配置分组说明
# ================================
def _section_display_hint(cfg, section):
    if section not in {SOURCE_SECTION, TARGET_SECTION} or not cfg.has_section(section):
        return ""

    default_mode = MODE_LOCAL if section == SOURCE_SECTION else MODE_S3
    current_mode = _normalize_mode(cfg.get(section, "type", fallback=default_mode), default=default_mode)
    hidden_count = len(_hidden_keys_for_section(cfg, section))

    source_selection_mode = ""
    if section == SOURCE_SECTION and current_mode == MODE_LOCAL:
        source_selection_mode = _normalize_source_selection_mode(
            cfg.get(section, "selection_mode", fallback=SOURCE_MODE_DIRECTORY),
            default=SOURCE_MODE_DIRECTORY,
        )
    elif section == SOURCE_SECTION and current_mode == MODE_S3:
        source_selection_mode = _normalize_source_selection_mode(
            cfg.get(section, "selection_mode", fallback=SOURCE_MODE_DIRECTORY),
            default=SOURCE_MODE_DIRECTORY,
        )

    shown_mode = f"{current_mode}/{source_selection_mode}" if source_selection_mode else current_mode

    if hidden_count > 0:
        return f"当前模式：{shown_mode}，仅显示生效项，已折叠 {hidden_count} 项"

    return f"当前模式：{shown_mode}"


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
# 渲染彩色文本片段
# ================================
def _style_text(text, color=""):
    if not color:
        return str(text)
    return f"{color}{text}{Style.RESET_ALL}"


# ================================
# 生成 256 色前景色
# ================================
def _ansi256(code):
    return f"\033[38;5;{int(code)}m"


# ================================
# 菜单调色板
# ================================
MENU_COLOR_BORDER = _ansi256(39)
MENU_COLOR_TITLE = _ansi256(45)
MENU_COLOR_HOTKEY = _ansi256(226)
MENU_COLOR_ACTION = _ansi256(118)
MENU_COLOR_MODE = _ansi256(81)
MENU_COLOR_LIST = _ansi256(213)
MENU_COLOR_PARAM = _ansi256(220)
MENU_COLOR_KEY = _ansi256(117)
MENU_COLOR_DESC = _ansi256(222)
MENU_COLOR_VALUE = _ansi256(255)
MENU_COLOR_MUTED = _ansi256(245)
MENU_COLOR_TRUE = _ansi256(82)
MENU_COLOR_FALSE = _ansi256(203)
MENU_COLOR_PATH = _ansi256(159)


# ================================
# 高亮颜色
# ================================
def _bright(color):
    return f"{getattr(Style, 'BRIGHT', '')}{color}"


# ================================
# 判断是否为彩色片段行
# ================================
def _is_segment_line(line):
    return isinstance(line, list)


# ================================
# 提取彩色片段纯文本
# ================================
def _segments_text(segments):
    return "".join(str(part[0]) for part in segments)


# ================================
# 构建彩色盒子内容行
# ================================
def _box_line_segments(segments, width):
    inside_width = max(width - 4, 0)
    plain_text = _segments_text(segments)
    visible_text = _truncate_display(plain_text, inside_width)
    remaining = _display_width(visible_text)
    rendered_parts = []

    for text, color in segments:
        if remaining <= 0:
            break
        clipped = _truncate_display(text, remaining)
        if not clipped:
            continue
        rendered_parts.append(_style_text(clipped, color))
        remaining -= _display_width(clipped)

    padding = " " * max(inside_width - _display_width(visible_text), 0)
    return f"{MENU_COLOR_BORDER}│{Style.RESET_ALL} {''.join(rendered_parts)}{padding} {MENU_COLOR_BORDER}│{Style.RESET_ALL}"


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
# 构建彩色文本盒子内容行
# ================================
def _box_line_colored(text, width, color):
    inside_width = max(width - 4, 0)
    content = _fit_display(text, inside_width)
    return f"{MENU_COLOR_BORDER}│{Style.RESET_ALL} {color}{content}{Style.RESET_ALL} {MENU_COLOR_BORDER}│{Style.RESET_ALL}"


# ================================
# 固定刷新交互面板
# ================================
def _clear_interactive_screen():
    if not sys.stdout.isatty():
        return
    try:
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()
    except Exception:
        os.system("cls" if os.name == "nt" else "clear")


# ================================
# 渲染文本盒子
# ================================
def _print_box(title, body_lines, footer_lines=None, subtitle_lines=None):
    _clear_interactive_screen()

    def line_text(line):
        if _is_segment_line(line):
            return _segments_text(line)
        if isinstance(line, tuple):
            return line[0]
        return line

    def line_color(line, default_color):
        if isinstance(line, tuple) and len(line) > 1:
            return line[1]
        return default_color

    lines = []
    if subtitle_lines:
        lines.extend(line_text(line) for line in subtitle_lines)
    lines.extend(line_text(line) for line in body_lines)
    if footer_lines:
        lines.extend(line_text(line) for line in footer_lines)

    width = _resolve_box_width(title, lines)
    print(f"{MENU_COLOR_BORDER}{_box_border(title, width, top=True)}{Style.RESET_ALL}")

    if subtitle_lines:
        for line in subtitle_lines:
            if _is_segment_line(line):
                print(_box_line_segments(line, width))
                continue
            text = line_text(line)
            color = line_color(line, Fore.LIGHTBLACK_EX)
            print(_box_line_colored(text, width, color))
        if body_lines or footer_lines:
            print(f"{MENU_COLOR_BORDER}{_box_separator(width)}{Style.RESET_ALL}")

    for line in body_lines:
        if _is_segment_line(line):
            print(_box_line_segments(line, width))
            continue
        text = line_text(line)
        color = line_color(line, Fore.WHITE)
        print(_box_line_colored(text, width, color))

    if footer_lines:
        if body_lines:
            print(f"{MENU_COLOR_BORDER}{_box_separator(width)}{Style.RESET_ALL}")
        for line in footer_lines:
            if _is_segment_line(line):
                print(_box_line_segments(line, width))
                continue
            text = line_text(line)
            if "[" in str(text) and "]" in str(text):
                print(_box_line_segments(_hotkey_segments(text), width))
                continue
            color = line_color(line, Fore.GREEN)
            print(_box_line_colored(text, width, color))

    print(f"{MENU_COLOR_BORDER}{_box_border(title, width, top=False)}{Style.RESET_ALL}")


# ================================
# 获取菜单分组内的配置项
# ================================
def _format_browser_size(value):
    if value is None:
        return "-"

    try:
        size = float(value)
    except Exception:
        return "-"

    units = [
        (1024.0 ** 4, "T"),
        (1024.0 ** 3, "G"),
        (1024.0 ** 2, "M"),
        (1024.0, "KB"),
    ]
    for factor, unit in units:
        if size >= factor:
            return f"{size / factor:.2f}{unit}"
    return f"{int(size)}B"


def _format_browser_time(value):
    try:
        timestamp = float(value or 0)
    except Exception:
        timestamp = 0

    if timestamp <= 0:
        return "-"

    try:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"


def _browser_lines(page):
    rows = []
    for index, item in enumerate(page.items or [], start=1):
        kind = {"bucket": "桶", "dir": "目录", "file": "文件"}.get(item.kind, item.kind)
        name = item.name + ("/" if item.kind == "dir" and not item.name.endswith("/") else "")
        detail = (
            f"{index:>3}. "
            f"{_fit_display(kind, 4)} "
            f"{_fit_display(_shorten_text(name, 34), 34)} "
            f"{_fit_display(_format_browser_size(item.size), 10)} "
            f"{_fit_display(_format_browser_time(item.mtime), 19)}"
        )
        rows.append(detail)

    if not rows:
        rows.append("(空)")
    return rows


def _browser_count_text(page, total=None):
    shown = len(page.items or [])
    if shown <= 0:
        shown_text = "0"
    else:
        start = (max(int(page.page or 1), 1) - 1) * max(int(page.page_size or shown), 1) + 1
        end = start + shown - 1
        shown_text = str(start) if start == end else f"{start}-{end}"

    total_value = page.total_known if total is None else total
    if total_value is None:
        return f"当前显示: {shown_text} / 对象数量: 统计中，按 R 刷新"
    return f"当前显示: {shown_text} / 对象数量: {total_value}"


def _remote_browser_title(section, bucket, prefix):
    side = "源端" if section == SOURCE_SECTION else "目标端"
    if not bucket:
        return f"{side}桶列表"

    shown_prefix = prefix or "/"
    return f"{side}浏览: {bucket}/{shown_prefix}"


def _save_browser_selection(cfg, section, bucket=None, prefix=None, path=None):
    if bucket is not None:
        cfg[section]["bucket"] = bucket
    if prefix is not None:
        cfg[section]["prefix"] = prefix
    if path is not None:
        cfg[section]["path"] = path
    write_config_with_comments(cfg)
    print(f"\n已保存到 {resolve_config_file()}\n")


# ================================
# 获取源端列表文件路径
# ================================
def _source_paths_file(cfg):
    raw_value = ""
    if cfg.has_section("PATH"):
        raw_value = cfg.get("PATH", "migration_list_file", fallback="").strip()
    if not raw_value:
        raw_value = cfg.get(SOURCE_SECTION, "paths_file", fallback="").strip()
    if not raw_value:
        raw_value = DEFAULT_CONFIG["PATH"]["migration_list_file"]
    if not raw_value:
        return ""
    expanded = os.path.expanduser(raw_value)
    if os.path.isabs(expanded):
        return expanded
    return os.path.abspath(os.path.join(config_base_dir(), expanded))


# ================================
# 从源端列表文件读取条目
# ================================
def _read_source_paths_file(file_path):
    if not file_path or not os.path.exists(file_path):
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        return parse_source_path_list(f.read())


# ================================
# 写回源端列表文件
# ================================
def _write_source_paths_file(file_path, paths):
    parent_dir = os.path.dirname(os.path.abspath(file_path))
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    with open(file_path, "w", encoding="utf-8", newline="\n") as f:
        content = serialize_source_path_list(paths)
        if content:
            f.write(content)
            f.write("\n")


# ================================
# 读取源端列表
# ================================
def _source_path_list(cfg):
    file_path = _source_paths_file(cfg)
    if file_path:
        file_paths = _read_source_paths_file(file_path)
        if file_paths or os.path.exists(file_path):
            return file_paths
    return parse_source_path_list(cfg.get(SOURCE_SECTION, "paths", fallback=""))


# ================================
# 写回源端列表
# ================================
def _set_source_path_list(cfg, paths):
    file_path = _source_paths_file(cfg)
    if file_path:
        _write_source_paths_file(file_path, paths)
    write_config_with_comments(cfg)


# ================================
# 添加源端列表项
# ================================
def _add_source_path_to_list(cfg, path):
    paths = _source_path_list(cfg)
    normalized = os.path.normcase(os.path.abspath(os.path.expanduser(path)))
    if any(os.path.normcase(os.path.abspath(os.path.expanduser(item))) == normalized for item in paths):
        print(f"\n已在列表中：{path}\n")
        return
    paths.append(path)
    _set_source_path_list(cfg, paths)
    print(f"\n已添加到源端列表：{path}\n")


# ================================
# 清空源端列表
# ================================
def _clear_source_path_list(cfg):
    _set_source_path_list(cfg, [])
    print("\n源端列表已清空。\n")


# ================================
# 删除源端列表项
# ================================
def _remove_source_path_from_list(cfg, index):
    paths = _source_path_list(cfg)
    if index < 1 or index > len(paths):
        print("\n编号不存在。\n")
        return

    removed = paths.pop(index - 1)
    _set_source_path_list(cfg, paths)
    print(f"\n已从源端列表删除：{removed}\n")


# ================================
# 生成源端列表展示行
# ================================
def _source_path_list_lines(cfg):
    paths = _source_path_list(cfg)
    if not paths:
        return ["(源端列表为空)"]

    source_type = _normalize_mode(cfg.get(SOURCE_SECTION, "type", fallback=MODE_LOCAL), default=MODE_LOCAL)
    rows = []
    for index, path in enumerate(paths, start=1):
        if source_type == MODE_S3:
            kind = "前缀/对象"
        else:
            kind = "目录" if os.path.isdir(path) else "文件" if os.path.isfile(path) else "不存在"
        rows.append(f"{index}. [{kind}] {_shorten_text(path, 74)}")
    return rows


# ================================
# 展示源端列表
# ================================
def show_source_path_list(cfg):
    file_path = _source_paths_file(cfg)
    subtitle_lines = ["列表模式会迁移这里的所有源端条目；本地可填文件/目录，S3 可填桶内前缀/对象。"]
    if file_path:
        subtitle_lines.append(f"列表文件：{_shorten_text(file_path, 74)}")
    _print_box(
        "源端列表",
        _source_path_list_lines(cfg),
        footer_lines=["[A] 添加路径    [D] 删除路径    [C] 清空列表    [B] 返回上一级"],
        subtitle_lines=subtitle_lines,
    )


# ================================
# 管理源端列表
# ================================
def prompt_source_path_list_action(cfg):
    while True:
        show_source_path_list(cfg)
        paths = _source_path_list(cfg)
        list_action = _read_menu_input(
            "\n请选择 A/D/C/B: ",
            hotkeys={"a", "d", "c", "b"},
        ).strip().lower()

        if list_action == "b":
            return

        if list_action == "a":
            path_value = _read_input("请输入要添加的源端路径/前缀/对象: ").strip()
            if path_value:
                _add_source_path_to_list(cfg, path_value)
            continue

        if list_action == "d":
            if not paths:
                print("\n源端列表为空，无需删除。\n")
                continue
            raw_index = _read_menu_input(
                "请输入要删除的编号，或按 B 取消: ",
                hotkeys={"b"},
                max_number=len(paths),
            ).strip().lower()
            if raw_index == "b":
                continue
            if raw_index.isdigit():
                _remove_source_path_from_list(cfg, int(raw_index))
            continue

        if list_action == "c":
            confirm = _read_menu_input("确认清空源端列表? (y/N): ", hotkeys={"y", "n"}).strip().lower()
            if confirm in {"y", "yes"}:
                _clear_source_path_list(cfg)


# ================================
# 解析浏览器序号输入
# ================================
def _parse_item_indexes(raw_value, max_number):
    indexes = []
    seen = set()
    for chunk in re.split(r"[,，\s]+", str(raw_value or "").strip()):
        if not chunk:
            continue
        match = re.match(r"^(\d+)\s*[-~～]\s*(\d+)$", chunk)
        if match:
            start = int(match.group(1))
            end = int(match.group(2))
            if start > end:
                start, end = end, start
            values = range(start, end + 1)
        elif chunk.isdigit():
            values = [int(chunk)]
        else:
            continue
        for value in values:
            if 1 <= value <= max_number and value not in seen:
                indexes.append(value - 1)
                seen.add(value)
    return indexes


# ================================
# 添加浏览器中指定条目
# ================================
def _prompt_add_browser_items_to_list(cfg, page, label="文件/目录"):
    items = page.items or []
    if not items:
        print("\n当前没有可添加的条目。\n")
        return
    raw_value = _read_input(f"请输入要添加的{label}序号（支持空格/逗号/范围，如 1 3 5-8）: ").strip()
    indexes = _parse_item_indexes(raw_value, len(items))
    if not indexes:
        print("\n未选择有效序号。\n")
        return
    for index in indexes:
        _add_source_path_to_list(cfg, items[index].path)


# ================================
# 输入浏览器筛选条件
# ================================
def _prompt_filter_terms(current_filter):
    prompt = "筛选关键字（多个用空格分隔，空值清除）"
    if current_filter:
        prompt += f" [{current_filter}]"
    prompt += ": "
    return _read_input(prompt).strip()


def browse_local_config(cfg, section, page_size=30):
    current_path = cfg.get(section, "path", fallback="").strip() or os.getcwd()
    page_no = 1
    filters = ""
    is_source_list_mode = (
        section == SOURCE_SECTION
        and get_source_selection_mode(cfg) == SOURCE_MODE_LIST
    )

    while True:
        try:
            page = list_local_path(current_path, page=page_no, page_size=page_size, filters=filters)
        except Exception as exc:
            print(f"无法读取本地目录: {exc}")
            return

        subtitle_lines = [
            (f"当前位置: {_shorten_text(page.path, 72)}", Fore.YELLOW),
            (_browser_count_text(page), Fore.MAGENTA),
        ]
        if filters:
            subtitle_lines.append((f"筛选: {filters}", Fore.CYAN))
        if is_source_list_mode:
            subtitle_lines.append("编号进入目录；F 添加指定文件/目录到迁移列表；A 添加当前目录；K 筛选。")
            footer = [
                _hotkey_segments("[F] 添加指定项至迁移列表    [A] 添加当前目录至迁移列表"),
                _hotkey_segments("[K] 筛选    [B] 上一层/返回    [N] 下一页    [P] 上一页"),
            ]
            hotkeys = {"f", "a", "k", "b", "n", "p"}
            prompt_text = "\n选择编号进入目录，或按 F/A/K/B/N/P: "
        else:
            subtitle_lines.append("目录可进入；S 保存当前目录；K 筛选；B 上一层/返回；N/P 翻页。")
            footer = ["[S] 保存当前目录    [K] 筛选    [B] 上一层/返回    [N] 下一页    [P] 上一页"]
            hotkeys = {"s", "k", "b", "n", "p"}
            prompt_text = "\n选择编号，或按 S/K/B/N/P: "
        _print_box("本地目录浏览", _browser_lines(page), footer_lines=footer, subtitle_lines=subtitle_lines)

        answer = _read_menu_input(
            prompt_text,
            hotkeys=hotkeys,
            max_number=len(page.items or []),
        ).strip()
        lowered = answer.lower()

        if lowered == "k":
            filters = _prompt_filter_terms(filters)
            page_no = 1
            continue
        if lowered == "f" and is_source_list_mode:
            _prompt_add_browser_items_to_list(cfg, page)
            continue
        if lowered == "a" and is_source_list_mode:
            _add_source_path_to_list(cfg, page.path)
            continue
        if lowered == "s":
            if is_source_list_mode:
                print("列表模式请使用 A 添加当前目录，或选择编号添加文件/目录。")
                continue
            _save_browser_selection(cfg, section, path=page.path)
            return
        if lowered == "b":
            parent = os.path.dirname(page.path)
            if parent and parent != page.path:
                current_path = parent
                page_no = 1
                filters = ""
            else:
                return
            continue
        if lowered == "n":
            if page.has_next:
                page_no += 1
            continue
        if lowered == "p":
            page_no = max(1, page_no - 1)
            continue
        if answer.isdigit():
            item_index = int(answer) - 1
            if 0 <= item_index < len(page.items):
                item = page.items[item_index]
                if is_source_list_mode:
                    if item.kind == "dir":
                        current_path = item.path
                        page_no = 1
                        filters = ""
                    else:
                        print("列表模式请按 F 后输入文件序号添加指定文件。")
                    continue
                if item.kind == "dir":
                    current_path = item.path
                    page_no = 1
                    filters = ""
                else:
                    print("文件仅展示属性，请选择目录进入或保存当前目录。")
            continue

        if is_source_list_mode:
            print("请输入有效编号，或输入 F / A / K / B / N / P。")
        else:
            print("请输入有效编号，或输入 S / K / B / N / P。")


def browse_remote_config(cfg, section, page_size=30):
    ak = _decrypt_from_config(cfg, section, "ak")
    sk = _decrypt_from_config(cfg, section, "sk")
    endpoint = cfg.get(section, "endpoint", fallback="").strip()
    bucket = cfg.get(section, "bucket", fallback="").strip()
    prefix = sanitize_key(cfg.get(section, "prefix", fallback="")).strip("/")

    if not (ak and sk and endpoint):
        print("请先配置 ak / sk / endpoint，再使用远端浏览。")
        return

    request_timeout = 60
    if cfg.has_section("UPLOAD"):
        request_timeout = cfg.getint("UPLOAD", "request_timeout", fallback=60)

    try:
        client = create_obs_client(ak, sk, endpoint, request_timeout=request_timeout)
    except Exception as exc:
        print(f"创建远端客户端失败: {exc}")
        return

    mode = "objects" if bucket else "buckets"
    is_source_list_mode = (
        section == SOURCE_SECTION
        and get_source_selection_mode(cfg) == SOURCE_MODE_LIST
    )
    bucket_page_no = 1
    object_page_no = 1
    object_markers = [None]
    filters = ""
    object_count_cache = {}
    object_count_threads = {}

    def get_cached_or_start_count(count_bucket, count_prefix):
        count_key = (count_bucket, count_prefix)
        if count_key in object_count_cache:
            return object_count_cache[count_key]

        if count_key not in object_count_threads:
            def count_worker():
                try:
                    object_count_cache[count_key] = count_remote_prefix_items(
                        client,
                        count_bucket,
                        count_prefix,
                    )
                except Exception:
                    object_count_cache[count_key] = None

            thread = threading.Thread(target=count_worker, daemon=True)
            object_count_threads[count_key] = thread
            thread.start()

        return None

    while True:
        try:
            if mode == "buckets":
                page = list_remote_buckets(client, page=bucket_page_no, page_size=page_size)
            else:
                marker = None if filters else (
                    object_markers[object_page_no - 1] if object_page_no - 1 < len(object_markers) else None
                )
                page = list_remote_prefix(
                    client,
                    bucket,
                    prefix,
                    marker=marker,
                    page=object_page_no,
                    page_size=page_size,
                    filters=filters,
                )
        except Exception as exc:
            print(f"远端列表读取失败: {exc}")
            if mode == "objects":
                answer = _read_menu_input("是否返回桶列表? (y/N): ", hotkeys={"y", "n"}).strip().lower()
                if answer in {"y", "yes"}:
                    mode = "buckets"
                    bucket_page_no = 1
                    continue
            return

        if mode == "objects" and page.next_marker:
            if len(object_markers) <= object_page_no:
                object_markers.append(page.next_marker)
            else:
                object_markers[object_page_no] = page.next_marker

        if mode == "buckets":
            total_items = page.total_known
        else:
            if page.has_next:
                total_items = get_cached_or_start_count(bucket, prefix)
            else:
                total_items = len(page.items or [])

        subtitle_lines = [
            (f"Endpoint: {_shorten_text(endpoint, 72)}", Fore.LIGHTBLACK_EX),
            "选择目录可进入；目录列表大于10需要手动键入回车跳转\nS 保存当前桶/目录；列表模式下 F 添加指定目录/对象，A 添加当前位置。",
        ]
        if mode == "buckets":
            subtitle_lines.append((_browser_count_text(page, total=total_items), Fore.MAGENTA))
        else:
            subtitle_lines.append((f"当前: {bucket}/{prefix or '/'}", Fore.YELLOW))
            subtitle_lines.append((_browser_count_text(page, total=total_items), Fore.MAGENTA))
            if filters:
                subtitle_lines.append((f"筛选: {filters}", Fore.CYAN))
        if is_source_list_mode:
            footer = [
                _hotkey_segments("[F] 添加指定项至迁移列表    [A] 添加当前位置至迁移列表"),
                _hotkey_segments("[K] 筛选    [B] 上一层/返回    [N] 下一页    [P] 上一页    [R] 刷新"),
            ]
            hotkeys = {"f", "a", "k", "b", "n", "p", "r"}
            prompt_text = "\n选择编号进入目录，或按 F/A/K/B/N/P/R: "
        else:
            footer = ["[S] 保存当前位置到配置文件    [K] 筛选    [B] 上一层/返回    [N] 下一页    [P] 上一页    [R] 刷新"]
            hotkeys = {"s", "k", "b", "n", "p", "r"}
            prompt_text = "\n选择编号，或按 S/K/B/N/P/R: "
        _print_box(
            _remote_browser_title(section, bucket if mode == "objects" else "", prefix),
            _browser_lines(page),
            footer_lines=footer,
            subtitle_lines=subtitle_lines,
        )

        answer = _read_menu_input(
            prompt_text,
            hotkeys=hotkeys,
            max_number=len(page.items or []),
        ).strip()
        lowered = answer.lower()

        if lowered == "r":
            continue
        if lowered == "k":
            if mode == "buckets":
                print("请先选择一个桶，进入后再筛选目录/对象。")
            else:
                filters = _prompt_filter_terms(filters)
                object_page_no = 1
                object_markers = [None]
            continue
        if lowered == "f" and is_source_list_mode:
            if mode == "buckets":
                print("请先选择一个桶，进入后再添加。")
            else:
                _prompt_add_browser_items_to_list(cfg, page, label="目录/对象")
            continue
        if lowered == "a" and is_source_list_mode:
            if mode == "buckets":
                print("请先选择一个桶，进入后再添加。")
            else:
                _add_source_path_to_list(cfg, prefix or "")
            continue
        if lowered == "s":
            if is_source_list_mode:
                print("列表模式请使用 A 添加当前位置，或选择编号添加目录/对象。")
                continue
            if mode == "buckets":
                print("请先选择一个桶，进入后再保存。")
            else:
                _save_browser_selection(cfg, section, bucket=bucket, prefix=prefix)
                return
            continue
        if lowered == "b":
            if mode == "buckets":
                return
            parent = parent_prefix(prefix)
            if parent != prefix:
                prefix = parent
                object_page_no = 1
                object_markers = [None]
                filters = ""
            else:
                mode = "buckets"
                bucket_page_no = 1
                filters = ""
            continue
        if lowered == "n":
            if page.has_next:
                if mode == "buckets":
                    bucket_page_no += 1
                else:
                    object_page_no += 1
            continue
        if lowered == "p":
            if mode == "buckets":
                bucket_page_no = max(1, bucket_page_no - 1)
            else:
                object_page_no = max(1, object_page_no - 1)
            continue
        if answer.isdigit():
            item_index = int(answer) - 1
            if 0 <= item_index < len(page.items):
                item = page.items[item_index]
                if item.kind == "bucket":
                    bucket = item.name
                    prefix = ""
                    mode = "objects"
                    object_page_no = 1
                    object_markers = [None]
                    filters = ""
                elif item.kind == "dir":
                    prefix = item.path
                    object_page_no = 1
                    object_markers = [None]
                    filters = ""
                else:
                    if is_source_list_mode:
                        print("列表模式请按 F 后输入对象序号添加指定对象。")
                        continue
                    print("文件仅展示属性，请选择目录进入或保存当前目录。")
            continue

        if is_source_list_mode:
            print("请输入有效编号，或输入 F / A / K / B / N / P / R。")
        else:
            print("请输入有效编号，或输入 S / K / B / N / P / R。")


def browse_storage_config(cfg, group_id):
    if group_id == "source":
        section = SOURCE_SECTION
        default_mode = MODE_LOCAL
    elif group_id == "target":
        section = TARGET_SECTION
        default_mode = MODE_S3
    else:
        return

    mode = _normalize_mode(cfg.get(section, "type", fallback=default_mode), default=default_mode)
    if mode == MODE_S3:
        browse_remote_config(cfg, section)
    else:
        browse_local_config(cfg, section)


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

    if (
        group_id == "source"
        and cfg.has_section("PATH")
        and get_source_selection_mode(cfg) == SOURCE_MODE_LIST
    ):
        rows.append(("PATH", "migration_list_file", cfg.get("PATH", "migration_list_file", fallback="")))

    return rows


# ================================
# 生成存储端简要摘要
# ================================
def _storage_summary(mode, path_value, endpoint, bucket, prefix, selection_mode=SOURCE_MODE_DIRECTORY, paths_value="", paths_count=None):
    normalized_mode = _normalize_mode(mode, default=MODE_LOCAL) or MODE_LOCAL
    normalized_selection_mode = _normalize_source_selection_mode(selection_mode, default=SOURCE_MODE_DIRECTORY)
    if normalized_mode == MODE_LOCAL:
        if normalized_selection_mode == SOURCE_MODE_LIST:
            count = len(parse_source_path_list(paths_value)) if paths_count is None else paths_count
            return f"local | list | {count} 项"
        return f"local | {_shorten_text(path_value or '-', 56)}"

    scheme = detect_storage_scheme(endpoint, fallback="s3")
    if normalized_selection_mode == SOURCE_MODE_LIST:
        count = len(parse_source_path_list(paths_value)) if paths_count is None else paths_count
        return f"{normalized_mode} | list | {bucket or '-'} | {count} 项"
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
            cfg.get(SOURCE_SECTION, "selection_mode", fallback=SOURCE_MODE_DIRECTORY),
            cfg.get(SOURCE_SECTION, "paths", fallback=""),
            len(_source_path_list(cfg)),
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
            f"scan_workers={cfg.get('SCAN', 'scan_workers', fallback='-')} | "
            f"batch={cfg.get('SCAN', 'batch_size', fallback='-')} | "
            f"queue={cfg.get('SCAN', 'queue_size', fallback='-')}"
        )

    if group_id == "transfer":
        return (
            f"upload_workers={cfg.get('UPLOAD', 'workers', fallback='-')} | "
            f"part={cfg.get('UPLOAD', 'part_size', fallback='-')} | "
            f"threshold={cfg.get('UPLOAD', 'multipart_threshold', fallback='-')}"
        )

    if group_id == "scheduler":
        return (
            f"check_workers={cfg.get('UPLOAD', 'checkers', fallback='-')} | "
            f"qps={cfg.get('UPLOAD', 'rate_limit', fallback='-')}/{cfg.get('UPLOAD', 'rate_limit_burst', fallback='-')} | "
            f"conn={cfg.get('UPLOAD', 'max_connections', fallback='-')}"
        )

    if group_id == "check":
        return (
            f"compare_mode={cfg.get('CHECK', 'target_compare_mode', fallback='-')} | "
            f"verify_after={cfg.get('CHECK', 'verify_after_upload', fallback='-')} | "
            f"head_check={_format_bool_text(cfg.get('CHECK', 'enable_head_check', fallback='true'))}"
        )

    if group_id == "path":
        return (
            f"logs={_shorten_text(cfg.get('PATH', 'log_dir', fallback='-'), 18)} | "
            f"state={_shorten_text(cfg.get('PATH', 'state_dir', fallback='-'), 18)} | "
            f"failed={_shorten_text(cfg.get('PATH', 'failed_dir', fallback='-'), 18)}"
        )

    if group_id == "ui":
        return (
            f"prompt_config={_format_bool_text(cfg.get('UI', 'prompt_config', fallback='true'))} | "
            f"dashboard={_format_bool_text(cfg.get('UI', 'show_dashboard', fallback='true'))} | "
            f"lang={get_ui_language(cfg)}"
        )

    return ""


# ================================
# 为摘要片段配色
# ================================
def _summary_value_color(value):
    text = str(value or "").strip().lower()
    if "://" in text or "\\" in text:
        return _bright(MENU_COLOR_LIST)
    if text in {MODE_LOCAL, MODE_S3, SOURCE_MODE_DIRECTORY, "auto", "head", "etag", "size", "none", "zh", "en"}:
        return _bright(MENU_COLOR_MODE)
    if text == SOURCE_MODE_LIST or text.endswith("项"):
        return _bright(MENU_COLOR_LIST)
    if text in {"true", "false"}:
        return _bright(MENU_COLOR_TRUE if text == "true" else MENU_COLOR_FALSE)
    if re.match(r"^\d+(\.\d+)?[a-z]*(/[0-9.]+[a-z]*)?$", text):
        return _bright(MENU_COLOR_PARAM)
    if "/" in text or "\\" in text:
        return _bright(MENU_COLOR_PATH)
    return MENU_COLOR_VALUE


def _summary_segments(summary):
    parts = str(summary or "").split(" | ")
    segments = []
    for index, part in enumerate(parts):
        if index > 0:
            segments.append((" | ", Fore.LIGHTBLACK_EX))

        if "=" in part:
            key, value = part.split("=", 1)
            segments.append((key, _bright(MENU_COLOR_KEY)))
            segments.append(("=", MENU_COLOR_MUTED))
            segments.append((value, _summary_value_color(value)))
        elif "://" in part or "\\" in part:
            segments.append((part, _summary_value_color(part)))
        elif "/" in part:
            slash_parts = part.split("/")
            for slash_index, slash_part in enumerate(slash_parts):
                if slash_index > 0:
                    segments.append(("/", MENU_COLOR_MUTED))
                segments.append((slash_part, _summary_value_color(slash_part)))
        else:
            segments.append((part, _summary_value_color(part)))
    return segments


# ================================
# 为配置值选择颜色
# ================================
def _config_value_color(key, value):
    text = str(value or "").strip().lower()
    if key == "selection_mode":
        return _bright(MENU_COLOR_LIST if text == SOURCE_MODE_LIST else MENU_COLOR_MODE)
    if key in {"type", "selection_mode", "target_compare_mode", "verify_after_upload", "language"}:
        return _bright(MENU_COLOR_MODE)
    if text in {"true", "false"}:
        return _bright(MENU_COLOR_TRUE if text == "true" else MENU_COLOR_FALSE)
    if text.endswith("项"):
        return _bright(MENU_COLOR_LIST)
    if re.match(r"^\d+(\.\d+)?[a-z]*$", text):
        return _bright(MENU_COLOR_PARAM)
    if text and text not in {"-", "(空)"}:
        return _bright(MENU_COLOR_PATH if ("\\" in text or "/" in text) else MENU_COLOR_VALUE)
    return MENU_COLOR_MUTED


# ================================
# 渲染快捷键提示片段
# ================================
def _hotkey_segments(text):
    segments = []
    cursor = 0
    for match in re.finditer(r"\[[A-Z]\]", str(text or "")):
        if match.start() > cursor:
            segments.append((text[cursor:match.start()], MENU_COLOR_ACTION))
        segments.append((match.group(0), _bright(MENU_COLOR_HOTKEY)))
        cursor = match.end()
    if cursor < len(text):
        segments.append((text[cursor:], MENU_COLOR_ACTION))
    return segments or [(str(text or ""), MENU_COLOR_ACTION)]


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
        title_text = _fit_display(f"[{title}]", title_width + 2)
        body_lines.append(
            [
                (f"{index}. ", Fore.WHITE),
                (title_text, _bright(MENU_COLOR_TITLE)),
                ("  ", Fore.WHITE),
                *_summary_segments(_shorten_text(summary, 56)),
            ]
        )
        mapping[str(index)] = group["id"]

    subtitle_lines = [
        f"配置文件：{_shorten_text(resolve_config_file(), 62)}",
        "提示：先选分组进入详情，修改后会自动保存。",
    ]
    footer_lines = ["[S] 浏览源端目录    [T] 浏览目标端目录    [Y] 启动迁移程序    [Q] 退出程序"]
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
        if section == SOURCE_SECTION and key == "paths":
            paths = _source_path_list(cfg)
            shown_value = f"{len(paths)} 项" if paths else "(空)"
        if section == "PATH" and key == "migration_list_file":
            shown_value = _shorten_text(value or DEFAULT_CONFIG["PATH"]["migration_list_file"], 66)
        if key in {"rate_limit", "rate_limit_burst"} and shown_value not in {"", None}:
            shown_value = f"{shown_value} req/s"

        desc = CONFIG_DESC.get(f"{section}.{key}", "")
        if group_id == "source" and section == "PATH" and key == "migration_list_file":
            desc = "源端列表模式清单文件与列表管理（SOURCE.selection_mode=list 时使用）"
        if desc:
            body_lines.append([(f"{index}. ", Fore.WHITE), (desc, _bright(MENU_COLOR_DESC))])
            body_lines.append(
                [
                    ("   └─ ", MENU_COLOR_MUTED),
                    (key, _bright(MENU_COLOR_KEY)),
                    (" = ", MENU_COLOR_MUTED),
                    (str(shown_value), _config_value_color(key, shown_value)),
                ]
            )
        else:
            body_lines.append(
                [
                    (f"{index}. ", Fore.WHITE),
                    (key, _bright(MENU_COLOR_KEY)),
                    (" = ", MENU_COLOR_MUTED),
                    (str(shown_value), _config_value_color(key, shown_value)),
                ]
            )

        mapping[str(index)] = (section, key)

    subtitle_lines = [_summary_segments(_group_summary(cfg, group_id))]
    if section_hint:
        subtitle_lines.append(section_hint)

    if group_id == "source":
        footer_lines = ["[O] 浏览桶/目录    [B] 返回上一级    [Y] 启动迁移程序    [Q] 退出程序"]
    elif group_id == "target":
        footer_lines = ["[O] 浏览桶/目录    [B] 返回上一级    [Y] 启动迁移程序    [Q] 退出程序"]
    else:
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
        if group_id == "source":
            answer = _read_menu_input(
                "\n请选择编号，或按 O/B/Y/Q: ",
                hotkeys={"o", "b", "y", "q"},
                max_number=len(mapping),
            ).strip()
        elif group_id == "target":
            answer = _read_menu_input(
                "\n请选择编号，或按 O/B/Y/Q: ",
                hotkeys={"o", "b", "y", "q"},
                max_number=len(mapping),
            ).strip()
        else:
            answer = _read_menu_input(
                "\n请选择编号，或按 B/Y/Q: ",
                hotkeys={"b", "y", "q"},
                max_number=len(mapping),
            ).strip()
        lowered = answer.lower()

        if lowered == "o" and group_id in {"source", "target"}:
            browse_storage_config(cfg, group_id)
            continue

        if lowered == "b":
            return "back"

        if lowered == "y":
            return "start"

        if lowered == "q":
            return "quit"

        if answer not in mapping:
            if group_id in {"source", "target"}:
                if group_id == "source":
                    print("请输入有效编号，或输入 O / B / Y / Q。")
                else:
                    print("请输入有效编号，或输入 O / B / Y / Q。")
            else:
                print("请输入有效编号，或输入 B / Y / Q。")
            continue

        section, key = mapping[answer]
        if section == "PATH" and key == "migration_list_file":
            prompt_source_path_list_action(cfg)
            continue

        desc = CONFIG_DESC.get(f"{section}.{key}", "")
        if desc:
            print(f"\n{desc}")

        if _is_sensitive(section, key):
            print(f"注意：敏感信息会以加密形式写入 {resolve_config_file()}")

        if key == "type" and section in {SOURCE_SECTION, TARGET_SECTION}:
            section_label = "source" if section == SOURCE_SECTION else "target"
            new_value = _prompt_mode(section_label, cfg.get(section, key, fallback=MODE_LOCAL), allow_empty=True)
        elif key == "selection_mode" and section == SOURCE_SECTION:
            new_value = _prompt_source_selection_mode(cfg.get(section, key, fallback=SOURCE_MODE_DIRECTORY), allow_empty=True)
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
        answer = _read_menu_input(
            "\n请选择分组编号，或按 S/T/Y/Q: ",
            hotkeys={"s", "t", "y", "q"},
            max_number=len(mapping),
        ).strip()
        lowered = answer.lower()

        if lowered == "s":
            browse_storage_config(cfg, "source")
            continue

        if lowered == "t":
            browse_storage_config(cfg, "target")
            continue

        if lowered == "y":
            print()
            return cfg

        if lowered == "q":
            print("\n已退出。\n")
            raise SystemExit(0)

        group_id = mapping.get(answer)
        if group_id is None:
            print("请输入有效分组编号，或输入 S / T / Y / Q。")
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
# 解析本地源端列表
# ================================
def parse_source_path_list(value):
    paths = []
    seen = set()
    for raw_line in str(value or "").splitlines():
        path_value = raw_line.strip()
        if not path_value:
            continue
        key = os.path.normcase(os.path.abspath(os.path.expanduser(path_value)))
        if key in seen:
            continue
        seen.add(key)
        paths.append(path_value)
    return paths


# ================================
# 序列化本地源端列表
# ================================
def serialize_source_path_list(paths):
    return "\n".join(str(item).strip() for item in paths if str(item).strip())


# ================================
# 获取源端选择模式
# ================================
def get_source_selection_mode(cfg):
    return _normalize_source_selection_mode(
        cfg.get(SOURCE_SECTION, "selection_mode", fallback=SOURCE_MODE_DIRECTORY),
        default=SOURCE_MODE_DIRECTORY,
    )


# ================================
# 生成本地源端标签
# ================================
def _local_source_label(source_selection_mode, source_path, source_paths):
    if source_selection_mode == SOURCE_MODE_LIST:
        items = parse_source_path_list(source_paths)
        if not items:
            return "source_list_empty"
        return f"source_list_{len(items)}"
    return source_path


def _s3_source_label(source_selection_mode, source_bucket, source_prefix, source_paths):
    if source_selection_mode == SOURCE_MODE_LIST:
        items = parse_source_path_list(source_paths)
        return f"{source_bucket}/source_list_{len(items)}" if items else f"{source_bucket}/source_list_empty"
    return _source_label(MODE_S3, "", source_bucket, source_prefix)


# ================================
# 判断本地路径是否包含通配符
# ================================
def _has_local_glob(path_value):
    return glob.has_magic(str(path_value or ""))


# ================================
# 解析本地通配符的静态根目录
# ================================
def _local_glob_static_root(path_value):
    text = str(path_value or "").strip()
    if not text:
        return "."

    drive, tail = os.path.splitdrive(text)
    split_chars = re.escape(os.sep + (os.altsep or ""))
    parts = [item for item in re.split(f"[{split_chars}]+", tail) if item]
    static_parts = []

    for part in parts:
        if glob.has_magic(part):
            break
        static_parts.append(part)

    is_abs = os.path.isabs(text)
    if not static_parts:
        if is_abs:
            if drive:
                return os.path.normpath(drive + os.sep)
            return os.path.normpath(os.sep)
        return "."

    prefix = os.path.join(*static_parts)
    if is_abs:
        if drive:
            return os.path.normpath(os.path.join(drive + os.sep, prefix))
        return os.path.normpath(os.path.join(os.sep, prefix))

    if drive:
        return os.path.normpath(os.path.join(drive, prefix))

    return os.path.normpath(prefix)


# ================================
# 判断子路径是否落在父路径下
# ================================
def _is_sub_path(child_path, parent_path):
    try:
        return os.path.commonpath([child_path, parent_path]) == parent_path
    except Exception:
        return False


# ================================
# 构建本地源扫描计划
# ================================
def build_local_source_plan(source_path):
    source_path = (source_path or "").strip()
    has_glob = _has_local_glob(source_path)

    if not has_glob:
        if os.path.isdir(source_path):
            return {
                "pattern": source_path,
                "has_glob": False,
                "base_dir": source_path,
                "match_count": 1,
                "entries": [
                    {
                        "type": "dir",
                        "path": source_path,
                        "base_dir": source_path,
                    }
                ],
            }

        return {
            "pattern": source_path,
            "has_glob": False,
            "base_dir": os.path.dirname(source_path) or ".",
            "match_count": 1,
            "entries": [
                {
                    "type": "file",
                    "path": source_path,
                    "base_dir": os.path.dirname(source_path) or ".",
                }
            ],
        }

    base_dir = _local_glob_static_root(source_path)
    raw_matches = glob.glob(source_path, recursive=True)
    dedup = {}
    for matched in sorted(raw_matches):
        abs_key = os.path.normcase(os.path.abspath(matched))
        if abs_key not in dedup:
            dedup[abs_key] = matched

    pruned_entries = []
    kept_dirs = []
    for abs_key in sorted(dedup, key=lambda item: (len(item), item)):
        matched_path = dedup[abs_key]
        if any(_is_sub_path(abs_key, parent_dir) for parent_dir in kept_dirs):
            continue

        entry_type = "dir" if os.path.isdir(matched_path) else "file"
        pruned_entries.append(
            {
                "type": entry_type,
                "path": matched_path,
                "base_dir": base_dir,
            }
        )
        if entry_type == "dir":
            kept_dirs.append(abs_key)

    return {
        "pattern": source_path,
        "has_glob": True,
        "base_dir": base_dir,
        "match_count": len(pruned_entries),
        "entries": pruned_entries,
    }


# ================================
# 构建本地源列表扫描计划
# ================================
def build_local_source_list_plan(source_paths):
    entries = []
    for path_value in parse_source_path_list(source_paths):
        entry_type = "dir" if os.path.isdir(path_value) else "file"
        entries.append(
            {
                "type": entry_type,
                "path": path_value,
                "base_dir": os.path.dirname(path_value) or ".",
            }
        )

    return {
        "pattern": "<source_list>",
        "has_glob": False,
        "base_dir": "",
        "match_count": len(entries),
        "entries": entries,
    }


def build_local_source_plan_from_config(cfg):
    source_selection_mode = get_source_selection_mode(cfg)
    if source_selection_mode == SOURCE_MODE_LIST:
        return build_local_source_list_plan(serialize_source_path_list(_source_path_list(cfg)))
    return build_local_source_plan(cfg.get(SOURCE_SECTION, "path", fallback="").strip())


def build_s3_source_entries_from_config(cfg):
    source_selection_mode = get_source_selection_mode(cfg)
    source_bucket = cfg.get(SOURCE_SECTION, "bucket", fallback="").strip()
    if source_selection_mode != SOURCE_MODE_LIST:
        return [
            {
                "bucket": source_bucket,
                "prefix": sanitize_key(cfg.get(SOURCE_SECTION, "prefix", fallback="")).strip("/"),
            }
        ]

    return [
        {
            "bucket": source_bucket,
            "prefix": sanitize_key(item).strip("/"),
        }
        for item in _source_path_list(cfg)
    ]


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
def init_config(prompt=True):
    print("\n首次运行，初始化配置\n")

    cfg = configparser.ConfigParser()
    for section, items in DEFAULT_CONFIG.items():
        cfg[section] = {}
        for key, default_value in items.items():
            if not prompt:
                cfg[section][key] = default_value
                continue

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
            if section == SOURCE_SECTION and key == "paths":
                paths = _source_path_list(cfg)
                shown_value = f"{len(paths)} 项" if paths else "(空)"
            if section == "PATH" and key == "migration_list_file":
                shown_value = _shorten_text(value or DEFAULT_CONFIG["PATH"]["migration_list_file"], 66)
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
        if section == "PATH" and key == "migration_list_file":
            prompt_source_path_list_action(cfg)
            mapping = show_config(cfg)
            choice = None
            continue

        desc = CONFIG_DESC.get(f"{section}.{key}", "")
        if desc:
            print(desc)

        if _is_sensitive(section, key):
            print(f"注意：敏感信息会以加密形式写入 {resolve_config_file()}")

        if key == "type" and section in {SOURCE_SECTION, TARGET_SECTION}:
            section_label = "source" if section == SOURCE_SECTION else "target"
            new_value = _prompt_mode(section_label, cfg.get(section, key, fallback=MODE_LOCAL), allow_empty=True)
        elif key == "selection_mode" and section == SOURCE_SECTION:
            new_value = _prompt_source_selection_mode(cfg.get(section, key, fallback=SOURCE_MODE_DIRECTORY), allow_empty=True)
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
def load_config(prompt=True):
    config_file = resolve_config_file()
    if not os.path.exists(config_file):
        return init_config(prompt=prompt)

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

    if cfg.has_option(SOURCE_SECTION, "local_mode"):
        legacy_selection_mode = cfg.get(SOURCE_SECTION, "local_mode", fallback="")
        if not cfg.has_option(SOURCE_SECTION, "selection_mode"):
            cfg.set(SOURCE_SECTION, "selection_mode", legacy_selection_mode)
        cfg.remove_option(SOURCE_SECTION, "local_mode")
        updated = True

    if cfg.has_option(SOURCE_SECTION, "paths_file"):
        legacy_paths_file = cfg.get(SOURCE_SECTION, "paths_file", fallback="").strip()
        current_list_file = cfg.get("PATH", "migration_list_file", fallback="").strip()
        default_list_file = DEFAULT_CONFIG["PATH"]["migration_list_file"]
        if legacy_paths_file and current_list_file in {"", default_list_file}:
            cfg.set("PATH", "migration_list_file", legacy_paths_file)
        cfg.remove_option(SOURCE_SECTION, "paths_file")
        updated = True

    if cfg.has_option(SOURCE_SECTION, "paths"):
        legacy_paths = parse_source_path_list(cfg.get(SOURCE_SECTION, "paths", fallback=""))
        if legacy_paths:
            list_file = _source_paths_file(cfg)
            current_paths = _read_source_paths_file(list_file)
            merged_paths = list(current_paths)
            for legacy_path in legacy_paths:
                if legacy_path not in merged_paths:
                    merged_paths.append(legacy_path)
            if merged_paths != current_paths:
                _write_source_paths_file(list_file, merged_paths)
        cfg.remove_option(SOURCE_SECTION, "paths")
        updated = True

    raw_source_selection_mode = cfg.get(SOURCE_SECTION, "selection_mode", fallback=SOURCE_MODE_DIRECTORY)
    normalized_source_selection_mode = _normalize_source_selection_mode(
        raw_source_selection_mode,
        default=SOURCE_MODE_DIRECTORY,
    )
    if normalized_source_selection_mode is None:
        normalized_source_selection_mode = SOURCE_MODE_DIRECTORY
    if raw_source_selection_mode != normalized_source_selection_mode:
        cfg.set(SOURCE_SECTION, "selection_mode", normalized_source_selection_mode)
        updated = True

    if updated:
        write_config_with_comments(cfg)
        print(f"\n检测到新配置项，已自动更新 {resolve_config_file()}\n")

    if prompt and should_prompt_config(cfg):
        run_config_menu(cfg)

    return cfg


def load_config_for_web():
    return load_config(prompt=False)


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

            for key, value in _ordered_items_for_section(cfg, section):
                desc = CONFIG_DESC.get(f"{section}.{key}", "")
                if desc:
                    f.write(f"# {desc}\n")
                value_text = str(value)
                if "\n" in value_text:
                    lines = value_text.splitlines()
                    first_line = lines[0] if lines else ""
                    f.write(f"{key} = {first_line}\n")
                    for line in lines[1:]:
                        f.write(f"    {line}\n")
                    f.write("\n")
                else:
                    f.write(f"{key} = {value_text}\n\n")

            f.write("\n")


# ================================
# 校验配置有效性
# ================================
def validate_config(cfg):
    source_type = _normalize_mode(cfg.get(SOURCE_SECTION, "type", fallback=MODE_LOCAL), default=MODE_LOCAL)
    target_type = _normalize_mode(cfg.get(TARGET_SECTION, "type", fallback=MODE_S3), default=MODE_S3)

    source_path = cfg.get(SOURCE_SECTION, "path", fallback="").strip()
    source_selection_mode = get_source_selection_mode(cfg)
    source_paths = serialize_source_path_list(_source_path_list(cfg))
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
        if source_selection_mode not in {SOURCE_MODE_DIRECTORY, SOURCE_MODE_LIST}:
            print("❌ SOURCE.selection_mode 仅支持 directory 或 list")
            sys.exit(1)

        if source_selection_mode == SOURCE_MODE_LIST:
            paths = _source_path_list(cfg)
            if not paths:
                print("❌ PATH.migration_list_file 未配置或列表为空，列表模式至少需要一个文件或目录")
                sys.exit(1)
            for path_value in paths:
                if not os.path.exists(path_value):
                    print(f"❌ 源端列表文件中的路径不存在：{path_value}")
                    sys.exit(1)
        else:
            if not source_path:
                print("❌ SOURCE.path 未配置")
                sys.exit(1)

            if _has_local_glob(source_path):
                source_plan = build_local_source_plan(source_path)
                if not source_plan["entries"]:
                    print("❌ SOURCE.path 通配符未匹配到任何本地文件或目录")
                    sys.exit(1)
            elif not os.path.exists(source_path):
                print("❌ SOURCE.path 不存在")
                sys.exit(1)
    else:
        if source_selection_mode not in {SOURCE_MODE_DIRECTORY, SOURCE_MODE_LIST}:
            print("❌ SOURCE.selection_mode 仅支持 directory 或 list")
            sys.exit(1)
        if not source_ak or not source_sk:
            print("❌ SOURCE.ak / SOURCE.sk 未配置")
            sys.exit(1)
        if not source_endpoint:
            print("❌ SOURCE.endpoint 未配置")
            sys.exit(1)
        if not source_bucket:
            print("❌ SOURCE.bucket 未配置")
            sys.exit(1)
        if source_selection_mode == SOURCE_MODE_LIST and not _source_path_list(cfg):
            print("❌ PATH.migration_list_file 未配置或列表为空，S3 列表模式至少需要一个前缀或对象")
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
def run_migration(cfg, controls=None):
    source_type = _normalize_mode(cfg.get(SOURCE_SECTION, "type", fallback=MODE_LOCAL), default=MODE_LOCAL)
    target_type = _normalize_mode(cfg.get(TARGET_SECTION, "type", fallback=MODE_S3), default=MODE_S3)

    source_path = cfg.get(SOURCE_SECTION, "path", fallback="").strip()
    source_selection_mode = get_source_selection_mode(cfg)
    source_paths = serialize_source_path_list(_source_path_list(cfg))
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

    source_label = (
        _local_source_label(source_selection_mode, source_path, source_paths)
        if source_type == MODE_LOCAL
        else _s3_source_label(source_selection_mode, source_bucket, source_prefix, source_paths)
    )
    local_source_plan = build_local_source_plan_from_config(cfg) if source_type == MODE_LOCAL else None
    s3_source_entries = build_s3_source_entries_from_config(cfg) if source_type == MODE_S3 else None

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
    runtime_output_paths = [
        report_dir,
        log_dir,
        state_dir,
        failed_dir,
    ]

    log_file = build_log_file(log_dir, source_label)
    setup_logger(log_file)
    logging.getLogger().propagate = False
    if local_source_plan is not None and local_source_plan["has_glob"]:
        logging.info(
            "[SOURCE_GLOB] pattern=%s preserve_root=%s matched_entries=%s",
            source_path,
            local_source_plan["base_dir"],
            local_source_plan["match_count"],
        )

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
    print("⏳ 正在准备断点数据库与目标索引，请稍候...")
    startup_prepare_begin = time.time()
    checkpoint = Checkpoint(db_path)
    if target_type == MODE_S3 and compare_mode != "head_only":
        checkpoint.reset_obs_index()
    startup_prepare_cost = time.time() - startup_prepare_begin
    if startup_prepare_cost >= 1:
        print(f"✓ 启动预处理完成，用时 {startup_prepare_cost:.1f}s")
    logging.info("[STARTUP] checkpoint/index prepare cost=%.2fs", startup_prepare_cost)

    is_local_single_file = (
        source_type == MODE_LOCAL
        and local_source_plan is not None
        and not local_source_plan["has_glob"]
        and len(local_source_plan["entries"]) == 1
        and local_source_plan["entries"][0]["type"] == "file"
    )
    progress = Progress()
    check_queue = queue.Queue(maxsize=cfg.getint("SCAN", "queue_size"))
    task_queue = queue.Queue(maxsize=cfg.getint("SCAN", "queue_size"))

    scan_controller = None
    if not is_local_single_file and scan_workers > 1:
        scan_controller = AdaptiveScanController(
            check_queue,
            max_workers=scan_workers,
            min_workers=resolve_min_scan_workers(scan_workers),
            controls=controls,
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
    if controls is not None:
        controls.update_status(
            logs={
                "log_file": log_file,
                "log_dir": log_dir,
                "state_dir": state_dir,
                "report_dir": report_dir,
                "report_file": reporter.file,
                "summary_file": getattr(reporter, "summary_file", ""),
                "failed_dir": failed_dir,
            }
        )
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
        controls=controls,
    )
    checker_handler = TaskChecker(uploader, task_queue, controls=controls)
    transfer_handler = TaskTransfer(uploader)
    checker_scheduler = Scheduler(
        check_queue,
        checker_handler,
        workers=checker_workers,
        stage_name="check",
        stall_timeout=worker_stall_timeout,
        controls=controls,
    )
    scheduler = Scheduler(
        task_queue,
        transfer_handler,
        workers=workers,
        stage_name="upload",
        stall_timeout=worker_stall_timeout,
        controls=controls,
    )

    enable_index_build = target_type == MODE_S3 and compare_mode != "head_only"
    index_workers = min(8, max(2, checker_workers // 32)) if enable_index_build else 1
    index_stop_event = threading.Event() if enable_index_build else None

    pipeline_status = {
        "index": "pending" if enable_index_build else "n/a",
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

    def publish_controls_status():
        if controls is None:
            return
        controls.update_status(
            progress=progress.snapshot(),
            pipeline=get_status(),
            workers={
                "check": checker_scheduler.get_status_snapshot(),
                "upload": scheduler.get_status_snapshot(),
            },
            queues={
                "check": {
                    "current": check_queue.qsize(),
                    "max": getattr(check_queue, "maxsize", 0),
                    "unfinished": check_queue.unfinished_tasks,
                },
                "transfer": {
                    "current": task_queue.qsize(),
                    "max": getattr(task_queue, "maxsize", 0),
                    "unfinished": task_queue.unfinished_tasks,
                },
            },
        )

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
        language=get_ui_language(cfg),
    )

    def run_index():
        if not enable_index_build:
            set_status("index", "n/a")
            return

        set_status("index", "running")
        try:
            completed = build_obs_index(
                target_ak,
                target_sk,
                target_endpoint,
                target_bucket,
                target_prefix,
                checkpoint,
                stop_event=index_stop_event,
                low_level_retries=low_level_retries,
                low_level_retry_sleep=low_level_retry_sleep,
                request_timeout=request_timeout,
                workers=index_workers,
                governor=uploader_module._governor,
            )
        except Exception as exc:
            set_status("index", "error")
            record_background_error(exc)
            raise
        else:
            set_status("index", "done" if completed else "done (early stop)")

    index_thread = threading.Thread(target=run_index, daemon=True) if enable_index_build else None
    scan_thread = None
    scan_done_event = threading.Event()

    def start_work():
        nonlocal scan_thread

        if controls is not None and controls.stop_requested():
            set_status("scan", "stopped")
            set_status("check", "stopped")
            scan_done_event.set()
            if index_stop_event is not None:
                index_stop_event.set()
            publish_controls_status()
            return

        progress.start()
        checker_scheduler.start()
        scheduler.start()
        set_status("check", "running")

        if index_thread is not None:
            index_thread.start()

        if is_local_single_file:
            single_entry = local_source_plan["entries"][0]
            single_path = single_entry["path"]
            stat_result = os.stat(single_path)
            filename = os.path.basename(single_path)
            if hasattr(reporter, "track_task"):
                reporter.track_task(single_path, size=stat_result.st_size)
            check_queue.put(
                {
                    "source_type": MODE_LOCAL,
                    "local": single_path,
                    "source_path": single_path,
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
                    scan_local_sources(
                        local_source_plan["entries"],
                        check_queue,
                        progress,
                        checkpoint,
                        reporter,
                        scan_workers,
                        scan_done_event,
                        scan_controller=scan_controller,
                        controls=controls,
                        excluded_roots=runtime_output_paths,
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
                scan_s3_sources(
                    s3_source_entries,
                    uploader_module._source_client,
                    source_bucket,
                    check_queue,
                    progress,
                    reporter,
                    scan_workers=scan_workers,
                    scan_done_event=scan_done_event,
                    source_scheme=uploader_module._source_uri_scheme,
                    scan_controller=scan_controller,
                    low_level_retries=low_level_retries,
                    low_level_retry_sleep=low_level_retry_sleep,
                    controls=controls,
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
            nonlocal interrupted

            with background_error_lock:
                if background_error[0] is not None:
                    raise background_error[0]

            index_finished = index_thread is None or not index_thread.is_alive()
            if controls is not None and controls.stop_requested():
                interrupted = True
                if index_stop_event is not None:
                    index_stop_event.set()
                scan_done_event.set()
                set_status("check", "stopping")
                if not is_local_single_file:
                    set_status("scan", "stopping")
                checker_scheduler.discard_pending_tasks()
                scheduler.discard_pending_tasks()
                publish_controls_status()
                return (
                    checker_scheduler.get_active_workers() == 0
                    and scheduler.get_active_workers() == 0
                    and index_finished
                )

            queues_finished = (
                scan_done_event.is_set()
                and check_queue.unfinished_tasks == 0
                and task_queue.unfinished_tasks == 0
                and checker_scheduler.get_active_workers() == 0
                and scheduler.get_active_workers() == 0
            )
            if queues_finished and index_stop_event is not None and index_thread is not None and index_thread.is_alive():
                index_stop_event.set()
            if queues_finished and index_finished:
                set_status("check", "done")
                publish_controls_status()
                return True
            publish_controls_status()
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
        if index_stop_event is not None:
            index_stop_event.set()
        if scan_thread is not None:
            scan_thread.join(timeout=5)
        if index_thread is not None:
            index_thread.join(timeout=5)

        checker_scheduler.stop()
        scheduler.stop()
        progress.stop()
        publish_controls_status()
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="OBS / S3 migration tool")
    parser.add_argument("--web", action="store_true", help="启动 Web 控制台")
    parser.add_argument("--web-reload", action="store_true", help="开发模式：Web UI 文件变更后自动重载控制台")
    return parser.parse_args(argv)


def should_start_web_ui(cfg, web_flag=False):
    if web_flag:
        return True
    return cfg.getboolean("WEB_UI", "enabled", fallback=False)


def _new_task_manager(cfg=None):
    state_dir = DEFAULT_CONFIG["PATH"]["state_dir"]
    if cfg is not None:
        state_dir = cfg.get("PATH", "state_dir", fallback=state_dir)
    persistence_path = os.path.join(resolve_runtime_path(state_dir), "web_tasks.json")
    return TaskManager(run_migration, persistence_path=persistence_path)


def _start_web_console(cfg, task_manager):
    host = cfg.get("WEB_UI", "host", fallback="127.0.0.1")
    port = cfg.get("WEB_UI", "port", fallback="8765")
    server = None
    try:
        server = WebConsoleServer(
            cfg,
            task_manager,
            load_config_for_web,
            write_config_with_comments,
            decrypt_value,
            encrypt_value,
            runtime_path_resolver=resolve_runtime_path,
        )
        server.start()
    except OSError as exc:
        _shutdown_web_runtime(server, task_manager)
        raise RuntimeError(f"Web 控制台启动失败 ({host}:{port}): {exc}") from exc
    except Exception as exc:
        _shutdown_web_runtime(server, task_manager)
        raise RuntimeError(f"Web 控制台启动失败 ({host}:{port}): {exc}") from exc

    print(f"Web 控制台: {server.url}")
    if cfg.getboolean("WEB_UI", "auto_open", fallback=False):
        try:
            webbrowser.open(server.url)
        except Exception as exc:
            print(f"⚠️ 浏览器自动打开失败: {exc}")
    return server


def _shutdown_web_runtime(server, task_manager, join_timeout=5):
    if task_manager is not None:
        try:
            if hasattr(type(task_manager), "stop_all"):
                task_manager.stop_all()
            else:
                task_manager.stop()
        except Exception as exc:
            print(f"⚠️ Web 任务停止失败: {exc}")

    if server is not None:
        try:
            server.stop()
        except Exception as exc:
            print(f"⚠️ Web 控制台关闭失败: {exc}")

    if task_manager is not None and hasattr(type(task_manager), "join_all"):
        try:
            task_manager.join_all(timeout=join_timeout)
        except Exception as exc:
            print(f"⚠️ Web 任务等待结束失败: {exc}")
    elif task_manager is not None and hasattr(task_manager, "join"):
        try:
            task_manager.join(timeout=join_timeout)
        except Exception as exc:
            print(f"⚠️ Web 任务等待结束失败: {exc}")


def _wait_for_web_console(server):
    print("Web 控制台已启动，按 Ctrl+C 退出。")
    try:
        while True:
            thread = getattr(server, "_thread", None)
            if thread is not None and not thread.is_alive():
                return
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n收到退出请求，正在关闭 Web 控制台...")


def _web_reload_watch_paths():
    return [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "core", "web_ui.py")),
    ]


def _web_reload_snapshot(paths=None):
    snapshot = {}
    for path in paths or _web_reload_watch_paths():
        try:
            snapshot[os.path.abspath(path)] = os.path.getmtime(path)
        except OSError:
            snapshot[os.path.abspath(path)] = None
    return snapshot


def _reload_web_modules():
    import core.web_ui as web_ui_module

    importlib.invalidate_caches()
    reloaded = importlib.reload(web_ui_module)
    globals()["WebConsoleServer"] = reloaded.WebConsoleServer


def _restart_web_console_for_reload(server, cfg, task_manager):
    print("检测到 Web UI 文件变更，正在重载控制台...")
    _reload_web_modules()
    if server is not None:
        try:
            server.stop()
        except Exception as exc:
            print(f"⚠️ 旧 Web 控制台关闭失败: {exc}")
    return _start_web_console(cfg, task_manager)


def _wait_for_web_console_reload(server, cfg, task_manager, poll_interval=0.5, watch_paths=None):
    print("Web 控制台开发热重载已启用；修改 Web UI 后刷新浏览器即可看到新页面。")
    watched = watch_paths or _web_reload_watch_paths()
    snapshot = _web_reload_snapshot(watched)
    try:
        while True:
            thread = getattr(server, "_thread", None)
            if thread is not None and not thread.is_alive():
                return server
            time.sleep(poll_interval)
            current = _web_reload_snapshot(watched)
            if current != snapshot:
                try:
                    server = _restart_web_console_for_reload(server, cfg, task_manager)
                except Exception as exc:
                    print(f"⚠️ Web 控制台热重载失败，继续使用当前服务: {exc}")
                snapshot = current
    except KeyboardInterrupt:
        print("\n收到退出请求，正在关闭 Web 控制台...")
        return server


def main(argv=None):
    ensure_dirs()

    args = parse_args(argv)
    cfg = load_config(prompt=False)
    start_web = should_start_web_ui(cfg, args.web or args.web_reload)
    if not start_web and should_prompt_config(cfg):
        run_config_menu(cfg)

    if not start_web:
        validate_config(cfg)
    _ensure_secret_fields_encrypted(cfg)

    if not start_web:
        return run_migration(cfg)

    task_manager = _new_task_manager(cfg)
    server = _start_web_console(cfg, task_manager)
    try:
        try:
            if args.web_reload:
                server = _wait_for_web_console_reload(server, cfg, task_manager) or server
            else:
                _wait_for_web_console(server)
        except KeyboardInterrupt:
            print("\n收到退出请求，正在关闭 Web 控制台...")
        return None
    finally:
        _shutdown_web_runtime(server, task_manager)


if __name__ == "__main__":
    main()

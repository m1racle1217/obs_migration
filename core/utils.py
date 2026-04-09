# core/utils.py
# -*- coding: utf-8 -*-

import os
import sys
import logging
import re



# ================================
# size 解析
# ================================

def parse_size(s):

    s = s.strip().upper()

    if s.endswith("K"):
        return int(float(s[:-1]) * 1024)

    if s.endswith("M"):
        return int(float(s[:-1]) * 1024 ** 2)

    if s.endswith("G"):
        return int(float(s[:-1]) * 1024 ** 3)

    return int(s)


# ================================
# 日志
# ================================

def setup_logger(log_file):

    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    # 防止重复 handler
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)

    logger.addHandler(fh)


# ================================
# 编码自动恢复
# ================================

def safe_decode(b):

    if isinstance(b, str):
        return b

    for enc in ("utf-8", "gbk", "gb18030", "latin1"):

        try:
            return b.decode(enc)

        except Exception:
            pass

    return b.decode("utf-8", "ignore")


def safe_path(p):

    if isinstance(p, bytes):
        return safe_decode(p)

    return p


# ================================
# OBS key 安全
# ================================

def normalize_obs_key(k):

    k = safe_path(k)

    # k = k.replace("\\", "/")

    while "//" in k:
        k = k.replace("//", "/")

    return k


# ================================
# Windows 长路径
# ================================

def fix_windows_path(p):

    if os.name != "nt":
        return p

    if p.startswith("\\\\?\\"):
        return p

    if len(p) > 240:
        return "\\\\?\\" + os.path.abspath(p)

    return p

def clean_path_to_utf8(p):

    if isinstance(p, str):
        raw = p.encode("utf-8", "surrogateescape")
    else:
        raw = p

    for enc in ("utf-8", "gb18030", "gbk"):
        try:
            return raw.decode(enc)
        except Exception:
            pass

    return raw.decode("utf-8", "replace")


def sanitize_key(key):

    if isinstance(key, bytes):
        key = key.decode("utf-8", "surrogateescape")

    key = key.replace("：", ":")

    # surrogate 转义
    key = key.encode(
        "utf-8",
        "surrogateescape"
    ).decode(
        "utf-8"
    )

    return key

def safe_log(s):

    if isinstance(s, bytes):

        s = s.decode(
            "utf-8",
            "surrogateescape"
        )

    return s.encode(
        "utf-8",
        "backslashreplace"
    ).decode("utf-8")

WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")


def normalize_relative_path(relative_bytes):
    """
    统一相对路径（核心函数）

    规则：
    - Windows：\ → /
    - Linux：保留 \（因为可能是合法文件名）
    - 去掉开头 /
    - 防止盘符污染
    """

    s = clean_path_to_utf8(relative_bytes)

    if os.name == "nt":
        # Windows：\ 是路径分隔符
        s = s.replace("\\", "/")
    else:
        # Linux：\ 是普通字符，不能替换
        pass

    # 去掉 Windows 盘符（极端情况）
    if WINDOWS_DRIVE_RE.match(s):
        s = s.split(":", 1)[-1]

    # 清理路径
    s = s.lstrip("/")

    return s

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""提供离线运行时依赖发现与本地 `vendor` 加载能力。"""

import os
import platform
import sys
from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
VENDOR_ENV_VAR = "OBS_MIGRATE_VENDOR"


# ================================
# 平台架构归一化
# ================================
def _normalize_machine(machine):
    value = (machine or "").strip().lower()
    mapping = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "x86-64": "x86_64",
        "arm64": "aarch64",
    }
    return mapping.get(value, value or "unknown")


# ================================
# 生成 vendor 搜索标签
# ================================
def build_vendor_tags():
    system_name = platform.system().strip().lower() or "unknown"
    machine_name = _normalize_machine(platform.machine())
    py_tag = f"py{sys.version_info.major}{sys.version_info.minor}"

    tags = [
        f"{system_name}-{machine_name}-{py_tag}",
        f"{system_name}-{machine_name}",
        f"{system_name}-{py_tag}",
        system_name,
        "common",
    ]

    seen = set()
    ordered = []
    for tag in tags:
        if tag and tag not in seen:
            seen.add(tag)
            ordered.append(tag)
    return ordered


# ================================
# 构建 vendor 搜索路径
# ================================
def build_vendor_search_paths(base_dir=None):
    root_dir = Path(base_dir or APP_DIR).resolve()
    candidates = []

    env_value = (os.getenv(VENDOR_ENV_VAR) or "").strip()
    if env_value:
        env_path = Path(env_value)
        if not env_path.is_absolute():
            env_path = root_dir / env_path
        candidates.append(env_path.resolve())

    vendor_root = root_dir / "vendor"
    for tag in build_vendor_tags():
        candidates.append(vendor_root / tag)

    seen = set()
    ordered = []
    for candidate in candidates:
        text = str(candidate)
        if text in seen:
            continue
        seen.add(text)
        ordered.append(candidate)
    return ordered


# ================================
# 遍历可导入目录
# ================================
def _iter_import_paths(vendor_dir):
    yield vendor_dir

    try:
        entries = sorted(vendor_dir.iterdir(), key=lambda item: item.name.lower())
    except OSError:
        return

    for entry in entries:
        if not entry.is_dir():
            continue
        yield entry


# ================================
# 注入本地离线依赖
# ================================
def bootstrap_local_deps(base_dir=None):
    added = []

    for vendor_dir in build_vendor_search_paths(base_dir):
        if not vendor_dir.exists() or not vendor_dir.is_dir():
            continue

        for import_path in _iter_import_paths(vendor_dir):
            text = str(import_path)
            if text in sys.path:
                continue
            sys.path.insert(0, text)
            added.append(text)

    return added

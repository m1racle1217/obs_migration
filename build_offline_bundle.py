#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""构建包含离线依赖与启动脚本的发布目录。"""

import argparse
import json
import shutil
import stat
import sys
import time
from pathlib import Path

from tools.prepare_vendor import (
    build_target_tag,
    normalize_machine_name,
    normalize_platform_name,
    normalize_python_tag,
    prepare_vendor_tree,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DIST_ROOT = PROJECT_ROOT / "dist"

FILES_TO_COPY = (
    "bootstrap_runtime.py",
    "obs_migrate.py",
    "requirements.txt",
    "README.md",
    "OFFLINE_DEPLOY.md",
    "config.example.ini",
)

DIRS_TO_COPY = (
    "core",
)


# ================================
# 交互选择目标平台
# ================================
def prompt_platform(default_platform):
    """在未显式传参时，引导用户选择目标平台。"""

    if not sys.stdin.isatty():
        return default_platform

    print("请选择离线发布平台：")
    print("1. windows")
    print("2. linux (CentOS 7.9)")

    mapping = {
        "1": "windows",
        "windows": "windows",
        "2": "linux",
        "linux": "linux",
    }

    while True:
        raw = input(f"平台 [{default_platform}]: ").strip().lower()
        if not raw:
            return default_platform
        selected = mapping.get(raw)
        if selected:
            return selected
        print("请输入 windows / linux，或输入 1 / 2。")


# ================================
# 交互确认 Python 标签
# ================================
def prompt_python_tag(default_python_tag):
    """在未显式传参时，引导用户确认目标 Python 版本标签。"""

    if not sys.stdin.isatty():
        return default_python_tag

    while True:
        raw = input(f"目标 Python 标签 [{default_python_tag}]（例如 py39）: ").strip()
        if not raw:
            return default_python_tag
        try:
            return normalize_python_tag(raw)
        except ValueError:
            print("请输入类似 py39、3.9、39 这样的 Python 版本标签。")


# ================================
# 清理输出目录
# ================================
def ensure_clean_dir(path):
    """重新创建输出目录，确保本次打包结果干净可控。"""

    if path.exists():
        # ================================
        # 处理只读文件删除失败
        # ================================
        def _onerror(func, failed_path, exc_info):
            failed = Path(failed_path)
            try:
                failed.chmod(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
            except OSError:
                pass
            func(failed_path)

        shutil.rmtree(path, onerror=_onerror)
    path.mkdir(parents=True, exist_ok=True)


# ================================
# 复制发布所需文件
# ================================
def copy_release_files(bundle_dir):
    """复制离线运行所需的最小源码和配置模板。"""

    for name in FILES_TO_COPY:
        source = PROJECT_ROOT / name
        if source.exists():
            shutil.copy2(source, bundle_dir / name)

    for name in DIRS_TO_COPY:
        source = PROJECT_ROOT / name
        if source.exists():
            shutil.copytree(
                source,
                bundle_dir / name,
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
            )


# ================================
# 生成启动脚本
# ================================
def write_launchers(bundle_dir, target_platform):
    """在发布目录中生成 Windows / Linux 启动脚本。"""

    windows_launcher = bundle_dir / "run_obs_migrate.bat"
    windows_launcher.write_text(
        "@echo off\r\n"
        "setlocal\r\n"
        "python obs_migrate.py %*\r\n",
        encoding="utf-8",
    )

    linux_launcher = bundle_dir / "run_obs_migrate.sh"
    linux_launcher.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        "python3 obs_migrate.py \"$@\"\n",
        encoding="utf-8",
    )

    current_mode = linux_launcher.stat().st_mode
    linux_launcher.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    if target_platform == "windows":
        return windows_launcher
    return linux_launcher


# ================================
# 写入构建清单
# ================================
def write_manifest(bundle_dir, metadata):
    """写入离线包清单，记录打包目标与依赖信息。"""

    manifest_path = bundle_dir / "bundle_manifest.json"
    manifest_path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ================================
# 解析命令行参数
# ================================
def parse_args():
    """解析离线发包脚本的命令行参数。"""

    parser = argparse.ArgumentParser(description="Build an offline release bundle for Windows or Linux.")
    parser.add_argument(
        "--platform",
        choices=("windows", "linux"),
        default=None,
        help="target platform for the offline bundle",
    )
    parser.add_argument(
        "--python-tag",
        default=None,
        help="target Python tag, for example py39",
    )
    parser.add_argument(
        "--machine",
        default="x86_64",
        help="target CPU architecture, default: x86_64",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="custom output directory, default: dist/obs_sync_check_<platform>_<machine>_<python-tag>",
    )
    parser.add_argument(
        "--source",
        default="lib",
        help="dependency archive directory, default: lib",
    )
    parser.add_argument(
        "--strict-centos7",
        dest="strict_centos7",
        action="store_true",
        default=None,
        help="fail when a Linux wheel looks incompatible with CentOS 7.9",
    )
    parser.add_argument(
        "--no-strict-centos7",
        dest="strict_centos7",
        action="store_false",
        help="disable the CentOS 7.9 compatibility guard",
    )
    return parser.parse_args()


# ================================
# 主流程
# ================================
def main():
    """执行离线发布目录构建主流程。"""

    args = parse_args()

    default_platform = normalize_platform_name()
    target_platform = normalize_platform_name(args.platform or prompt_platform(default_platform))
    target_python_tag = prompt_python_tag(
        normalize_python_tag(args.python_tag) if args.python_tag else normalize_python_tag()
    )
    target_machine = normalize_machine_name(args.machine)
    strict_centos7 = args.strict_centos7 if args.strict_centos7 is not None else (target_platform == "linux")

    target_tag = build_target_tag(
        target_platform,
        target_machine,
        target_python_tag,
    )

    bundle_name = f"obs_sync_check_{target_platform}_{target_machine}_{target_python_tag}"
    bundle_dir = Path(args.output).resolve() if args.output else (DIST_ROOT / bundle_name).resolve()
    ensure_clean_dir(bundle_dir)
    copy_release_files(bundle_dir)

    vendor_result = prepare_vendor_tree(
        source_dir=PROJECT_ROOT / args.source,
        vendor_dir=bundle_dir / "vendor" / target_tag,
        target_platform=target_platform,
        target_machine=target_machine,
        target_python_tag=target_python_tag,
        clean=False,
        strict_centos7=strict_centos7,
        requirements_file=PROJECT_ROOT / "requirements.txt",
        strict_missing=True,
    )

    launcher_path = write_launchers(bundle_dir, target_platform)
    write_manifest(
        bundle_dir,
        {
            "bundle_name": bundle_name,
            "target_platform": target_platform,
            "target_machine": target_machine,
            "target_python_tag": target_python_tag,
            "strict_centos7": strict_centos7,
            "vendor_tag": target_tag,
            "vendor_packages": vendor_result["selected"],
            "vendor_warnings": vendor_result["warnings"],
            "missing_requirements": vendor_result["missing_requirements"],
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
    )

    print(f"[bundle] output:  {bundle_dir}")
    print(f"[bundle] vendor:  {bundle_dir / 'vendor' / target_tag}")
    print(f"[bundle] launch:  {launcher_path}")
    print(f"[bundle] count:   {len(vendor_result['selected'])}")

    if vendor_result["warnings"]:
        print("[bundle] warnings:")
        for item in vendor_result["warnings"]:
            print(f"  - {item}")

    print("[bundle] done")


if __name__ == "__main__":
    main()

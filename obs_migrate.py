#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import queue
import logging
import threading
import configparser
import getpass
import time
from datetime import datetime
from cryptography.fernet import Fernet
from core.obs_index import build_obs_index
import csv

from core import (
    scan_directory,
    Scheduler,
    OBSUploader,
    init_uploader,
    Progress,
    Checkpoint,
    Dashboard,
    Reporter
)

from core.utils import parse_size, setup_logger
import core.uploader as uploader
from colorama import Fore, Style, init

init(autoreset=True)


CONFIG_FILE = "config.ini"
KEY_FILE = ".config.key"


# ==========================================================
# 加密模块
# ==========================================================

def load_cipher():

    if not os.path.exists(KEY_FILE):
        key = Fernet.generate_key()

        with open(KEY_FILE, "wb") as f:
            f.write(key)

    else:
        with open(KEY_FILE, "rb") as f:
            key = f.read()

    return Fernet(key)


cipher = load_cipher()


def encrypt_value(v):
    return cipher.encrypt(v.encode()).decode()


def decrypt_value(v):

    if not v:
        return ""

    try:
        return cipher.decrypt(v.encode()).decode()

    except Exception:
        return v


def mask_secret(v):

    if not v:
        return ""

    return "*" * 8


# ==========================================================
# 目录初始化
# ==========================================================

def ensure_dirs():

    for d in ["logs", "state", "failed"]:
        os.makedirs(d, exist_ok=True)


# ==========================================================
# 参数说明
# ==========================================================

CONFIG_DESC = {

    "OBS.ak": "华为云 AccessKey",
    "OBS.sk": "华为云 SecretKey",
    "OBS.endpoint": "OBS Endpoint，例如 obs.cn-south-1.myhuaweicloud.com",
    "OBS.bucket": "目标桶名称",

    "TASK.local_dir": "需要迁移的本地目录或文件",
    "TASK.obs_prefix": "OBS 目标前缀",

    "UPLOAD.workers": "上传并发线程数 (推荐 16-64)",
    "UPLOAD.part_size": "分片大小 (例如 64M)",
    "UPLOAD.multipart_threshold": "超过该大小启用分片上传",
    "UPLOAD.retry": "失败重试次数",
    "UPLOAD.rate_limit": "API QPS 限制",

    "SCAN.batch_size": "扫描批次",
    "SCAN.queue_size": "任务队列最大长度",
    "SCAN.scan_workers": "扫描线程数 (推荐 2-8，目录多时可调大)",

    "PATH.log_dir": "日志目录",
    "PATH.state_dir": "断点数据库目录",
    "PATH.failed_dir": "失败任务目录",

    "CHECK.enable_head_check": "是否启用 HEAD 校验（更准确但更慢）",
    "CHECK.strict_client_check": "client 未初始化是否报错（true=严格模式）"


}


# ==========================================================
# 默认配置
# ==========================================================

DEFAULT_CONFIG = {

    "OBS": {

        "ak": "",
        "sk": "",
        "endpoint": "",
        "bucket": ""
    },

    "TASK": {

        "local_dir": "",
        "obs_prefix": ""
    },

    "UPLOAD": {

        "workers": "32",
        "part_size": "64M",
        "multipart_threshold": "128M",
        "retry": "3",
        "rate_limit": "200"
    },

    "SCAN": {

        "batch_size": "1000",
        "queue_size": "20000",
        "scan_workers": "4"
    },

    "PATH": {

        "log_dir": "./logs",
        "state_dir": "./state",
        "failed_dir": "./failed"
    },
    "CHECK": {

    "enable_head_check": "true",
    "strict_client_check": "true",
    "enable_etag_check" : "false"
    }
}



# ==========================================================
# 初始化配置
# ==========================================================

def init_config():

    print("\n首次运行，初始化配置\n")

    cfg = configparser.ConfigParser()

    for s, items in DEFAULT_CONFIG.items():

        cfg[s] = {}

        for k, v in items.items():

            desc = CONFIG_DESC.get(f"{s}.{k}", "")

            print(f"\n{desc}")

            if k in ["ak", "sk"]:

                print("⚠️ 重要信息，请妥善保管")

                # 修改这里
                val = input(f"{k}: ")

                val = encrypt_value(val)

            else:

                val = input(f"{k}: ")

            if not val:
                val = v

            cfg[s][k] = val

    write_config_with_comments(cfg)

    print("\n配置文件已生成 config.ini\n")

    return cfg


# ==========================================================
# 显示配置
# ==========================================================

from colorama import Fore, Style

def show_config(cfg):

    print("\n当前配置\n")

    idx = 1
    mapping = {}

    for s in cfg.sections():

        # 👉 section 蓝色
        print(f"{Fore.CYAN}[{s}]{Style.RESET_ALL}")

        for k, v in cfg[s].items():

            # 敏感信息隐藏
            if k in ["ak", "sk"]:
                v = mask_secret(v)

            # 单位增强
            if k == "rate_limit":
                v = f"{v} req/s"

            desc = CONFIG_DESC.get(f"{s}.{k}", "")

            # 👉 描述黄色
            if desc:
                print(f"{idx}. {Fore.YELLOW}{desc}{Style.RESET_ALL}")
                print(f"    {Fore.GREEN}{k}{Style.RESET_ALL} = {v}")
            else:
                print(f"{idx}. {Fore.GREEN}{k}{Style.RESET_ALL} = {v}")

            mapping[str(idx)] = (s, k)
            idx += 1

        print()

    return mapping

# ==========================================================
# 修改配置
# ==========================================================

def modify_config(cfg):

    mapping = show_config(cfg)

    print("\n输入编号修改，q退出\n")

    while True:

        c = input("选择编号: ")

        if c.lower() == "q":
            break

        if c not in mapping:
            continue

        s, k = mapping[c]

        print(CONFIG_DESC.get(f"{s}.{k}", ""))

        if k in ["ak", "sk"]:

            print("⚠️ 重要信息，请妥善保管")

            # 修改这里
            val = input("新值: ")

            val = encrypt_value(val)

        else:

            val = input("新值: ")

        cfg[s][k] = val

    write_config_with_comments(cfg)

    print("\n配置已更新\n")


# ==========================================================
# 加载配置
# ==========================================================

def load_config():

    if not os.path.exists(CONFIG_FILE):
        return init_config()

    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE, encoding="utf-8")

    updated = False

    # 自动补全
    for section, items in DEFAULT_CONFIG.items():
        if not cfg.has_section(section):
            cfg.add_section(section)
            updated = True

        for k, v in items.items():
            if not cfg.has_option(section, k):
                cfg.set(section, k, v)
                updated = True

    # 写回
    if updated:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            cfg.write(f)

        print("\n⚙️ 检测到新配置项，已自动更新 config.ini\n")

    show_config(cfg)

    c = input("\n是否修改配置? (y/N): ")

    if c.lower() == "y":
        modify_config(cfg)

    return cfg

# ==========================================================
# 写配置（带注释）
# ==========================================================

def write_config_with_comments(cfg):

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:

        for section in cfg.sections():

            f.write(f"# ------------------------------\n")
            f.write(f"# {section}\n")
            f.write(f"# ------------------------------\n")
            f.write(f"[{section}]\n\n")

            for key, value in cfg[section].items():

                desc = CONFIG_DESC.get(f"{section}.{key}", "")

                if desc:
                    f.write(f"# {desc}\n")

                f.write(f"{key} = {value}\n\n")

            f.write("\n")

# ==========================================================
# 配置校验
# ==========================================================

def validate_config(cfg):

    endpoint = cfg.get("OBS", "endpoint")
    bucket = cfg.get("OBS", "bucket")
    local_dir = cfg.get("TASK", "local_dir")

    if not endpoint:
        print("❌ endpoint 未配置")
        sys.exit(1)

    if not bucket:
        print("❌ bucket 未配置")
        sys.exit(1)

    if not os.path.exists(local_dir):
        print("❌ local_dir 不存在")
        sys.exit(1)


# ==========================================================
# 日志
# ==========================================================

def build_log_file(log_dir, local_dir):

    os.makedirs(log_dir, exist_ok=True)

    name = os.path.basename(os.path.normpath(local_dir))

    if not name:
        name = "root"

    date = datetime.now().strftime("%Y%m%d")

    i = 1

    while True:

        f = os.path.join(log_dir, f"{name}_{date}_{i}.log")

        if not os.path.exists(f):
            return f

        i += 1

# ==========================================================
# 主程序
# ==========================================================

def main():

    ensure_dirs()

    cfg = load_config()

    validate_config(cfg)

    ak_raw = cfg.get("OBS", "ak", fallback="").strip()
    sk_raw = cfg.get("OBS", "sk", fallback="").strip()

    changed = False

    if ak_raw and not ak_raw.startswith("gAAAA"):
        ak_raw = encrypt_value(ak_raw)
        cfg.set("OBS", "ak", ak_raw)
        changed = True

    if sk_raw and not sk_raw.startswith("gAAAA"):
        sk_raw = encrypt_value(sk_raw)
        cfg.set("OBS", "sk", sk_raw)
        changed = True

    if changed:
        write_config_with_comments(cfg)

    if not ak_raw or not sk_raw:
        print("\n❌ AK/SK 未配置")
        sys.exit(1)

    ak = decrypt_value(ak_raw)
    sk = decrypt_value(sk_raw)

    local_dir = cfg.get("TASK", "local_dir")

    obs_prefix = cfg.get("TASK", "obs_prefix")

    workers = cfg.getint("UPLOAD", "workers")

    log_dir = cfg.get("PATH", "log_dir")

    state_dir = cfg.get("PATH", "state_dir")

    failed_dir = cfg.get("PATH", "failed_dir")

    scan_workers = cfg.getint("SCAN", "scan_workers", fallback=4)

    enable_head = cfg.getboolean("CHECK", "enable_head_check", fallback=True)

    strict_check = cfg.getboolean("CHECK", "strict_client_check", fallback=True)

    enable_etag = cfg.getboolean("CHECK", "enable_etag_check", fallback=False)

    # ================= 新增目录 =================
    report_dir = os.path.join(os.getcwd(), "check_report")
    os.makedirs(report_dir, exist_ok=True)

    log_file = build_log_file(log_dir, local_dir)

    setup_logger(log_file)

    logging.getLogger().propagate = False

    db_path = os.path.join(state_dir, "tasks.db")

    checkpoint = Checkpoint(db_path)

    progress = Progress()

    task_queue = queue.Queue(
        maxsize=cfg.getint("SCAN", "queue_size")
    )

    rate_limit = cfg.getint("UPLOAD", "rate_limit")

    init_uploader(
        ak,
        sk,
        cfg.get("OBS", "endpoint"),
        cfg.get("OBS", "bucket"),
        parse_size(cfg.get("UPLOAD", "part_size")),
        parse_size(cfg.get("UPLOAD", "multipart_threshold")),
        rate_limit
    )

    # ================= 新增 =================
    reporter = Reporter(report_dir, local_dir)

    bucket = cfg.get("OBS", "bucket")


    # ================= 原逻辑 =================
    uploader = OBSUploader(
        progress,
        checkpoint,
        reporter=reporter,
        failed_dir=failed_dir,
        enable_head_check=enable_head,
        strict_client_check=strict_check,
        enable_etag_check=enable_etag  # ✅ 新增
    )

    scheduler = Scheduler(
        task_queue,
        uploader,
        workers=workers
    )

    dashboard = Dashboard(
        progress,
        task_queue,
        scheduler,
        scan_workers=scan_workers
    )
    index_thread = threading.Thread(
        target=build_obs_index,
        args=(
            ak,
            sk,
            cfg.get("OBS", "endpoint"),
            bucket,
            obs_prefix,
            checkpoint
        ),
        daemon=True
    )

    progress.start()
    scheduler.start()
    dashboard.start()
    index_thread.start()

    if os.path.isfile(local_dir):

        st = os.stat(local_dir)

        filename = os.path.basename(local_dir)

        obs_key = "/".join(
            filter(None, [obs_prefix.strip("/"), filename])
        )

        task = {
            "local": local_dir,
            "obs": obs_key,
            "size": st.st_size
        }

        progress.add_total(st.st_size)

        task_queue.put(task)

    else:

        scan_thread = threading.Thread(
            target=scan_directory,
            args=(
                local_dir,
                obs_prefix,
                task_queue,
                progress,
                checkpoint,
                reporter,
                scan_workers
            ),
            daemon=True
        )

        scan_thread.start()

    try:

        logging.info(f"Task Started. Log: {log_file}")

        while True:

            if os.path.isfile(local_dir):

                if task_queue.empty():
                    break

            else:

                if not scan_thread.is_alive() and task_queue.empty():
                    break

            time.sleep(1)

    except KeyboardInterrupt:

        logging.warning("用户手动停止任务")

    finally:

        scheduler.stop()
        progress.stop()
        checkpoint.close()
        dashboard.stop()

        reporter.close()

        print("\n" + "=" * 50)
        print("✨ 任务结束")
        print("日志:", log_file)
        print("数据库:", db_path)
        print("对比报告:", reporter.file)
        print("=" * 50)

if __name__ == "__main__":

    main()
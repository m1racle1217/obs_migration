#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""汇总迁移器核心模块，向命令行入口暴露稳定的公共接口。"""

from .capabilities import detect_backend_capabilities
from .checkpoint import Checkpoint
from .dashboard import Dashboard
from .governor import ResourceGovernor
from .progress import Progress
from .ratelimiter import RateLimiter
from .report import Reporter
from .s3_scanner import scan_s3_objects
from .scan_control import AdaptiveScanController
from .scanner import scan_directory
from .scheduler import Scheduler
from .uploader import (
    OBSUploader,
    TaskChecker,
    TaskTransfer,
    init_source_client,
    init_target,
    init_uploader,
)
from .utils import (
    build_object_uri,
    detect_storage_scheme,
    normalize_obs_key,
    parse_size,
    safe_decode,
    safe_path,
    setup_logger,
)

__all__ = [
    "scan_directory",
    "scan_s3_objects",
    "Scheduler",
    "OBSUploader",
    "TaskChecker",
    "TaskTransfer",
    "init_source_client",
    "init_target",
    "init_uploader",
    "RateLimiter",
    "ResourceGovernor",
    "Progress",
    "AdaptiveScanController",
    "Checkpoint",
    "Dashboard",
    "Reporter",
    "detect_backend_capabilities",
    "safe_decode",
    "safe_path",
    "normalize_obs_key",
    "parse_size",
    "setup_logger",
    "build_object_uri",
    "detect_storage_scheme",
]

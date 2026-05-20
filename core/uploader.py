# -*- coding: utf-8 -*-
"""执行本地与远端对象迁移的检查、传输、校验与失败治理。"""

import logging
import math
import os
import queue
import random
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from types import SimpleNamespace

try:
    from obs import (
        CompleteMultipartUploadRequest,
        CompletePart,
        GetObjectHeader,
        ObsClient,
        PutObjectHeader,
    )
except ImportError:
    class ObsClient:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError("obs sdk is required for remote storage operations")

    class GetObjectHeader:  # type: ignore
        def __init__(self, range=None):
            self.range = range

    class PutObjectHeader:  # type: ignore
        def __init__(self, contentLength=None):
            self.contentLength = contentLength

    class CompletePart:  # type: ignore
        def __init__(self, partNum=None, etag=None, crc64=None, size=None):
            self.partNum = partNum
            self.etag = etag
            self.crc64 = crc64
            self.size = size

    class CompleteMultipartUploadRequest:  # type: ignore
        def __init__(self, parts=None):
            self.parts = parts or []

from .capabilities import detect_backend_capabilities
from .governor import ResourceGovernor
from .ratelimiter import RateLimiter
from .retry import call_with_retries
from .utils import (
    build_object_uri,
    calc_file_md5,
    clean_path_to_utf8,
    detect_storage_scheme,
    fix_windows_path,
    normalize_endpoint,
    safe_log,
    sanitize_key,
)


TARGET_TYPE_LOCAL = "local"
TARGET_TYPE_S3 = "s3"

_target_type = TARGET_TYPE_S3
_target_root = None
_target_prefix = ""
_target_uri_scheme = "s3"
_target_endpoint_host = ""

_client = None
_bucket = None
_part_size = 8 * 1024 * 1024
_threshold = 128 * 1024 * 1024
_limiter = None
_governor = None

_source_client = None
_source_bucket = None
_source_uri_scheme = "s3"
_source_endpoint_host = ""

_capabilities = {}
_multipart_concurrency = 4
_low_level_retries = 3
_low_level_retry_sleep = 0.5
_compare_mode = "auto"
_verify_after_upload = "none"
_stream_buffer_budget = 8 * 1024 * 1024


# ================================
# 刷新能力探测结果
# ================================
def _refresh_capabilities(source_type="s3"):
    global _capabilities
    _capabilities = detect_backend_capabilities(
        source_type=source_type,
        target_type=_target_type,
        source_scheme=_source_uri_scheme,
        target_scheme=_target_uri_scheme,
        source_endpoint_host=_source_endpoint_host,
        target_endpoint_host=_target_endpoint_host,
    )


# ================================
# 初始化目标端
# ================================
def init_target(
    target_type,
    part_size,
    threshold,
    rate_limit=200,
    ak="",
    sk="",
    endpoint="",
    bucket="",
    path="",
    prefix="",
    rate_limit_burst=None,
    max_connections=0,
    max_buffer_bytes=0,
    multipart_concurrency=4,
    low_level_retries=3,
    low_level_retry_sleep=0.5,
    compare_mode="auto",
    verify_after_upload="none",
    request_timeout=60,
):
    global _target_type, _target_root, _target_prefix
    global _client, _bucket, _part_size, _threshold, _limiter, _governor
    global _target_uri_scheme, _target_endpoint_host
    global _multipart_concurrency, _low_level_retries, _low_level_retry_sleep
    global _compare_mode, _verify_after_upload, _stream_buffer_budget

    normalized_type = (target_type or TARGET_TYPE_S3).strip().lower()
    if normalized_type not in {TARGET_TYPE_LOCAL, TARGET_TYPE_S3}:
        raise ValueError(f"unsupported target type: {target_type}")

    _target_type = normalized_type
    _target_root = os.path.abspath(path) if path else None
    _target_prefix = sanitize_key(prefix or "").strip("/")
    _part_size = max(int(part_size or 0), 1)
    _threshold = max(int(threshold or 0), 0)
    _target_endpoint_host = normalize_endpoint(endpoint)
    _multipart_concurrency = max(1, int(multipart_concurrency or 1))
    _low_level_retries = max(0, int(low_level_retries or 0))
    _low_level_retry_sleep = max(float(low_level_retry_sleep or 0.0), 0.0)
    _compare_mode = (compare_mode or "auto").strip().lower() or "auto"
    _verify_after_upload = (verify_after_upload or "none").strip().lower() or "none"
    _stream_buffer_budget = max(int(max_buffer_bytes or 0), 8 * 1024 * 1024)

    if normalized_type == TARGET_TYPE_S3:
        _target_uri_scheme = detect_storage_scheme(endpoint, fallback="s3")
        _client = ObsClient(
            access_key_id=ak,
            secret_access_key=sk,
            server=endpoint,
            timeout=max(int(request_timeout or 60), 1),
        )
        _bucket = bucket
        _limiter = RateLimiter(rate_limit, burst=rate_limit_burst) if float(rate_limit or 0) > 0 else None
        _governor = ResourceGovernor(
            rate_limit=rate_limit,
            rate_limit_burst=rate_limit_burst,
            max_connections=max_connections,
            max_buffer_bytes=max_buffer_bytes,
        )
    else:
        _target_uri_scheme = ""
        _client = None
        _bucket = None
        _limiter = None
        _governor = None
        _target_endpoint_host = ""
        if _target_root:
            os.makedirs(_target_root, exist_ok=True)

    _refresh_capabilities()


# ================================
# 兼容初始化上传器
# ================================
def init_uploader(*args, **kwargs):
    if args and isinstance(args[0], str) and args[0].strip().lower() in {TARGET_TYPE_LOCAL, TARGET_TYPE_S3}:
        return init_target(*args, **kwargs)

    if len(args) >= 6:
        ak, sk, endpoint, bucket, part_size, threshold = args[:6]
        rate_limit = args[6] if len(args) >= 7 else kwargs.get("rate_limit", 200)
        return init_target(
            TARGET_TYPE_S3,
            part_size,
            threshold,
            rate_limit=rate_limit,
            ak=ak,
            sk=sk,
            endpoint=endpoint,
            bucket=bucket,
            prefix=kwargs.get("prefix", ""),
        )

    return init_target(*args, **kwargs)


# ================================
# 初始化源端客户端
# ================================
def init_source_client(ak, sk, endpoint, bucket, request_timeout=60):
    global _source_client, _source_bucket, _source_uri_scheme, _source_endpoint_host

    if ak and sk and endpoint and bucket:
        _source_uri_scheme = detect_storage_scheme(endpoint, fallback="s3")
        _source_endpoint_host = normalize_endpoint(endpoint)
        _source_client = ObsClient(
            access_key_id=ak,
            secret_access_key=sk,
            server=endpoint,
            timeout=max(int(request_timeout or 60), 1),
        )
        _source_bucket = bucket
    else:
        _source_client = None
        _source_bucket = None
        _source_uri_scheme = "s3"
        _source_endpoint_host = ""

    _refresh_capabilities()


# ================================
# 源对象不存在异常
# ================================
class SourceObjectMissingError(FileNotFoundError):
    """表示远端源对象在传输开始前已经不存在。"""


# ================================
# 上传执行器
# ================================
class OBSUploader:
    """执行检查、传输、校验、重试与失败记录。"""

    # ================================
    # 初始化上传器
    # ================================
    def __init__(
        self,
        progress,
        checkpoint,
        reporter=None,
        failed_dir="failed",
        enable_head_check=True,
        strict_client_check=True,
        enable_etag_check=False,
        retry_limit=3,
        compare_mode=None,
        verify_after_upload=None,
        low_level_retries=None,
        low_level_retry_sleep=None,
        multipart_concurrency=None,
        controls=None,
    ):
        self.progress = progress
        self.checkpoint = checkpoint
        self.reporter = reporter
        self.failed_dir = failed_dir
        self.enable_head_check = enable_head_check
        self.strict_client_check = strict_client_check
        self.enable_etag_check = enable_etag_check
        self.retry_limit = max(1, int(retry_limit or 1))
        self.compare_mode = (compare_mode or _compare_mode or "auto").strip().lower()
        self.verify_after_upload = (verify_after_upload or _verify_after_upload or "none").strip().lower()
        self.low_level_retries = max(
            0,
            int(_low_level_retries if low_level_retries is None else low_level_retries),
        )
        self.low_level_retry_sleep = max(
            float(_low_level_retry_sleep if low_level_retry_sleep is None else low_level_retry_sleep),
            0.0,
        )
        self.multipart_concurrency = max(
            1,
            int(_multipart_concurrency if multipart_concurrency is None else multipart_concurrency),
        )
        self.controls = controls

        self.lock = threading.Lock()
        self._server_side_copy_disabled = False
        self._server_side_copy_logged = False
        os.makedirs(failed_dir, exist_ok=True)

    # ================================
    # 兼容旧入口：检查后传输
    # ================================
    def upload(self, task, heartbeat=None, worker_name=None):
        if self._stop_requested():
            return
        checked_task = self.check_task(task, heartbeat=heartbeat, worker_name=worker_name)
        if checked_task is None:
            return
        self.transfer_task(checked_task, heartbeat=heartbeat, worker_name=worker_name)

    # ================================
    # 作为 transfer handler 的入口
    # ================================
    def process(self, task, heartbeat=None, worker_name=None):
        if self._stop_requested():
            return
        self.transfer_task(task, heartbeat=heartbeat, worker_name=worker_name)

    # ================================
    # 执行预检查
    # ================================
    def check_task(self, task, heartbeat=None, worker_name=None):
        if self._stop_requested():
            return None
        if self.strict_client_check and _target_type == TARGET_TYPE_S3 and _client is None:
            raise RuntimeError("target client not initialized")

        source_type = (task.get("source_type") or "local").lower()
        if self.strict_client_check and source_type == "s3" and _source_client is None:
            raise RuntimeError("source S3 client not initialized")

        context = self._build_task_context(task)
        if context is None:
            return None

        if heartbeat is not None:
            heartbeat("check")

        should_skip = self._maybe_skip_existing(
            source_ref=context["source_ref"],
            source_display=context["source_display"],
            target_ref=context["target_ref"],
            target_display=context["target_display"],
            size=context["size"],
            mtime=context["mtime"],
            source_etag=context.get("source_etag"),
            local_path_bytes=context.get("local_path_bytes"),
            heartbeat=heartbeat,
        )
        if should_skip:
            return None

        task["_transfer_ctx"] = context
        return task

    # ================================
    # 执行传输
    # ================================
    def transfer_task(self, task, heartbeat=None, worker_name=None):
        if self._stop_requested():
            return
        context = task.get("_transfer_ctx") or self._build_task_context(task)
        if context is None:
            return
        self._upload_with_retry(context, heartbeat=heartbeat)

    def _stop_requested(self):
        return self.controls is not None and self.controls.stop_requested()

    def _sleep_or_stop(self, seconds):
        if self.controls is None:
            time.sleep(seconds)
            return False

        deadline = time.time() + max(float(seconds or 0), 0.0)
        while time.time() < deadline:
            if self._stop_requested():
                return True
            time.sleep(min(0.05, max(deadline - time.time(), 0.0)))
        return self._stop_requested()

    # ================================
    # 构建任务上下文
    # ================================
    def _build_task_context(self, task):
        source_type = (task.get("source_type") or "local").lower()
        if source_type == "s3":
            return self._build_s3_task_context(task)
        return self._build_local_task_context(task)

    # ================================
    # 构建本地任务上下文
    # ================================
    def _build_local_task_context(self, task):
        local_path_bytes = task["local"]
        source_ref = task.get("source_path") or fix_windows_path(clean_path_to_utf8(local_path_bytes))
        source_display = task.get("source_display") or source_ref
        relative_path = task.get("relative_path") or os.path.basename(source_ref)
        size = int(task.get("size", 0) or 0)

        try:
            stat_result = os.stat(local_path_bytes, follow_symlinks=False)
            size = stat_result.st_size
            mtime = stat_result.st_mtime
        except FileNotFoundError:
            if os.path.exists(local_path_bytes):
                logging.error("[BUG][ENCODING] %s", safe_log(local_path_bytes))
                self._report(source_display, "", size, "ERROR", "encoding issue")
            else:
                logging.debug("[REAL_MISSING] %s", source_display)
                self._report(source_display, "", size, "MISSING", "file not found")

            self.progress.skip()
            self.progress.add_done(size)
            return None

        target_ref, target_display = self._resolve_target(relative_path)
        return {
            "source_type": "local",
            "local_path_bytes": local_path_bytes,
            "source_ref": source_ref,
            "source_display": source_display,
            "relative_path": relative_path,
            "size": size,
            "mtime": mtime,
            "target_ref": target_ref,
            "target_display": target_display,
            "source_etag": None,
        }

    # ================================
    # 构建远端任务上下文
    # ================================
    def _build_s3_task_context(self, task):
        if self.strict_client_check and _source_client is None:
            raise RuntimeError("source S3 client not initialized")

        source_bucket = task.get("source_bucket") or _source_bucket
        source_key = sanitize_key(task["source_key"]).strip("/")
        source_ref = task.get("source_path") or build_object_uri(source_bucket, source_key, scheme="s3")
        source_display = task.get("source_display") or build_object_uri(
            source_bucket,
            source_key,
            scheme=_source_uri_scheme,
        )
        relative_path = task.get("relative_path") or source_key.rsplit("/", 1)[-1]
        size = int(task.get("size", 0) or 0)
        mtime = float(task.get("mtime", 0) or 0.0)

        target_ref, target_display = self._resolve_target(relative_path)
        return {
            "source_type": "s3",
            "source_bucket": source_bucket,
            "source_key": source_key,
            "source_ref": source_ref,
            "source_display": source_display,
            "relative_path": relative_path,
            "size": size,
            "mtime": mtime,
            "target_ref": target_ref,
            "target_display": target_display,
            "source_etag": task.get("etag"),
            "local_path_bytes": None,
        }

    # ================================
    # 带高层重试执行传输
    # ================================
    def _upload_with_retry(self, context, heartbeat=None):
        retry = 0
        last_err = ""

        while retry < self.retry_limit:
            if self._stop_requested():
                logging.info("[STOP] skip transfer after stop requested: %s", context["source_display"])
                return
            start = time.time()

            try:
                if heartbeat is not None:
                    heartbeat("transfer")

                response = self._perform_transfer(context, heartbeat=heartbeat)
                if getattr(response, "status", 500) >= 300:
                    raise Exception(f"target status {getattr(response, 'status', None)}")

                self._verify_after_transfer(context, heartbeat=heartbeat)

                cost = time.time() - start
                self.checkpoint.mark_done(context["source_ref"], context["size"], context["mtime"])
                self.progress.add_done(context["size"])
                logging.info(
                    "[UPLOAD][SUCCESS] %s -> %s size=%s cost=%.2fs",
                    context["source_display"],
                    context["target_display"],
                    context["size"],
                    cost,
                )
                self._report(
                    context["source_display"],
                    context["target_display"],
                    context["size"],
                    "SUCCESS",
                    "",
                )
                return
            except SourceObjectMissingError as exc:
                logging.warning("[SOURCE_MISSING] %s -> %s", context["source_display"], context["target_display"])
                self.progress.skip()
                self.progress.add_done(context["size"])
                self._report(
                    context["source_display"],
                    context["target_display"],
                    context["size"],
                    "MISSING",
                    str(exc),
                )
                return
            except Exception as exc:
                retry += 1
                self.progress.upload_error_inc()
                last_err = repr(exc)
                logging.exception("[RETRY] %s retry=%s", context["source_display"], retry)
                if self._stop_requested():
                    logging.info("[STOP] abort retry after stop requested: %s", context["source_display"])
                    return
                if self._sleep_or_stop(min(2 ** retry + random.random(), 10)):
                    logging.info("[STOP] abort retry sleep after stop requested: %s", context["source_display"])
                    return

        logging.error("[UPLOAD_FAIL] %s err=%s", context["source_display"], last_err)
        self.record_failed(context["source_display"])
        self._report(
            context["source_display"],
            context["target_display"],
            context["size"],
            "FAILED",
            last_err or f"retry exceeded ({self.retry_limit})",
        )

    # ================================
    # 执行实际传输
    # ================================
    def _perform_transfer(self, context, heartbeat=None):
        if context["source_type"] == "local":
            if _target_type == TARGET_TYPE_S3:
                return self._put_local_file_to_s3(
                    context["local_path_bytes"],
                    context["target_ref"],
                    context["size"],
                    heartbeat=heartbeat,
                )
            return self._copy_local_file_to_local(context["local_path_bytes"], context["target_ref"])

        if _target_type == TARGET_TYPE_S3:
            return self._copy_s3_object_to_s3(
                context["source_bucket"],
                context["source_key"],
                context["target_ref"],
                context["size"],
                heartbeat=heartbeat,
            )

        return self._download_s3_to_local(
            context["source_bucket"],
            context["source_key"],
            context["target_ref"],
            heartbeat=heartbeat,
        )

    # ================================
    # 判断是否可跳过
    # ================================
    def _maybe_skip_existing(
        self,
        source_ref,
        source_display,
        target_ref,
        target_display,
        size,
        mtime,
        source_etag=None,
        local_path_bytes=None,
        heartbeat=None,
    ):
        if _target_type == TARGET_TYPE_S3:
            return self._maybe_skip_existing_s3(
                source_ref,
                source_display,
                target_ref,
                target_display,
                size,
                mtime,
                source_etag=source_etag,
                local_path_bytes=local_path_bytes,
                heartbeat=heartbeat,
            )

        return self._maybe_skip_existing_local(
            source_ref,
            source_display,
            target_ref,
            target_display,
            size,
            mtime,
            source_etag=source_etag,
            local_path_bytes=local_path_bytes,
        )

    # ================================
    # 远端目标跳过判断
    # ================================
    def _maybe_skip_existing_s3(
        self,
        source_ref,
        source_display,
        target_key,
        target_display,
        size,
        mtime,
        source_etag=None,
        local_path_bytes=None,
        heartbeat=None,
    ):
        compare_mode = self._effective_compare_mode()
        index_ready = getattr(self.checkpoint, "obs_index_ready", False) and self._use_index_compare()
        index_row = None

        if index_ready:
            try:
                index_row = self.checkpoint.get_obs(target_key)
            except Exception:
                index_row = None

            if index_row and int(index_row[0] or 0) == int(size):
                self.progress.cache_hit_inc()
            else:
                self.progress.cache_miss_inc()

        if compare_mode == "index_only":
            need_head = not index_ready
        else:
            need_head = (
                compare_mode == "head_only"
                or not index_ready
                or (index_row is not None and (self.enable_head_check or compare_mode == "hybrid"))
            )

        if index_row and int(index_row[0] or 0) == int(size) and not need_head:
            self.progress.skip()
            self.progress.add_done(size)
            self.checkpoint.mark_done(source_ref, size, mtime)
            logging.info("[SKIP][INDEX] %s -> %s size=%s", source_display, target_display, size)
            self._report(source_display, target_display, size, "SKIP", "index(size)")
            return True

        head_status = "UNKNOWN"
        if need_head:
            try:
                meta = self._call_target(
                    lambda: _client.getObjectMetadata(_bucket, target_key),
                    operation=f"head:{target_key}",
                    heartbeat=heartbeat,
                )
                if meta.status < 300:
                    remote_size = int(meta.body.contentLength or 0)
                    remote_etag = self._normalize_etag(getattr(meta.body, "etag", None))
                    if remote_size != int(size):
                        head_status = "EXIST_DIFF"
                    else:
                        candidate_etag = source_etag
                        if candidate_etag is None and self.enable_etag_check:
                            candidate_etag = self._resolve_local_etag(local_path_bytes, size)
                        if self._can_compare_with_simple_etag(candidate_etag, remote_etag):
                            if self._normalize_etag(candidate_etag) == remote_etag:
                                head_status = "EXIST_SAME"
                            else:
                                head_status = "EXIST_DIFF"
                        else:
                            head_status = "EXIST_SAME"
                elif meta.status == 404:
                    head_status = "NOT_EXIST"
                else:
                    head_status = "ERROR"
            except Exception as exc:
                logging.debug("[HEAD_FAIL] %s err=%s", target_key, exc)
                head_status = "ERROR"

        if head_status == "EXIST_SAME":
            self.progress.skip()
            self.progress.add_done(size)
            self.checkpoint.mark_done(source_ref, size, mtime)
            tag = "HEAD_ETAG" if self.enable_etag_check else "HEAD_SIZE"
            logging.info("[SKIP][%s] %s -> %s size=%s", tag, source_display, target_display, size)
            self._report(source_display, target_display, size, "SKIP", "already exists")
            return True

        if head_status == "ERROR" and self.checkpoint.is_done(source_ref, size, mtime):
            logging.info("[SKIP][CHECKPOINT] %s -> %s", source_display, target_display)
            self.progress.skip()
            self.progress.add_done(size)
            self._report(source_display, target_display, size, "SKIP", "checkpoint")
            return True

        return False

    # ================================
    # 本地目标跳过判断
    # ================================
    def _maybe_skip_existing_local(
        self,
        source_ref,
        source_display,
        target_path,
        target_display,
        size,
        mtime,
        source_etag=None,
        local_path_bytes=None,
    ):
        target_status = "UNKNOWN"

        try:
            stat_result = os.stat(target_path)
            target_size = stat_result.st_size
            if int(target_size) != int(size):
                target_status = "EXIST_DIFF"
            else:
                candidate_etag = source_etag
                if candidate_etag is None and self.enable_etag_check:
                    candidate_etag = self._resolve_local_etag(local_path_bytes, size)
                if self._can_compare_with_local_file(candidate_etag, target_path, size):
                    target_hash = calc_file_md5(target_path)
                    if self._normalize_etag(candidate_etag) == target_hash:
                        target_status = "EXIST_SAME"
                    else:
                        target_status = "EXIST_DIFF"
                else:
                    target_status = "EXIST_SAME"
        except FileNotFoundError:
            target_status = "NOT_EXIST"
        except Exception as exc:
            logging.debug("[LOCAL_TARGET_CHECK_FAIL] %s err=%s", target_path, exc)
            target_status = "ERROR"

        if target_status == "EXIST_SAME":
            self.progress.skip()
            self.progress.add_done(size)
            self.checkpoint.mark_done(source_ref, size, mtime)
            tag = "LOCAL_ETAG" if self.enable_etag_check else "LOCAL_SIZE"
            logging.info("[SKIP][%s] %s -> %s size=%s", tag, source_display, target_display, size)
            self._report(source_display, target_display, size, "SKIP", "already exists")
            return True

        if target_status == "ERROR" and self.checkpoint.is_done(source_ref, size, mtime):
            logging.info("[SKIP][CHECKPOINT] %s -> %s", source_display, target_display)
            self.progress.skip()
            self.progress.add_done(size)
            self._report(source_display, target_display, size, "SKIP", "checkpoint")
            return True

        return False

    # ================================
    # 解析目标位置
    # ================================
    def _resolve_target(self, relative_path):
        relative_path = sanitize_key(relative_path or "").strip("/")
        if _target_type == TARGET_TYPE_S3:
            target_key = "/".join(filter(None, [_target_prefix, relative_path]))
            return target_key, build_object_uri(_bucket, target_key, scheme=_target_uri_scheme)

        local_relative = relative_path.replace("/", os.sep)
        target_path = os.path.abspath(os.path.join(_target_root or "", local_relative))
        return target_path, target_path

    # ================================
    # 本地文件上传到对象存储
    # ================================
    def _put_local_file_to_s3(self, local_path_bytes, target_key, size, heartbeat=None):
        local_path = fix_windows_path(os.fsdecode(local_path_bytes))
        threshold = max(int(_threshold or 0), 0)
        progress_callback = self._make_progress_callback(heartbeat=heartbeat, detail="upload")

        if size >= threshold:
            return self._call_target(
                lambda: _client.uploadFile(
                    _bucket,
                    target_key,
                    local_path,
                    partSize=max(int(_part_size or 0), 1),
                    taskNum=self.multipart_concurrency,
                    enableCheckpoint=True,
                    progressCallback=progress_callback,
                ),
                operation=f"uploadFile:{target_key}",
                heartbeat=heartbeat,
            )

        return self._call_target(
            lambda: _client.putFile(
                _bucket,
                target_key,
                local_path,
                progressCallback=progress_callback,
            ),
            operation=f"putFile:{target_key}",
            heartbeat=heartbeat,
        )

    # ================================
    # 本地文件复制到本地
    # ================================
    def _copy_local_file_to_local(self, local_path_bytes, target_path):
        source_path = fix_windows_path(os.fsdecode(local_path_bytes))
        target_path = fix_windows_path(target_path)
        part_path = target_path + ".part"
        self._ensure_parent_dir(target_path)

        try:
            shutil.copy2(source_path, part_path)
            os.replace(part_path, target_path)
            return SimpleNamespace(status=200)
        except Exception:
            if os.path.exists(part_path):
                try:
                    os.remove(part_path)
                except Exception:
                    pass
            raise

    # ================================
    # 远端对象复制到对象存储
    # ================================
    def _copy_s3_object_to_s3(self, source_bucket, source_key, target_key, size, heartbeat=None):
        threshold = max(int(_threshold or 0), 0)

        if self._should_use_server_side_copy():
            response = self._copy_s3_object_server_side(
                source_bucket,
                source_key,
                target_key,
                size,
                heartbeat=heartbeat,
            )
            if response is not None:
                return response

        if size >= threshold:
            return self._multipart_copy_from_s3(
                source_bucket,
                source_key,
                target_key,
                size,
                heartbeat=heartbeat,
            )

        headers = PutObjectHeader(contentLength=size)

        def do_put_content():
            stream = None
            progress_callback = self._make_progress_callback(heartbeat=heartbeat, detail="upload")
            try:
                stream = self._open_source_stream(source_bucket, source_key, heartbeat=heartbeat)
                return self._call_target_once(
                    lambda: _client.putContent(
                        _bucket,
                        target_key,
                        stream,
                        headers=headers,
                        progressCallback=progress_callback,
                    ),
                    operation=f"putContent:{target_key}",
                    heartbeat=heartbeat,
                )
            finally:
                self._close_stream(stream)

        return call_with_retries(
            do_put_content,
            retries=self.low_level_retries,
            base_sleep=self.low_level_retry_sleep,
            operation=f"putContent:{target_key}",
            heartbeat=heartbeat,
            logger=logging.getLogger(__name__),
        )

    # ================================
    # 优先尝试服务端拷贝
    # ================================
    def _copy_s3_object_server_side(self, source_bucket, source_key, target_key, size, heartbeat=None):
        try:
            threshold = max(int(_threshold or 0), 0)
            if size >= threshold:
                response = self._multipart_copy_from_s3_server_side(
                    source_bucket,
                    source_key,
                    target_key,
                    size,
                    heartbeat=heartbeat,
                )
            else:
                response = self._call_target(
                    lambda: _client.copyObject(source_bucket, source_key, _bucket, target_key),
                    operation=f"copyObject:{target_key}",
                    heartbeat=heartbeat,
                )
        except Exception as exc:
            self._maybe_disable_server_side_copy(error=exc)
            return None

        if getattr(response, "status", 500) < 300:
            with self.lock:
                if not self._server_side_copy_logged:
                    logging.info(
                        "[SERVER_COPY_ENABLED] endpoint=%s bucket=%s",
                        _target_endpoint_host,
                        _bucket,
                    )
                    self._server_side_copy_logged = True
            logging.debug("[SERVER_COPY] %s -> %s", source_key, target_key)
            return response

        self._maybe_disable_server_side_copy(status=getattr(response, "status", None))
        logging.warning(
            "[SERVER_COPY_FALLBACK] %s -> %s status=%s",
            build_object_uri(source_bucket, source_key, scheme=_source_uri_scheme),
            build_object_uri(_bucket, target_key, scheme=_target_uri_scheme),
            getattr(response, "status", None),
        )
        return None

    # ================================
    # 远端对象下载到本地
    # ================================
    def _download_s3_to_local(self, source_bucket, source_key, target_path, heartbeat=None):
        stream = None
        target_path = fix_windows_path(target_path)
        part_path = target_path + ".part"
        self._ensure_parent_dir(target_path)

        try:
            stream = self._open_source_stream(source_bucket, source_key, heartbeat=heartbeat)
            with open(part_path, "wb") as handle:
                while True:
                    if heartbeat is not None:
                        heartbeat("download")
                    chunk = stream.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)

            os.replace(part_path, target_path)
            return SimpleNamespace(status=200)
        except Exception:
            if os.path.exists(part_path):
                try:
                    os.remove(part_path)
                except Exception:
                    pass
            raise
        finally:
            self._close_stream(stream)

    # ================================
    # 流式分片复制
    # ================================
    def _multipart_copy_from_s3(self, source_bucket, source_key, target_key, size, heartbeat=None):
        init_response = self._call_target(
            lambda: _client.initiateMultipartUpload(_bucket, target_key),
            operation=f"initMultipart:{target_key}",
            heartbeat=heartbeat,
        )
        if init_response.status >= 300:
            raise Exception(f"init multipart failed {init_response.status}")

        upload_id = init_response.body.uploadId
        total_parts = int(math.ceil(float(size) / float(max(int(_part_size or 1), 1)))) if size > 0 else 1
        results = [None] * total_parts

        try:
            if self.multipart_concurrency <= 1 or total_parts <= 1:
                for part_number in range(1, total_parts + 1):
                    offset = (part_number - 1) * _part_size
                    current_part_size = min(_part_size, size - offset) if size > 0 else 0
                    part = self._upload_stream_part(
                        source_bucket,
                        source_key,
                        target_key,
                        upload_id,
                        part_number,
                        offset,
                        current_part_size,
                        heartbeat,
                    )
                    results[part_number - 1] = part
            else:
                with ThreadPoolExecutor(max_workers=self.multipart_concurrency) as executor:
                    futures = []
                    for part_number in range(1, total_parts + 1):
                        offset = (part_number - 1) * _part_size
                        current_part_size = min(_part_size, size - offset) if size > 0 else 0
                        futures.append(
                            executor.submit(
                                self._upload_stream_part,
                                source_bucket,
                                source_key,
                                target_key,
                                upload_id,
                                part_number,
                                offset,
                                current_part_size,
                                heartbeat,
                            )
                        )

                    for future in futures:
                        part = future.result()
                        results[int(part.partNum) - 1] = part

            return self._call_target(
                lambda: _client.completeMultipartUpload(
                    _bucket,
                    target_key,
                    upload_id,
                    CompleteMultipartUploadRequest(parts=results),
                ),
                operation=f"completeMultipart:{target_key}",
                heartbeat=heartbeat,
            )
        except Exception:
            try:
                self._call_target(
                    lambda: _client.abortMultipartUpload(_bucket, target_key, upload_id),
                    operation=f"abortMultipart:{target_key}",
                    heartbeat=heartbeat,
                )
            except Exception as abort_error:
                logging.debug("[ABORT_MULTIPART_FAIL] %s", abort_error)
            raise

    # ================================
    # 服务端分片复制
    # ================================
    def _multipart_copy_from_s3_server_side(self, source_bucket, source_key, target_key, size, heartbeat=None):
        init_response = self._call_target(
            lambda: _client.initiateMultipartUpload(_bucket, target_key),
            operation=f"initServerMultipart:{target_key}",
            heartbeat=heartbeat,
        )
        if init_response.status >= 300:
            raise Exception(f"init multipart failed {init_response.status}")

        upload_id = init_response.body.uploadId
        copy_source = f"/{source_bucket}/{source_key}"
        total_parts = int(math.ceil(float(size) / float(max(int(_part_size or 1), 1)))) if size > 0 else 1
        results = [None] * total_parts

        try:
            if self.multipart_concurrency <= 1 or total_parts <= 1:
                for part_number in range(1, total_parts + 1):
                    offset = (part_number - 1) * _part_size
                    current_part_size = min(_part_size, size - offset) if size > 0 else 0
                    part = self._copy_server_part(
                        target_key,
                        upload_id,
                        copy_source,
                        part_number,
                        offset,
                        current_part_size,
                        heartbeat,
                    )
                    results[part_number - 1] = part
            else:
                with ThreadPoolExecutor(max_workers=self.multipart_concurrency) as executor:
                    futures = []
                    for part_number in range(1, total_parts + 1):
                        offset = (part_number - 1) * _part_size
                        current_part_size = min(_part_size, size - offset) if size > 0 else 0
                        futures.append(
                            executor.submit(
                                self._copy_server_part,
                                target_key,
                                upload_id,
                                copy_source,
                                part_number,
                                offset,
                                current_part_size,
                                heartbeat,
                            )
                        )

                    for future in futures:
                        part = future.result()
                        results[int(part.partNum) - 1] = part

            return self._call_target(
                lambda: _client.completeMultipartUpload(
                    _bucket,
                    target_key,
                    upload_id,
                    CompleteMultipartUploadRequest(parts=results),
                ),
                operation=f"completeServerMultipart:{target_key}",
                heartbeat=heartbeat,
            )
        except Exception:
            try:
                self._call_target(
                    lambda: _client.abortMultipartUpload(_bucket, target_key, upload_id),
                    operation=f"abortServerMultipart:{target_key}",
                    heartbeat=heartbeat,
                )
            except Exception as abort_error:
                logging.debug("[ABORT_SERVER_COPY_FAIL] %s", abort_error)
            raise

    # ================================
    # 上传单个分片
    # ================================
    def _upload_stream_part(
        self,
        source_bucket,
        source_key,
        target_key,
        upload_id,
        part_number,
        offset,
        current_part_size,
        heartbeat,
    ):
        reserve_context = self._reserve_transfer_buffer(current_part_size)
        with reserve_context:
            def do_upload_part():
                stream = None
                progress_callback = self._make_progress_callback(
                    heartbeat=heartbeat,
                    detail=f"upload-part-{part_number}",
                )
                try:
                    stream = self._open_source_stream(
                        source_bucket,
                        source_key,
                        offset=offset,
                        part_size=current_part_size,
                        heartbeat=heartbeat,
                    )
                    return self._call_target_once(
                        lambda: _client.uploadPart(
                            _bucket,
                            target_key,
                            part_number,
                            upload_id,
                            content=stream,
                            partSize=current_part_size,
                            progressCallback=progress_callback,
                        ),
                        operation=f"uploadPart:{target_key}#{part_number}",
                        heartbeat=heartbeat,
                    )
                finally:
                    self._close_stream(stream)

            response = call_with_retries(
                do_upload_part,
                retries=self.low_level_retries,
                base_sleep=self.low_level_retry_sleep,
                operation=f"uploadPart:{target_key}#{part_number}",
                heartbeat=heartbeat,
                logger=logging.getLogger(__name__),
            )

        if response.status >= 300:
            raise Exception(f"upload part failed part={part_number} status={response.status}")

        return CompletePart(
            partNum=part_number,
            etag=getattr(response.body, "etag", None),
            crc64=getattr(response.body, "crc64", None),
            size=current_part_size,
        )

    # ================================
    # 服务端复制单个分片
    # ================================
    def _copy_server_part(
        self,
        target_key,
        upload_id,
        copy_source,
        part_number,
        offset,
        current_part_size,
        heartbeat,
    ):
        reserve_context = self._reserve_transfer_buffer(current_part_size)
        with reserve_context:
            range_end = offset + current_part_size - 1
            response = self._call_target(
                lambda: _client.copyPart(
                    _bucket,
                    target_key,
                    part_number,
                    upload_id,
                    copy_source,
                    copySourceRange=f"{offset}-{range_end}",
                ),
                operation=f"copyPart:{target_key}#{part_number}",
                heartbeat=heartbeat,
            )

        if response.status >= 300:
            raise Exception(f"copy part failed part={part_number} status={response.status}")

        return CompletePart(
            partNum=part_number,
            etag=getattr(response.body, "etag", None),
            size=current_part_size,
        )

    # ================================
    # 打开源对象流
    # ================================
    def _open_source_stream(self, source_bucket, source_key, offset=None, part_size=None, heartbeat=None):
        headers = None
        if offset is not None and part_size is not None:
            headers = GetObjectHeader(range=f"{offset}-{offset + part_size - 1}")

        response = self._call_source(
            lambda: _source_client.getObject(source_bucket, source_key, headers=headers),
            operation=f"getObject:{source_key}",
            heartbeat=heartbeat,
        )
        if response.status == 404:
            raise SourceObjectMissingError(
                f"source object not found: {build_object_uri(source_bucket, source_key, scheme=_source_uri_scheme)}"
            )
        if response.status >= 300:
            raise Exception(f"source get error {response.status}")

        return response.body.response

    # ================================
    # 传输后校验
    # ================================
    def _verify_after_transfer(self, context, heartbeat=None):
        mode = self.verify_after_upload
        if mode in {"", "none", "false"}:
            return

        if heartbeat is not None:
            heartbeat("verify")

        if _target_type == TARGET_TYPE_LOCAL:
            target_path = context["target_ref"]
            stat_result = os.stat(target_path)
            if int(stat_result.st_size) != int(context["size"]):
                raise Exception(f"verify size mismatch local target={stat_result.st_size} source={context['size']}")

            if mode in {"etag", "head"}:
                candidate_etag = context.get("source_etag")
                if candidate_etag is None and mode == "etag":
                    candidate_etag = self._resolve_local_etag(
                        context.get("local_path_bytes"),
                        context["size"],
                    )
                if self._can_compare_with_local_file(candidate_etag, target_path, context["size"]):
                    target_hash = calc_file_md5(target_path)
                    if self._normalize_etag(candidate_etag) != target_hash:
                        raise Exception("verify etag mismatch on local target")
            return

        meta = self._call_target(
            lambda: _client.getObjectMetadata(_bucket, context["target_ref"]),
            operation=f"verifyHead:{context['target_ref']}",
            heartbeat=heartbeat,
        )
        if meta.status >= 300:
            raise Exception(f"verify head failed status={meta.status}")

        remote_size = int(meta.body.contentLength or 0)
        if remote_size != int(context["size"]):
            raise Exception(f"verify size mismatch remote={remote_size} source={context['size']}")

        if mode == "size":
            return

        remote_etag = self._normalize_etag(getattr(meta.body, "etag", None))
        source_etag = context.get("source_etag")
        if source_etag is None and mode == "etag":
            source_etag = self._resolve_local_etag(
                context.get("local_path_bytes"),
                context["size"],
            )
        if mode == "etag" and not self._can_compare_with_simple_etag(source_etag, remote_etag):
            raise Exception("verify etag requested but unavailable")

        if self._can_compare_with_simple_etag(source_etag, remote_etag):
            if self._normalize_etag(source_etag) != remote_etag:
                raise Exception("verify etag mismatch on target")

    # ================================
    # 执行目标端调用
    # ================================
    def _call_target(self, func, operation, heartbeat=None):
        connection_context = _governor.connection_slot() if _governor is not None else nullcontext()
        with connection_context:
            self._acquire_api_token()
            return call_with_retries(
                func,
                retries=self.low_level_retries,
                base_sleep=self.low_level_retry_sleep,
                operation=operation,
                heartbeat=heartbeat,
                logger=logging.getLogger(__name__),
            )

    # ================================
    # 执行目标端单次调用
    # ================================
    def _call_target_once(self, func, operation, heartbeat=None):
        connection_context = _governor.connection_slot() if _governor is not None else nullcontext()
        with connection_context:
            self._acquire_api_token()
            if heartbeat is not None:
                heartbeat(operation)
            return func()

    # ================================
    # 执行源端调用
    # ================================
    def _call_source(self, func, operation, heartbeat=None):
        connection_context = _governor.connection_slot() if _governor is not None else nullcontext()
        with connection_context:
            self._acquire_api_token()
            return call_with_retries(
                func,
                retries=self.low_level_retries,
                base_sleep=self.low_level_retry_sleep,
                operation=operation,
                heartbeat=heartbeat,
                logger=logging.getLogger(__name__),
            )

    # ================================
    # 获取 API 令牌
    # ================================
    def _acquire_api_token(self):
        if _governor is not None:
            _governor.acquire_api(1)
            return
        if _limiter is not None:
            _limiter.acquire()

    # ================================
    # 预留传输缓冲
    # ================================
    def _reserve_transfer_buffer(self, current_part_size):
        if _governor is None:
            return nullcontext()
        return _governor.reserve_buffer(min(int(current_part_size or 0), _stream_buffer_budget))

    def _make_progress_callback(self, heartbeat=None, detail="upload"):
        state = {"last": 0}

        def callback(transferred, total_amount=None, total_seconds=None):
            try:
                current = max(int(transferred or 0), 0)
            except Exception:
                return

            delta = current - state["last"]
            if delta > 0:
                self.progress.record_upload_bytes(delta)
                state["last"] = current
            elif current > state["last"]:
                state["last"] = current

            if heartbeat is not None:
                heartbeat(detail)

        return callback

    # ================================
    # 计算有效比较模式
    # ================================
    def _effective_compare_mode(self):
        mode = (self.compare_mode or "auto").strip().lower()
        if mode == "auto":
            if _target_type != TARGET_TYPE_S3:
                return "head_only"
            return "hybrid" if getattr(self.checkpoint, "obs_index_ready", False) else "head_only"
        if mode in {"hybrid", "head_only", "index_only"}:
            return mode
        return "hybrid"

    # ================================
    # 判断是否使用索引比较
    # ================================
    def _use_index_compare(self):
        return _target_type == TARGET_TYPE_S3 and self._effective_compare_mode() != "head_only"

    # ================================
    # 判断是否启用服务端拷贝
    # ================================
    def _should_use_server_side_copy(self):
        return (
            _target_type == TARGET_TYPE_S3
            and _source_client is not None
            and not self._server_side_copy_disabled
            and bool(_target_endpoint_host)
            and _target_endpoint_host == _source_endpoint_host
            and bool(_capabilities.get("supports_server_side_copy", True))
        )

    # ================================
    # 按需关闭服务端拷贝
    # ================================
    def _maybe_disable_server_side_copy(self, status=None, error=None):
        should_disable = status in {400, 401, 403, 405, 409, 501}

        if error is not None:
            message = str(error).lower()
            should_disable = should_disable or any(
                text in message
                for text in (
                    "copyobject",
                    "copypart",
                    "not support",
                    "not supported",
                    "access denied",
                    "signaturedoesnotmatch",
                )
            )

        if not should_disable:
            return

        with self.lock:
            if self._server_side_copy_disabled:
                return
            self._server_side_copy_disabled = True

        logging.warning(
            "[SERVER_COPY_DISABLED] endpoint=%s reason=%s%s",
            _target_endpoint_host,
            f"status={status}" if status is not None else "error",
            f" err={error}" if error is not None else "",
        )

    # ================================
    # 判断是否可直接比较简单 ETag
    # ================================
    @staticmethod
    def _can_compare_with_simple_etag(source_etag, remote_etag):
        source_etag = OBSUploader._normalize_etag(source_etag)
        remote_etag = OBSUploader._normalize_etag(remote_etag)
        return bool(source_etag and remote_etag and "-" not in source_etag and "-" not in remote_etag)

    # ================================
    # 判断是否可与本地文件比较
    # ================================
    def _can_compare_with_local_file(self, source_etag, target_path, size):
        if not self.enable_etag_check:
            return False
        if size >= 100 * 1024 * 1024:
            return False
        if not os.path.isfile(target_path):
            return False
        normalized = self._normalize_etag(source_etag)
        return bool(normalized and "-" not in normalized)

    # ================================
    # 归一化 ETag
    # ================================
    @staticmethod
    def _normalize_etag(etag):
        if not etag:
            return None
        return str(etag).strip().strip('"')

    # ================================
    # 安全关闭流
    # ================================
    @staticmethod
    def _close_stream(stream):
        if stream is None:
            return

        try:
            close_func = getattr(stream, "close", None)
            if callable(close_func):
                close_func()
        except Exception:
            pass

    # ================================
    # 确保父目录存在
    # ================================
    @staticmethod
    def _ensure_parent_dir(path):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    # ================================
    # 解析本地 ETag
    # ================================
    def _resolve_local_etag(self, local_path_bytes, size):
        if not local_path_bytes or size >= 100 * 1024 * 1024:
            return None

        if isinstance(local_path_bytes, bytes):
            path = os.fsdecode(local_path_bytes)
        else:
            path = str(local_path_bytes)

        return calc_file_md5(fix_windows_path(path))

    # ================================
    # 写入报告
    # ================================
    def _report(self, local, obs, size, status, msg):
        if not self.reporter:
            return

        try:
            self.reporter.write(local, obs, size, status, msg)
        except Exception as exc:
            logging.debug("[REPORT_FAIL] %s err=%s", obs, exc)

    # ================================
    # 记录失败任务
    # ================================
    def record_failed(self, path):
        failed_file = os.path.join(self.failed_dir, "failed.txt")

        if os.path.exists(failed_file) and os.path.getsize(failed_file) > 50 * 1024 * 1024:
            os.rename(failed_file, failed_file + ".1")

        with self.lock:
            with open(failed_file, "a", encoding="utf-8") as handle:
                handle.write(path + "\n")


# ================================
# 检查阶段处理器
# ================================
class TaskChecker:
    """执行存在性检查，并把待传输任务转发到下一阶段。"""

    # ================================
    # 初始化检查器
    # ================================
    def __init__(self, uploader, transfer_queue, controls=None):
        self.uploader = uploader
        self.transfer_queue = transfer_queue
        self.controls = controls

    # ================================
    # 处理单个任务
    # ================================
    def process(self, task, heartbeat=None, worker_name=None):
        checked_task = self.uploader.check_task(task, heartbeat=heartbeat, worker_name=worker_name)
        if checked_task is None:
            return
        if self._stop_requested():
            return
        if heartbeat is not None:
            heartbeat("enqueue-transfer")
        if self.controls is None:
            self.transfer_queue.put(checked_task)
            return

        while not self._stop_requested():
            try:
                self.transfer_queue.put(checked_task, timeout=0.05)
                return
            except queue.Full:
                continue

    def _stop_requested(self):
        return self.controls is not None and self.controls.stop_requested()


# ================================
# 传输阶段处理器
# ================================
class TaskTransfer:
    """执行真正的数据传输。"""

    # ================================
    # 初始化传输处理器
    # ================================
    def __init__(self, uploader):
        self.uploader = uploader

    # ================================
    # 处理单个任务
    # ================================
    def process(self, task, heartbeat=None, worker_name=None):
        self.uploader.transfer_task(task, heartbeat=heartbeat, worker_name=worker_name)

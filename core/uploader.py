# core/uploader.py
# -*- coding: utf-8 -*-
"""执行本地与远端对象迁移的上传、复制与跳过判定逻辑。"""

import logging
import os
import random
import shutil
import threading
import time
from types import SimpleNamespace

from obs import (
    CompleteMultipartUploadRequest,
    CompletePart,
    GetObjectHeader,
    ObsClient,
    PutObjectHeader,
)

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
_part_size = None
_threshold = None
_limiter = None

_source_client = None
_source_bucket = None
_source_uri_scheme = "s3"
_source_endpoint_host = ""


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
):
    global _target_type, _target_root, _target_prefix
    global _client, _bucket, _part_size, _threshold, _limiter, _target_uri_scheme, _target_endpoint_host

    normalized_type = (target_type or TARGET_TYPE_S3).strip().lower()
    if normalized_type not in {TARGET_TYPE_LOCAL, TARGET_TYPE_S3}:
        raise ValueError(f"unsupported target type: {target_type}")

    _target_type = normalized_type
    _target_root = os.path.abspath(path) if path else None
    _target_prefix = sanitize_key(prefix or "").strip("/")
    _part_size = part_size
    _threshold = threshold
    _target_endpoint_host = normalize_endpoint(endpoint)

    if normalized_type == TARGET_TYPE_S3:
        _target_uri_scheme = detect_storage_scheme(endpoint, fallback="s3")
        _client = ObsClient(
            access_key_id=ak,
            secret_access_key=sk,
            server=endpoint,
        )
        _bucket = bucket

        from .ratelimiter import RateLimiter

        _limiter = RateLimiter(rate_limit)
    else:
        _target_uri_scheme = ""
        _client = None
        _bucket = None
        _limiter = None
        _target_endpoint_host = ""
        if _target_root:
            os.makedirs(_target_root, exist_ok=True)


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
def init_source_client(ak, sk, endpoint, bucket):
    global _source_client, _source_bucket, _source_uri_scheme, _source_endpoint_host

    if ak and sk and endpoint and bucket:
        _source_uri_scheme = detect_storage_scheme(endpoint, fallback="s3")
        _source_endpoint_host = normalize_endpoint(endpoint)
        _source_client = ObsClient(
            access_key_id=ak,
            secret_access_key=sk,
            server=endpoint,
        )
        _source_bucket = bucket
    else:
        _source_client = None
        _source_bucket = None
        _source_uri_scheme = "s3"
        _source_endpoint_host = ""


# ================================
# 源对象不存在异常
# ================================
class SourceObjectMissingError(FileNotFoundError):
    """表示远端源对象在传输开始前已经不存在。"""

    pass


# ================================
# 上传执行器
# ================================
class OBSUploader:
    """执行上传、下载、服务端拷贝与重试控制。"""

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
    ):
        self.progress = progress
        self.checkpoint = checkpoint
        self.reporter = reporter
        self.failed_dir = failed_dir

        self.enable_head_check = enable_head_check
        self.strict_client_check = strict_client_check
        self.enable_etag_check = enable_etag_check
        self.retry_limit = max(1, int(retry_limit))

        self.lock = threading.Lock()
        self._server_side_copy_disabled = False
        self._server_side_copy_logged = False
        os.makedirs(failed_dir, exist_ok=True)

    # ================================
    # 分发上传任务
    # ================================
    def upload(self, task):
        if self.strict_client_check and _target_type == TARGET_TYPE_S3 and _client is None:
            raise RuntimeError("target client not initialized")

        source_type = (task.get("source_type") or "local").lower()
        if source_type == "s3":
            self._upload_s3_task(task)
            return

        self._upload_local_task(task)

    # ================================
    # 处理本地源任务
    # ================================
    def _upload_local_task(self, task):
        local_path_bytes = task["local"]
        source_ref = task.get("source_path") or fix_windows_path(clean_path_to_utf8(local_path_bytes))
        source_display = task.get("source_display") or source_ref
        relative_path = task.get("relative_path") or os.path.basename(source_ref)
        size = int(task.get("size", 0) or 0)

        try:
            st = os.stat(local_path_bytes, follow_symlinks=False)
            size = st.st_size
            mtime = st.st_mtime
        except FileNotFoundError:
            if os.path.exists(local_path_bytes):
                logging.error("[BUG][ENCODING] %s", safe_log(local_path_bytes))
                self._report(source_display, "", size, "ERROR", "encoding issue")
            else:
                logging.debug("[REAL_MISSING] %s", source_display)
                self._report(source_display, "", size, "MISSING", "file not found")

            self.progress.skip()
            self.progress.add_done(size)
            return

        target_ref, target_display = self._resolve_target(relative_path)
        transfer_fn = (
            (lambda: self._put_local_file_to_s3(local_path_bytes, target_ref, size))
            if _target_type == TARGET_TYPE_S3
            else (lambda: self._copy_local_file_to_local(local_path_bytes, target_ref))
        )

        self._upload_with_retry(
            source_ref=source_ref,
            source_display=source_display,
            target_ref=target_ref,
            target_display=target_display,
            size=size,
            mtime=mtime,
            transfer_fn=transfer_fn,
            source_etag=None,
            local_path_bytes=local_path_bytes,
        )

    # ================================
    # 处理远端源任务
    # ================================
    def _upload_s3_task(self, task):
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
        transfer_fn = (
            (lambda: self._copy_s3_object_to_s3(source_bucket, source_key, target_ref, size))
            if _target_type == TARGET_TYPE_S3
            else (lambda: self._download_s3_to_local(source_bucket, source_key, target_ref))
        )

        self._upload_with_retry(
            source_ref=source_ref,
            source_display=source_display,
            target_ref=target_ref,
            target_display=target_display,
            size=size,
            mtime=mtime,
            transfer_fn=transfer_fn,
            source_etag=task.get("etag"),
            local_path_bytes=None,
        )

    # ================================
    # 带重试执行传输
    # ================================
    def _upload_with_retry(
        self,
        source_ref,
        source_display,
        target_ref,
        target_display,
        size,
        mtime,
        transfer_fn,
        source_etag=None,
        local_path_bytes=None,
    ):
        if self._maybe_skip_existing(
            source_ref=source_ref,
            source_display=source_display,
            target_ref=target_ref,
            target_display=target_display,
            size=size,
            mtime=mtime,
            source_etag=source_etag,
            local_path_bytes=local_path_bytes,
        ):
            return

        retry = 0
        last_err = ""

        while retry < self.retry_limit:
            start = time.time()

            try:
                if _limiter:
                    _limiter.acquire()

                resp = transfer_fn()
                if resp.status < 300:
                    cost = time.time() - start
                    self.checkpoint.mark_done(source_ref, size, mtime)
                    self.progress.add_done(size)
                    logging.info(
                        "[UPLOAD][SUCCESS] %s -> %s size=%s cost=%.2fs",
                        source_display,
                        target_display,
                        size,
                        cost,
                    )
                    self._report(source_display, target_display, size, "SUCCESS", "")
                    return

                raise Exception(f"target status {resp.status}")
            except SourceObjectMissingError as e:
                logging.warning("[SOURCE_MISSING] %s -> %s", source_display, target_display)
                self.progress.skip()
                self.progress.add_done(size)
                self._report(source_display, target_display, size, "MISSING", str(e))
                return
            except Exception as e:
                retry += 1
                self.progress.upload_error_inc()
                last_err = repr(e)
                logging.exception("[RETRY] %s retry=%s", source_display, retry)
                time.sleep(min(2 ** retry + random.random(), 10))

        logging.error("[UPLOAD_FAIL] %s err=%s", source_display, last_err)
        self.record_failed(source_display)
        self._report(
            source_display,
            target_display,
            size,
            "FAILED",
            last_err or f"retry exceeded ({self.retry_limit})",
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
    ):
        if _target_type == TARGET_TYPE_S3:
            return self._maybe_skip_existing_s3(
                source_ref,
                source_display,
                target_ref,
                target_display,
                size,
                mtime,
                source_etag,
                local_path_bytes,
            )

        return self._maybe_skip_existing_local(
            source_ref,
            source_display,
            target_ref,
            target_display,
            size,
            mtime,
            source_etag,
            local_path_bytes,
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
    ):
        index_ready = getattr(self.checkpoint, "obs_index_ready", False)
        index_row = None

        if index_ready:
            logging.debug("[DEBUG_KEY] query=%s", target_key)
            try:
                index_row = self.checkpoint.get_obs(target_key)
            except Exception:
                index_row = None

            logging.debug("[INDEX_RESULT] key=%s row=%s", target_key, index_row)
            if index_row:
                remote_size, _ = index_row
                if remote_size == size:
                    self.progress.cache_hit_inc()
                    if not self.enable_etag_check:
                        self.progress.skip()
                        self.progress.add_done(size)
                        self.checkpoint.mark_done(source_ref, size, mtime)
                        logging.info("[SKIP][INDEX] %s -> %s size=%s", source_display, target_display, size)
                        self._report(source_display, target_display, size, "SKIP", "index(size)")
                        return True
                    logging.debug("[INDEX_HIT_BUT_VERIFY] %s", target_key)
                else:
                    self.progress.cache_miss_inc()
            else:
                self.progress.cache_miss_inc()

        head_status = "UNKNOWN"
        force_head = not index_ready

        if self.enable_head_check or force_head:
            need_head = not index_ready or index_row is not None

            if index_row:
                remote_size, _ = index_row
                if remote_size == size and not self.enable_etag_check:
                    need_head = False

            if need_head:
                try:
                    meta = _client.getObjectMetadata(_bucket, target_key)
                    if meta.status < 300:
                        remote_size = int(meta.body.contentLength or 0)
                        remote_etag = self._normalize_etag(getattr(meta.body, "etag", None))

                        if remote_size != size:
                            head_status = "EXIST_DIFF"
                        else:
                            candidate_etag = source_etag or self._resolve_local_etag(local_path_bytes, size)
                            if self._can_compare_with_etag(candidate_etag, remote_etag):
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
                except Exception as e:
                    logging.debug("[HEAD_FAIL] %s err=%s", target_key, e)
                    head_status = "ERROR"
            else:
                logging.debug("[HEAD_SKIP][INDEX_MISS] %s", target_key)

        if head_status == "EXIST_SAME":
            self.progress.skip()
            self.progress.add_done(size)
            tag = "HEAD_ETAG" if self.enable_etag_check else "HEAD_SIZE"
            logging.info("[SKIP][%s] %s -> %s size=%s", tag, source_display, target_display, size)
            self.checkpoint.mark_done(source_ref, size, mtime)
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
            st = os.stat(target_path)
            target_size = st.st_size
            if target_size != size:
                target_status = "EXIST_DIFF"
            else:
                candidate_etag = source_etag or self._resolve_local_etag(local_path_bytes, size)
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
        except Exception as e:
            logging.debug("[LOCAL_TARGET_CHECK_FAIL] %s err=%s", target_path, e)
            target_status = "ERROR"

        if target_status == "EXIST_SAME":
            self.progress.skip()
            self.progress.add_done(size)
            tag = "LOCAL_ETAG" if self.enable_etag_check else "LOCAL_SIZE"
            logging.info("[SKIP][%s] %s -> %s size=%s", tag, source_display, target_display, size)
            self.checkpoint.mark_done(source_ref, size, mtime)
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
    # 解析目标路径
    # ================================
    def _resolve_target(self, relative_path):
        relative_path = sanitize_key(relative_path or "").strip("/")
        if _target_type == TARGET_TYPE_S3:
            target_key = "/".join(filter(None, [_target_prefix, relative_path]))
            return target_key, build_object_uri(_bucket, target_key, scheme=_target_uri_scheme)

        local_relative = relative_path.replace("/", os.sep)
        target_path = os.path.join(_target_root or "", local_relative)
        target_path = os.path.abspath(target_path)
        return target_path, target_path

    # ================================
    # 本地文件上传到对象存储
    # ================================
    def _put_local_file_to_s3(self, local_path_bytes, target_key, size):
        local_path = fix_windows_path(os.fsdecode(local_path_bytes))

        if size >= _threshold:
            return _client.uploadFile(
                _bucket,
                target_key,
                local_path,
                partSize=_part_size,
                taskNum=3,
                enableCheckpoint=True,
            )

        return _client.putFile(_bucket, target_key, local_path)

    # ================================
    # 本地文件复制到本地
    # ================================
    def _copy_local_file_to_local(self, local_path_bytes, target_path):
        source_path = fix_windows_path(os.fsdecode(local_path_bytes))
        target_path = fix_windows_path(target_path)
        self._ensure_parent_dir(target_path)
        shutil.copy2(source_path, target_path)
        return SimpleNamespace(status=200)

    # ================================
    # 远端对象复制到对象存储
# ================================
    def _copy_s3_object_to_s3(self, source_bucket, source_key, target_key, size):
        if self._should_use_server_side_copy():
            resp = self._copy_s3_object_server_side(source_bucket, source_key, target_key, size)
            if resp is not None:
                return resp

        if size >= _threshold:
            return self._multipart_copy_from_s3(source_bucket, source_key, target_key, size)

        stream = None
        try:
            stream = self._open_source_stream(source_bucket, source_key)
            headers = PutObjectHeader(contentLength=size)
            return _client.putContent(_bucket, target_key, stream, headers=headers)
        finally:
            self._close_stream(stream)

    # ================================
    # 优先尝试服务端拷贝
    # ================================
    def _copy_s3_object_server_side(self, source_bucket, source_key, target_key, size):
        try:
            if size >= _threshold:
                resp = self._multipart_copy_from_s3_server_side(
                    source_bucket,
                    source_key,
                    target_key,
                    size,
                )
            else:
                resp = _client.copyObject(
                    source_bucket,
                    source_key,
                    _bucket,
                    target_key,
                )
        except Exception as exc:
            self._maybe_disable_server_side_copy(error=exc)
            logging.warning(
                "[SERVER_COPY_FALLBACK] %s -> %s err=%s",
                build_object_uri(source_bucket, source_key, scheme=_source_uri_scheme),
                build_object_uri(_bucket, target_key, scheme=_target_uri_scheme),
                exc,
            )
            return None

        if getattr(resp, "status", 500) < 300:
            with self.lock:
                if not self._server_side_copy_logged:
                    logging.info(
                        "[SERVER_COPY_ENABLED] endpoint=%s bucket=%s",
                        _target_endpoint_host,
                        _bucket,
                    )
                    self._server_side_copy_logged = True
            logging.debug("[SERVER_COPY] %s -> %s", source_key, target_key)
            return resp

        self._maybe_disable_server_side_copy(status=getattr(resp, "status", None))
        logging.warning(
            "[SERVER_COPY_FALLBACK] %s -> %s status=%s",
            build_object_uri(source_bucket, source_key, scheme=_source_uri_scheme),
            build_object_uri(_bucket, target_key, scheme=_target_uri_scheme),
            getattr(resp, "status", None),
        )
        return None

    # ================================
    # 远端对象下载到本地
    # ================================
    def _download_s3_to_local(self, source_bucket, source_key, target_path):
        stream = None
        target_path = fix_windows_path(target_path)
        part_path = target_path + ".part"
        self._ensure_parent_dir(target_path)

        try:
            stream = self._open_source_stream(source_bucket, source_key)
            with open(part_path, "wb") as fp:
                while True:
                    chunk = stream.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    fp.write(chunk)

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
    def _multipart_copy_from_s3(self, source_bucket, source_key, target_key, size):
        init_resp = _client.initiateMultipartUpload(_bucket, target_key)
        if init_resp.status >= 300:
            raise Exception(f"init multipart failed {init_resp.status}")

        upload_id = init_resp.body.uploadId
        parts = []
        offset = 0
        part_number = 1

        try:
            while offset < size:
                current_part_size = min(_part_size, size - offset)
                stream = None
                try:
                    stream = self._open_source_stream(
                        source_bucket,
                        source_key,
                        offset=offset,
                        part_size=current_part_size,
                    )
                    resp = _client.uploadPart(
                        _bucket,
                        target_key,
                        part_number,
                        upload_id,
                        content=stream,
                        partSize=current_part_size,
                    )
                finally:
                    self._close_stream(stream)

                if resp.status >= 300:
                    raise Exception(
                        f"upload part failed part={part_number} status={resp.status}"
                    )

                parts.append(
                    CompletePart(
                        partNum=part_number,
                        etag=getattr(resp.body, "etag", None),
                        crc64=getattr(resp.body, "crc64", None),
                        size=current_part_size,
                    )
                )

                offset += current_part_size
                part_number += 1

            return _client.completeMultipartUpload(
                _bucket,
                target_key,
                upload_id,
                CompleteMultipartUploadRequest(parts=parts),
            )
        except Exception:
            try:
                _client.abortMultipartUpload(_bucket, target_key, upload_id)
            except Exception as abort_err:
                logging.debug("[ABORT_MULTIPART_FAIL] %s", abort_err)
            raise

    # ================================
    # 服务端分片复制
    # ================================
    def _multipart_copy_from_s3_server_side(self, source_bucket, source_key, target_key, size):
        init_resp = _client.initiateMultipartUpload(_bucket, target_key)
        if init_resp.status >= 300:
            raise Exception(f"init multipart failed {init_resp.status}")

        upload_id = init_resp.body.uploadId
        parts = []
        offset = 0
        part_number = 1
        copy_source = f"/{source_bucket}/{source_key}"

        try:
            while offset < size:
                current_part_size = min(_part_size, size - offset)
                range_end = offset + current_part_size - 1
                resp = _client.copyPart(
                    _bucket,
                    target_key,
                    part_number,
                    upload_id,
                    copy_source,
                    copySourceRange=f"{offset}-{range_end}",
                )

                if resp.status >= 300:
                    raise Exception(
                        f"copy part failed part={part_number} status={resp.status}"
                    )

                parts.append(
                    CompletePart(
                        partNum=part_number,
                        etag=getattr(resp.body, "etag", None),
                        size=current_part_size,
                    )
                )

                offset += current_part_size
                part_number += 1

            return _client.completeMultipartUpload(
                _bucket,
                target_key,
                upload_id,
                CompleteMultipartUploadRequest(parts=parts),
            )
        except Exception:
            try:
                _client.abortMultipartUpload(_bucket, target_key, upload_id)
            except Exception as abort_err:
                logging.debug("[ABORT_SERVER_COPY_FAIL] %s", abort_err)
            raise

    # ================================
    # 打开源对象流
    # ================================
    def _open_source_stream(self, source_bucket, source_key, offset=None, part_size=None):
        headers = None
        if offset is not None and part_size is not None:
            headers = GetObjectHeader(range=f"{offset}-{offset + part_size - 1}")

        resp = _source_client.getObject(source_bucket, source_key, headers=headers)
        if resp.status == 404:
            raise SourceObjectMissingError(
                f"source object not found: {build_object_uri(source_bucket, source_key, scheme=_source_uri_scheme)}"
            )
        if resp.status >= 300:
            raise Exception(f"source get error {resp.status}")

        return resp.body.response

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
            close = getattr(stream, "close", None)
            if callable(close):
                close()
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
        return calc_file_md5(fix_windows_path(os.fsdecode(local_path_bytes)))

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
    # 判断是否可直接比较 ETag
    # ================================
    def _can_compare_with_etag(self, source_etag, remote_etag):
        if not self.enable_etag_check:
            return False

        source_etag = self._normalize_etag(source_etag)
        remote_etag = self._normalize_etag(remote_etag)
        return bool(
            source_etag
            and remote_etag
            and "-" not in source_etag
            and "-" not in remote_etag
        )

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
    # 写入报告
    # ================================
    def _report(self, local, obs, size, status, msg):
        if not self.reporter:
            return

        try:
            self.reporter.write(local, obs, size, status, msg)
        except Exception as e:
            logging.debug("[REPORT_FAIL] %s err=%s", obs, e)

    # ================================
    # 记录失败任务
    # ================================
    def record_failed(self, path):
        failed_file = os.path.join(self.failed_dir, "failed.txt")

        if os.path.exists(failed_file) and os.path.getsize(failed_file) > 50 * 1024 * 1024:
            os.rename(failed_file, failed_file + ".1")

        with self.lock:
            with open(failed_file, "a", encoding="utf-8") as fp:
                fp.write(path + "\n")

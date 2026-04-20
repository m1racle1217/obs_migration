# core/uploader.py
# -*- coding: utf-8 -*-

import random
import logging
import os
import threading
import time

from obs import ObsClient
from .utils import (
    calc_file_md5,
    clean_path_to_utf8,
    fix_windows_path,
    safe_log,
    sanitize_key,
)

_client = None
_bucket = None
_part_size = None
_threshold = None
_limiter = None





def init_uploader(ak, sk, endpoint, bucket, part_size, threshold, rate_limit=200):
    global _client, _bucket, _part_size, _threshold, _limiter

    _client = ObsClient(
        access_key_id=ak,
        secret_access_key=sk,
        server=endpoint
    )

    _bucket = bucket
    _part_size = part_size
    _threshold = threshold

    from .ratelimiter import RateLimiter
    _limiter = RateLimiter(rate_limit)


class OBSUploader:

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
        os.makedirs(failed_dir, exist_ok=True)

    # ==========================================================
    # 核心上传
    # ==========================================================
    def upload(self, task):
        if self.strict_client_check and _client is None:
            raise RuntimeError("OBS client not initialized")

        # =========================
        # 原始路径（bytes，唯一可信）
        # =========================
        local_path_bytes = task["local"]

        # ✅ 只用于日志展示（不会参与IO）
        safe_local_str = clean_path_to_utf8(local_path_bytes)
        safe_local_str = fix_windows_path(safe_local_str)

        obs_key = sanitize_key(task["obs"]).strip("/")
        logging.debug(f"[UPLOAD_KEY] {obs_key}")

        size = task.get("size", 0)

        # =========================
        # 文件 stat（必须用 bytes）
        # =========================
        try:
            st = os.stat(local_path_bytes, follow_symlinks=False)
            size = st.st_size
            mtime = st.st_mtime

        except FileNotFoundError:

            # ⚠️ 再确认一次（防止误判）
            if os.path.exists(local_path_bytes):
                # 说明是编码/系统异常
                logging.error(f"[BUG][ENCODING] {safe_log(local_path_bytes)}")
                self._report(safe_local_str, obs_key, size, "ERROR", "encoding issue")
            else:
                # ✅ 真正不存在
                logging.debug(f"[REAL_MISSING] {safe_local_str}")
                self._report(safe_local_str, obs_key, size, "MISSING", "file not found")

            self.progress.skip()
            self.progress.add_done(size)
            return
        # =========================
        # INDEX 判断（超快）
        # =========================
        if getattr(self.checkpoint, "obs_index_ready", False):
            logging.debug(f"[DEBUG_KEY] query={obs_key}")

            try:
                row = self.checkpoint.get_obs(obs_key)
            except Exception:
                row = None
            logging.debug(f"[INDEX_RESULT] key={obs_key} row={row}")
            if row:
                remote_size, _ = row

                if remote_size == size:

                    # ✅ 命中缓存
                    self.progress.cache_hit_inc()

                    if not self.enable_etag_check:
                        self.progress.skip()
                        self.progress.add_done(size)

                        self.checkpoint.mark_done(safe_local_str, size, mtime)
                        logging.info(
                            f"[SKIP][INDEX] {safe_local_str} -> {obs_key} size={size}"
                        )

                        self._report(
                            safe_local_str,
                            obs_key,
                            size,
                            "SKIP",
                            "index(size)"
                        )
                        return

                    # ✅ 开启 ETAG → 不跳过，继续走 HEAD
                    logging.debug(f"[INDEX_HIT_BUT_VERIFY] {obs_key}")

                else:
                    self.progress.cache_miss_inc()
            else:
                self.progress.cache_miss_inc()
        # =========================
        # HEAD 判断
        # =========================
        head_status = "UNKNOWN"

        # 强制：没有index就必须HEAD
        force_head = not getattr(self.checkpoint, "obs_index_ready", False)

        if self.enable_head_check or force_head:

            need_head = True

            if getattr(self.checkpoint, "obs_index_ready", False):
                try:
                    row = self.checkpoint.get_obs(obs_key)
                except Exception:
                    row = None

                if row:
                    remote_size, _ = row

                    if remote_size == size:
                        if self.enable_etag_check:
                            need_head = True  # 🔥 强制走 HEAD
                        else:
                            need_head = False

            if need_head:
                try:
                    meta = _client.getObjectMetadata(_bucket, obs_key)

                    if meta.status < 300:
                        remote_size = meta.body.contentLength
                        remote_etag = meta.body.etag.strip('"')

                        if remote_size != size:
                            head_status = "EXIST_DIFF"

                        else:
                            if self.enable_etag_check and size < 100 * 1024 * 1024:

                                logging.debug(f"[ETAG_CHECK] {obs_key}")

                                local_etag = calc_file_md5(
                                    fix_windows_path(os.fsdecode(local_path_bytes))
                                )

                                if local_etag == remote_etag:
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
                    logging.debug(f"[HEAD_FAIL] {obs_key} err={e}")
                    head_status = "ERROR"

        # =========================
        # 3️⃣ 决策逻辑（核心）
        # =========================
        if head_status == "EXIST_SAME":
            self.progress.skip()
            self.progress.add_done(size)

            if self.enable_etag_check:
                tag = "HEAD_ETAG"
            else:
                tag = "HEAD_SIZE"

            logging.info(
                f"[SKIP][{tag}] {safe_local_str} -> {obs_key} size={size}"
            )

            self.checkpoint.mark_done(safe_local_str, size, mtime)

            self._report(safe_local_str, obs_key, size, "SKIP", "already exists")
            return

        # ⚠️ HEAD失败 → 才允许用 checkpoint
        if head_status == "ERROR":

            if self.checkpoint.is_done(safe_local_str, size, mtime):
                logging.info(
                    f"[SKIP][CHECKPOINT] {safe_local_str} -> {obs_key}"
                )

                self.progress.skip()
                self.progress.add_done(size)

                self._report(safe_local_str, obs_key, size, "SKIP", "checkpoint")

                return

        # =========================
        # 4️⃣ 上传（关键：路径转换）
        # =========================
        retry = 0
        last_err = ""

        # ⚠️ 这里只在上传时转换（OBS SDK必须用str）
        local_str_for_upload = fix_windows_path(os.fsdecode(local_path_bytes))

        while retry < self.retry_limit:

            start = time.time()

            try:
                if _limiter:
                    tokens_needed = max(1, int(size / (512 * 1024)))
                    _limiter.acquire(tokens_needed)

                if size >= _threshold:
                    resp = _client.uploadFile(
                        _bucket,
                        obs_key,
                        local_str_for_upload,
                        partSize=_part_size,
                        taskNum=3,
                        enableCheckpoint=True
                    )
                else:
                    resp = _client.putFile(_bucket, obs_key, local_str_for_upload)

                if resp.status < 300:
                    cost = time.time() - start

                    self.checkpoint.mark_done(safe_local_str, size, mtime)
                    self.progress.add_done(size)

                    logging.info(
                        f"[UPLOAD][SUCCESS] {safe_local_str} -> {obs_key} size={size} cost={cost:.2f}s"
                    )

                    self._report(safe_local_str, obs_key, size, "SUCCESS", "")

                    return

                raise Exception(f"OBS status {resp.status}")



            except Exception as e:

                retry += 1

                self.progress.upload_error_inc()

                last_err = repr(e)

                logging.exception(

                    f"[RETRY] {safe_local_str} retry={retry}"

                )

                time.sleep(min(2 ** retry + random.random(), 10))

        # =========================
        # 5️⃣ 失败
        # =========================
        logging.error(f"[UPLOAD_FAIL] {safe_local_str} err={last_err}")

        self.record_failed(safe_local_str)

        self._report(
            safe_local_str,
            obs_key,
            size,
            "FAILED",
            last_err or f"retry exceeded ({self.retry_limit})"
        )

    # ==========================================================
    # 统一写报告（唯一出口）
    # ==========================================================
    def _report(self, local, obs, size, status, msg):

        if not self.reporter:
            return

        try:
            self.reporter.write(local, obs, size, status, msg)
        except Exception as e:
            logging.debug(f"[REPORT_FAIL] {obs} err={e}")
    # ==========================================================
    # 失败记录
    # ==========================================================
    def record_failed(self, path):

        f = os.path.join(self.failed_dir, "failed.txt")

        if os.path.exists(f) and os.path.getsize(f) > 50 * 1024 * 1024:
            os.rename(f, f + ".1")

        with self.lock:
            with open(f, "a", encoding="utf-8") as fp:
                fp.write(path + "\n")

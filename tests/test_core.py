"""测试迁移核心组件与命令行辅助逻辑。"""

import configparser
import csv
import io
import json
import os
import queue
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import obs_migrate
import core.uploader as uploader_module
from rich.panel import Panel
from core.checkpoint import Checkpoint
from core.dashboard import Dashboard
from core.obs_index import build_obs_index
from core.progress import Progress
from core.report import Reporter
from core.scan_control import AdaptiveScanController
from core.s3_scanner import scan_s3_objects
from core.scanner import scan_directory
from core.uploader import OBSUploader
from core.utils import build_object_uri, detect_storage_scheme


# ================================
# 内存版测试报告器
# ================================
class MemoryReporter:
    """测试中使用的内存报告器，用于收集写出的报告记录。"""

    # ================================
    # 初始化内存报告器
    # ================================
    def __init__(self):
        self.rows = []
        self.tracked = []

    # ================================
    # 记录一条报告结果
    # ================================
    def write(self, local, obs, size=0, status="UNKNOWN", msg=""):
        self.rows.append(
            {
                "local": local,
                "obs": obs,
                "size": size,
                "status": status,
                "msg": msg,
            }
        )

    # ================================
    # 记录待处理任务
    # ================================
    def track_task(self, source_path, size=0, target_path="", msg=""):
        self.tracked.append(
            {
                "source_path": source_path,
                "target_path": target_path,
                "size": size,
                "msg": msg,
            }
        )


# ================================
# 测试断点管理器
# ================================
class CheckpointTests(unittest.TestCase):
    """测试 SQLite 断点持久化与目标索引状态。"""

    # ================================
    # 验证批量刷盘与索引重置
    # ================================
    def test_completed_batch_flushes_on_threshold_and_resets_obs_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state" / "tasks.db"
            checkpoint = Checkpoint(str(db_path), batch_size=2)

            checkpoint.mark_done("a.txt", 1, 1.0)
            with closing(sqlite3.connect(db_path)) as conn:
                count = conn.execute("SELECT COUNT(*) FROM completed").fetchone()[0]
            self.assertEqual(count, 0)

            checkpoint.mark_done("b.txt", 2, 2.0)
            with closing(sqlite3.connect(db_path)) as conn:
                count = conn.execute("SELECT COUNT(*) FROM completed").fetchone()[0]
            self.assertEqual(count, 2)

            checkpoint.upsert_obs_many([
                ("prefix/a.txt", 1, "etag-a"),
                ("prefix/b.txt", 2, "etag-b"),
            ])
            checkpoint.set_index_ready()

            self.assertTrue(checkpoint.obs_index_ready)
            self.assertEqual(checkpoint.get_obs("prefix/a.txt"), (1, "etag-a"))

            checkpoint.reset_obs_index()

            self.assertFalse(checkpoint.obs_index_ready)
            self.assertIsNone(checkpoint.get_obs("prefix/a.txt"))

            checkpoint.close()


# ================================
# 测试对象索引构建
# ================================
class ObsIndexTests(unittest.TestCase):
    """测试远端对象索引的构建与持久化。"""

    # ================================
    # 验证索引重建后可用
    # ================================
    def test_build_obs_index_rebuilds_and_marks_ready(self):
        # ================================
        # 模拟分页列举客户端
        # ================================
        class FakeObsClient:
            # ================================
            # 初始化假客户端
            # ================================
            def __init__(self, *args, **kwargs):
                self.calls = 0

            # ================================
            # 返回模拟分页结果
            # ================================
            def listObjects(self, bucket, prefix="", marker=None, max_keys=1000):
                self.calls += 1
                if self.calls == 1:
                    return SimpleNamespace(
                        status=200,
                        body=SimpleNamespace(
                            contents=[
                                SimpleNamespace(key="backup/a.txt", size=1, etag="etag-a")
                            ],
                            is_truncated=True,
                            next_marker="page-2",
                        ),
                    )

                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(
                        contents=[
                            SimpleNamespace(key="backup/b.txt", size=2, etag="etag-b")
                        ],
                        is_truncated=False,
                        next_marker=None,
                    ),
                )

        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = Checkpoint(str(Path(tmp) / "state" / "tasks.db"))
            checkpoint.reset_obs_index()

            with patch("core.obs_index.ObsClient", FakeObsClient):
                build_obs_index("ak", "sk", "endpoint", "bucket", "backup", checkpoint)

            self.assertTrue(checkpoint.obs_index_ready)
            self.assertEqual(checkpoint.get_obs("backup/a.txt"), (1, "etag-a"))
            self.assertEqual(checkpoint.get_obs("backup/b.txt"), (2, "etag-b"))

            checkpoint.close()


# ================================
# 测试结果报告器
# ================================
class ReporterTests(unittest.TestCase):
    """测试已完成与未完成任务的 CSV / JSON 报告。"""

    # ================================
    # 验证关闭时补写未完成任务
    # ================================
    def test_reporter_flushes_unfinished_tasks_on_close(self):
        with tempfile.TemporaryDirectory() as tmp:
            reporter = Reporter(tmp, "obs://src-bucket/source/prefix")
            reporter.track_task("obs://src-bucket/source/prefix/a.txt", size=12)
            reporter.track_task("obs://src-bucket/source/prefix/b.txt", size=34)
            reporter.write(
                "obs://src-bucket/source/prefix/a.txt",
                "obs://dst-bucket/target/prefix/a.txt",
                size=12,
                status="SUCCESS",
                msg="",
            )
            reporter.close(
                pending_status="INTERRUPTED",
                pending_message="detected_but_not_migrated",
            )

            with open(reporter.file, "r", encoding="utf-8") as fp:
                rows = list(csv.DictReader(fp))

            self.assertEqual(len(rows), 2)
            by_source = {row["source_path"]: row for row in rows}
            self.assertEqual(
                by_source["obs://src-bucket/source/prefix/a.txt"]["status"],
                "SUCCESS",
            )
            self.assertEqual(
                by_source["obs://src-bucket/source/prefix/b.txt"]["status"],
                "INTERRUPTED",
            )
            self.assertEqual(
                by_source["obs://src-bucket/source/prefix/b.txt"]["target_path"],
                "",
            )
            self.assertEqual(
                by_source["obs://src-bucket/source/prefix/b.txt"]["size"],
                "34",
            )
            self.assertEqual(
                by_source["obs://src-bucket/source/prefix/b.txt"]["message"],
                "detected_but_not_migrated",
            )

            with open(reporter.summary_file, "r", encoding="utf-8") as fp:
                summary = json.load(fp)

            self.assertEqual(summary["SUCCESS"], 1)
            self.assertEqual(summary["INTERRUPTED"], 1)
            self.assertEqual(summary["TOTAL_FILES"], 2)


# ================================
# 测试扫描逻辑
# ================================
class ScannerTests(unittest.TestCase):
    """测试本地与远端扫描行为。"""

    # ================================
    # 验证扫描线程会随队列压力收缩
    # ================================
    def test_adaptive_scan_controller_scales_with_queue_pressure(self):
        task_queue = queue.Queue(maxsize=10)
        controller = AdaptiveScanController(
            task_queue,
            max_workers=8,
            min_workers=1,
            sample_interval=0,
        )

        self.assertEqual(controller.get_desired_workers(), 8)

        for index in range(9):
            task_queue.put(index)

        self.assertLess(controller.get_desired_workers(), 8)
        self.assertGreaterEqual(controller.get_desired_workers(), 1)

        while not task_queue.empty():
            task_queue.get_nowait()

        self.assertEqual(controller.get_desired_workers(), 8)

    # ================================
    # 验证本地扫描入队与跳过记录
    # ================================
    def test_scan_directory_enqueues_files_and_reports_skips(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "sub").mkdir()
            (root / "file.txt").write_text("hello", encoding="utf-8")
            (root / "sub" / "nested.bin").write_bytes(b"1234")
            (root / ".hidden").write_text("skip", encoding="utf-8")
            (root / "Thumbs.db").write_text("skip", encoding="utf-8")
            (root / "temp.part").write_text("skip", encoding="utf-8")

            reporter = MemoryReporter()
            progress = Progress()
            task_queue = queue.Queue()

            scan_directory(
                str(root),
                task_queue,
                progress,
                checkpoint=None,
                reporter=reporter,
                scan_workers=2,
            )

            tasks = []
            while not task_queue.empty():
                tasks.append(task_queue.get_nowait())

            self.assertEqual(len(tasks), 2)
            self.assertEqual(
                sorted(task["relative_path"] for task in tasks),
                ["file.txt", "sub/nested.bin"],
            )
            self.assertTrue(all(task["source_type"] == "local" for task in tasks))

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["scan_files"], 2)
            self.assertEqual(snapshot["scan_skip"], 3)
            self.assertEqual(snapshot["scan_errors"], 0)
            self.assertEqual(snapshot["total_bytes"], 9)

            self.assertEqual(len(reporter.rows), 3)
            self.assertEqual(
                sorted(row["status"] for row in reporter.rows),
                ["SKIP_SCAN", "SKIP_SCAN", "SKIP_SCAN"],
            )
            self.assertEqual(
                sorted(item["source_path"] for item in reporter.tracked),
                sorted([
                    str(root / "file.txt"),
                    str(root / "sub" / "nested.bin"),
                ]),
            )

    # ================================
    # 验证远端对象扫描入队
    # ================================
    def test_scan_s3_objects_enqueues_remote_objects(self):
        # ================================
        # 模拟源端对象列举客户端
        # ================================
        class FakeSourceClient:
            # ================================
            # 返回模拟对象与子前缀
            # ================================
            def listObjects(self, bucket, prefix="", marker=None, max_keys=1000, delimiter=None):
                if prefix == "source/prefix/sub/":
                    return SimpleNamespace(
                        status=200,
                        body=SimpleNamespace(
                            contents=[
                                SimpleNamespace(
                                    key="source/prefix/sub/data.bin",
                                    size=7,
                                    etag="etag-b",
                                    lastModified=datetime(2024, 1, 2, tzinfo=timezone.utc),
                                ),
                            ],
                            commonPrefixs=[],
                            is_truncated=False,
                            next_marker=None,
                        ),
                    )

                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(
                        contents=[
                            SimpleNamespace(
                                key="source/prefix/file.txt",
                                size=5,
                                etag="etag-a",
                                lastModified=datetime(2024, 1, 1, tzinfo=timezone.utc),
                            ),
                            SimpleNamespace(
                                key="source/prefix/folder/",
                                size=0,
                                etag=None,
                                lastModified=None,
                            ),
                        ],
                        commonPrefixs=[
                            SimpleNamespace(prefix="source/prefix/sub/"),
                        ],
                        is_truncated=False,
                        next_marker=None,
                    ),
                )

        reporter = MemoryReporter()
        progress = Progress()
        task_queue = queue.Queue()

        scan_s3_objects(
            FakeSourceClient(),
            "src-bucket",
            "source/prefix",
            task_queue,
            progress,
            reporter=reporter,
            scan_workers=4,
            source_scheme="obs",
        )

        tasks = []
        while not task_queue.empty():
            tasks.append(task_queue.get_nowait())

        self.assertEqual(len(tasks), 2)
        self.assertEqual(
            sorted(task["relative_path"] for task in tasks),
            ["file.txt", "sub/data.bin"],
        )
        self.assertTrue(all(task["source_type"] == "s3" for task in tasks))
        self.assertEqual(tasks[0]["source_path"], "s3://src-bucket/source/prefix/file.txt")
        self.assertEqual(tasks[0]["source_display"], "obs://src-bucket/source/prefix/file.txt")

        snapshot = progress.snapshot()
        self.assertEqual(snapshot["scan_files"], 2)
        self.assertEqual(snapshot["scan_skip"], 1)
        self.assertEqual(snapshot["scan_errors"], 0)
        self.assertEqual(snapshot["total_bytes"], 12)

        self.assertEqual(len(reporter.rows), 1)
        self.assertEqual(reporter.rows[0]["status"], "SKIP_SCAN")
        self.assertEqual(reporter.rows[0]["msg"], "directory_marker")
        self.assertEqual(reporter.rows[0]["local"], "obs://src-bucket/source/prefix/folder")
        self.assertEqual(
            sorted(item["source_path"] for item in reporter.tracked),
            sorted([
                "obs://src-bucket/source/prefix/file.txt",
                "obs://src-bucket/source/prefix/sub/data.bin",
            ]),
        )


# ================================
# 测试上传器逻辑
# ================================
class UploaderTests(unittest.TestCase):
    """测试上传、复制、重试与跳过判定逻辑。"""

    # ================================
    # 验证失败重试次数受配置控制
    # ================================
    def test_uploader_uses_configured_retry_limit_for_s3_target(self):
        # ================================
        # 模拟目标端客户端
        # ================================
        class FakeTargetClient:
            # ================================
            # 初始化统计字段
            # ================================
            def __init__(self):
                self.head_calls = 0
                self.put_calls = 0

            # ================================
            # 模拟目标端 HEAD 请求
            # ================================
            def getObjectMetadata(self, bucket, key):
                self.head_calls += 1
                return SimpleNamespace(status=404, body=SimpleNamespace())

            # ================================
            # 模拟上传文件失败
            # ================================
            def putFile(self, bucket, key, local_path):
                self.put_calls += 1
                return SimpleNamespace(status=500)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            failed_dir = root / "failed"
            local_file = root / "payload.txt"
            local_file.write_text("payload", encoding="utf-8")

            progress = Progress()
            checkpoint = Checkpoint(str(root / "state" / "tasks.db"))
            fake_target = FakeTargetClient()

            uploader = OBSUploader(
                progress,
                checkpoint,
                failed_dir=str(failed_dir),
                retry_limit=2,
            )

            with patch.object(uploader_module, "_target_type", "s3"), \
                    patch.object(uploader_module, "_client", fake_target), \
                    patch.object(uploader_module, "_bucket", "bucket"), \
                    patch.object(uploader_module, "_target_prefix", "backup"), \
                    patch.object(uploader_module, "_threshold", 1024 * 1024), \
                    patch.object(uploader_module, "_part_size", 1024 * 1024), \
                    patch.object(uploader_module, "_limiter", None), \
                    patch("core.uploader.time.sleep", return_value=None):
                uploader.upload(
                    {
                        "source_type": "local",
                        "local": str(local_file),
                        "source_path": str(local_file),
                        "relative_path": "payload.txt",
                        "size": local_file.stat().st_size,
                    }
                )

            self.assertEqual(fake_target.head_calls, 1)
            self.assertEqual(fake_target.put_calls, 2)

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["upload_errors"], 2)

            failed_log = failed_dir / "failed.txt"
            self.assertTrue(failed_log.exists())
            self.assertIn(str(local_file), failed_log.read_text(encoding="utf-8"))

            checkpoint.close()

    # ================================
    # 验证小对象走流式上传
    # ================================
    def test_uploader_streams_s3_source_for_small_s3_target_objects(self):
        # ================================
        # 模拟目标端内容上传客户端
        # ================================
        class FakeTargetClient:
            # ================================
            # 初始化目标端统计字段
            # ================================
            def __init__(self):
                self.head_calls = 0
                self.put_content_calls = []

            # ================================
            # 模拟目标端 HEAD 请求
            # ================================
            def getObjectMetadata(self, bucket, key):
                self.head_calls += 1
                return SimpleNamespace(status=404, body=SimpleNamespace())

            # ================================
            # 模拟流式上传内容
            # ================================
            def putContent(self, bucket, key, stream, headers=None):
                self.put_content_calls.append(
                    {
                        "bucket": bucket,
                        "key": key,
                        "body": stream.read(),
                        "content_length": headers.contentLength,
                    }
                )
                return SimpleNamespace(status=200)

        # ================================
        # 模拟源端对象读取客户端
        # ================================
        class FakeSourceClient:
            # ================================
            # 返回对象流内容
            # ================================
            def getObject(self, bucket, key, headers=None):
                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(response=io.BytesIO(b"payload")),
                )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            progress = Progress()
            checkpoint = Checkpoint(str(root / "state" / "tasks.db"))
            fake_target = FakeTargetClient()
            fake_source = FakeSourceClient()

            uploader = OBSUploader(
                progress,
                checkpoint,
                reporter=MemoryReporter(),
                failed_dir=str(root / "failed"),
                retry_limit=2,
            )

            with patch.object(uploader_module, "_target_type", "s3"), \
                    patch.object(uploader_module, "_client", fake_target), \
                    patch.object(uploader_module, "_bucket", "dst-bucket"), \
                    patch.object(uploader_module, "_target_prefix", "backup"), \
                    patch.object(uploader_module, "_target_uri_scheme", "obs"), \
                    patch.object(uploader_module, "_threshold", 1024 * 1024), \
                    patch.object(uploader_module, "_part_size", 1024 * 1024), \
                    patch.object(uploader_module, "_limiter", None), \
                    patch.object(uploader_module, "_source_client", fake_source), \
                    patch.object(uploader_module, "_source_bucket", "src-bucket"), \
                    patch.object(uploader_module, "_source_uri_scheme", "obs"):
                uploader.upload(
                    {
                        "source_type": "s3",
                        "source_bucket": "src-bucket",
                        "source_key": "path/payload.bin",
                        "source_path": "s3://src-bucket/path/payload.bin",
                        "source_display": "obs://src-bucket/path/payload.bin",
                        "relative_path": "payload.bin",
                        "size": 7,
                        "mtime": 123.0,
                        "etag": "etag-a",
                    }
                )

            self.assertEqual(fake_target.head_calls, 1)
            self.assertEqual(len(fake_target.put_content_calls), 1)
            self.assertEqual(fake_target.put_content_calls[0]["key"], "backup/payload.bin")
            self.assertEqual(fake_target.put_content_calls[0]["body"], b"payload")
            self.assertEqual(fake_target.put_content_calls[0]["content_length"], 7)

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["files_done"], 1)
            self.assertEqual(snapshot["done_bytes"], 7)
            self.assertTrue(checkpoint.is_done("s3://src-bucket/path/payload.bin", 7, 123.0))
            self.assertEqual(
                uploader.reporter.rows[0]["local"],
                "obs://src-bucket/path/payload.bin",
            )
            self.assertEqual(
                uploader.reporter.rows[0]["obs"],
                "obs://dst-bucket/backup/payload.bin",
            )

            checkpoint.close()

    # ================================
    # 验证小对象优先服务端拷贝
    # ================================
    def test_uploader_uses_single_rate_limit_token_per_transfer(self):
        class FakeLimiter:
            def __init__(self):
                self.calls = []

            def acquire(self, tokens=1):
                self.calls.append(tokens)

        class FakeTargetClient:
            def __init__(self):
                self.copy_calls = []

            def getObjectMetadata(self, bucket, key):
                return SimpleNamespace(status=404, body=SimpleNamespace())

            def copyObject(self, source_bucket, source_key, dest_bucket, dest_key):
                self.copy_calls.append((source_bucket, source_key, dest_bucket, dest_key))
                return SimpleNamespace(status=200)

        huge_size = 6 * 1024 * 1024 * 1024

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            progress = Progress()
            checkpoint = Checkpoint(str(root / "state" / "tasks.db"))
            checkpoint.set_index_ready()
            fake_target = FakeTargetClient()
            fake_limiter = FakeLimiter()

            uploader = OBSUploader(
                progress,
                checkpoint,
                reporter=MemoryReporter(),
                failed_dir=str(root / "failed"),
                retry_limit=2,
            )

            with patch.object(uploader_module, "_target_type", "s3"), \
                    patch.object(uploader_module, "_client", fake_target), \
                    patch.object(uploader_module, "_bucket", "dst-bucket"), \
                    patch.object(uploader_module, "_target_prefix", "backup"), \
                    patch.object(uploader_module, "_target_uri_scheme", "obs"), \
                    patch.object(uploader_module, "_target_endpoint_host", "obs.cn-south-1.myhuaweicloud.com"), \
                    patch.object(uploader_module, "_threshold", huge_size + 1), \
                    patch.object(uploader_module, "_part_size", 128 * 1024 * 1024), \
                    patch.object(uploader_module, "_limiter", fake_limiter), \
                    patch.object(uploader_module, "_source_client", object()), \
                    patch.object(uploader_module, "_source_bucket", "src-bucket"), \
                    patch.object(uploader_module, "_source_uri_scheme", "obs"), \
                    patch.object(uploader_module, "_source_endpoint_host", "obs.cn-south-1.myhuaweicloud.com"):
                uploader.upload(
                    {
                        "source_type": "s3",
                        "source_bucket": "src-bucket",
                        "source_key": "path/huge.bin",
                        "source_path": "s3://src-bucket/path/huge.bin",
                        "source_display": "obs://src-bucket/path/huge.bin",
                        "relative_path": "huge.bin",
                        "size": huge_size,
                        "mtime": 123.0,
                        "etag": "etag-a",
                    }
                )

            self.assertEqual(fake_limiter.calls, [1])
            self.assertEqual(
                fake_target.copy_calls,
                [("src-bucket", "path/huge.bin", "dst-bucket", "backup/huge.bin")],
            )

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["files_done"], 1)
            self.assertEqual(snapshot["done_bytes"], huge_size)

            checkpoint.close()

    def test_uploader_prefers_server_side_copy_for_small_s3_objects(self):
        # ================================
        # 模拟目标端拷贝客户端
        # ================================
        class FakeTargetClient:
            # ================================
            # 初始化目标端统计字段
            # ================================
            def __init__(self):
                self.head_calls = 0
                self.copy_calls = []

            # ================================
            # 模拟服务端拷贝
            # ================================
            def copyObject(self, source_bucket, source_key, dest_bucket, dest_key):
                self.copy_calls.append((source_bucket, source_key, dest_bucket, dest_key))
                return SimpleNamespace(status=200)

            # ================================
            # 模拟目标端 HEAD 请求
            # ================================
            def getObjectMetadata(self, bucket, key):
                self.head_calls += 1
                return SimpleNamespace(status=404, body=SimpleNamespace())

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            progress = Progress()
            checkpoint = Checkpoint(str(root / "state" / "tasks.db"))
            checkpoint.set_index_ready()
            fake_target = FakeTargetClient()

            uploader = OBSUploader(
                progress,
                checkpoint,
                reporter=MemoryReporter(),
                failed_dir=str(root / "failed"),
                retry_limit=2,
            )

            with patch.object(uploader_module, "_target_type", "s3"), \
                    patch.object(uploader_module, "_client", fake_target), \
                    patch.object(uploader_module, "_bucket", "dst-bucket"), \
                    patch.object(uploader_module, "_target_prefix", "backup"), \
                    patch.object(uploader_module, "_target_uri_scheme", "obs"), \
                    patch.object(uploader_module, "_target_endpoint_host", "obs.cn-south-1.myhuaweicloud.com"), \
                    patch.object(uploader_module, "_threshold", 1024 * 1024), \
                    patch.object(uploader_module, "_part_size", 1024 * 1024), \
                    patch.object(uploader_module, "_limiter", None), \
                    patch.object(uploader_module, "_source_client", object()), \
                    patch.object(uploader_module, "_source_bucket", "src-bucket"), \
                    patch.object(uploader_module, "_source_uri_scheme", "obs"), \
                    patch.object(uploader_module, "_source_endpoint_host", "obs.cn-south-1.myhuaweicloud.com"):
                uploader.upload(
                    {
                        "source_type": "s3",
                        "source_bucket": "src-bucket",
                        "source_key": "path/payload.bin",
                        "source_path": "s3://src-bucket/path/payload.bin",
                        "source_display": "obs://src-bucket/path/payload.bin",
                        "relative_path": "payload.bin",
                        "size": 7,
                        "mtime": 123.0,
                        "etag": "etag-a",
                    }
                )

            self.assertEqual(fake_target.head_calls, 0)
            self.assertEqual(
                fake_target.copy_calls,
                [("src-bucket", "path/payload.bin", "dst-bucket", "backup/payload.bin")],
            )

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["files_done"], 1)
            self.assertEqual(snapshot["done_bytes"], 7)

            checkpoint.close()

    # ================================
    # 验证大对象优先分片服务端拷贝
    # ================================
    def test_uploader_prefers_server_side_multipart_copy_for_large_s3_objects(self):
        # ================================
        # 模拟分片拷贝目标端客户端
        # ================================
        class FakeTargetClient:
            # ================================
            # 初始化分片调用记录
            # ================================
            def __init__(self):
                self.copy_part_calls = []

            # ================================
            # 模拟创建分片上传任务
            # ================================
            def initiateMultipartUpload(self, bucket, key):
                return SimpleNamespace(status=200, body=SimpleNamespace(uploadId="upload-1"))

            # ================================
            # 模拟执行分片拷贝
            # ================================
            def copyPart(self, bucket, key, part_number, upload_id, copy_source, copySourceRange=None):
                self.copy_part_calls.append(
                    (bucket, key, part_number, upload_id, copy_source, copySourceRange)
                )
                return SimpleNamespace(status=200, body=SimpleNamespace(etag=f"etag-{part_number}"))

            # ================================
            # 模拟完成分片上传
            # ================================
            def completeMultipartUpload(self, bucket, key, upload_id, request):
                return SimpleNamespace(status=200, body=SimpleNamespace(parts=request.parts))

            # ================================
            # 模拟中止分片上传
            # ================================
            def abortMultipartUpload(self, bucket, key, upload_id):
                return SimpleNamespace(status=204)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            progress = Progress()
            checkpoint = Checkpoint(str(root / "state" / "tasks.db"))
            checkpoint.set_index_ready()
            fake_target = FakeTargetClient()

            uploader = OBSUploader(
                progress,
                checkpoint,
                failed_dir=str(root / "failed"),
                retry_limit=2,
            )

            with patch.object(uploader_module, "_target_type", "s3"), \
                    patch.object(uploader_module, "_client", fake_target), \
                    patch.object(uploader_module, "_bucket", "dst-bucket"), \
                    patch.object(uploader_module, "_target_prefix", "backup"), \
                    patch.object(uploader_module, "_target_uri_scheme", "obs"), \
                    patch.object(uploader_module, "_target_endpoint_host", "obs.cn-south-1.myhuaweicloud.com"), \
                    patch.object(uploader_module, "_threshold", 5), \
                    patch.object(uploader_module, "_part_size", 4), \
                    patch.object(uploader_module, "_limiter", None), \
                    patch.object(uploader_module, "_source_client", object()), \
                    patch.object(uploader_module, "_source_bucket", "src-bucket"), \
                    patch.object(uploader_module, "_source_uri_scheme", "obs"), \
                    patch.object(uploader_module, "_source_endpoint_host", "obs.cn-south-1.myhuaweicloud.com"):
                uploader.upload(
                    {
                        "source_type": "s3",
                        "source_bucket": "src-bucket",
                        "source_key": "path/big.bin",
                        "source_path": "s3://src-bucket/path/big.bin",
                        "source_display": "obs://src-bucket/path/big.bin",
                        "relative_path": "big.bin",
                        "size": 10,
                        "mtime": 123.0,
                        "etag": "etag-a",
                    }
                )

            self.assertEqual(
                fake_target.copy_part_calls,
                [
                    ("dst-bucket", "backup/big.bin", 1, "upload-1", "/src-bucket/path/big.bin", "0-3"),
                    ("dst-bucket", "backup/big.bin", 2, "upload-1", "/src-bucket/path/big.bin", "4-7"),
                    ("dst-bucket", "backup/big.bin", 3, "upload-1", "/src-bucket/path/big.bin", "8-9"),
                ],
            )

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["files_done"], 1)
            self.assertEqual(snapshot["done_bytes"], 10)

            checkpoint.close()

    # ================================
    # 验证索引未命中时跳过 HEAD
    # ================================
    def test_uploader_skips_head_when_index_misses_target_key(self):
        # ================================
        # 模拟目标端上传客户端
        # ================================
        class FakeTargetClient:
            # ================================
            # 初始化目标端统计字段
            # ================================
            def __init__(self):
                self.head_calls = 0
                self.put_calls = 0

            # ================================
            # 模拟目标端 HEAD 请求
            # ================================
            def getObjectMetadata(self, bucket, key):
                self.head_calls += 1
                return SimpleNamespace(status=404, body=SimpleNamespace())

            # ================================
            # 模拟直接上传文件
            # ================================
            def putFile(self, bucket, key, local_path):
                self.put_calls += 1
                return SimpleNamespace(status=200)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_file = root / "payload.txt"
            local_file.write_text("payload", encoding="utf-8")

            progress = Progress()
            checkpoint = Checkpoint(str(root / "state" / "tasks.db"))
            checkpoint.set_index_ready()
            fake_target = FakeTargetClient()

            uploader = OBSUploader(
                progress,
                checkpoint,
                failed_dir=str(root / "failed"),
                retry_limit=2,
            )

            with patch.object(uploader_module, "_target_type", "s3"), \
                    patch.object(uploader_module, "_client", fake_target), \
                    patch.object(uploader_module, "_bucket", "dst-bucket"), \
                    patch.object(uploader_module, "_target_prefix", "backup"), \
                    patch.object(uploader_module, "_threshold", 1024 * 1024), \
                    patch.object(uploader_module, "_part_size", 1024 * 1024), \
                    patch.object(uploader_module, "_limiter", None):
                uploader.upload(
                    {
                        "source_type": "local",
                        "local": str(local_file),
                        "source_path": str(local_file),
                        "relative_path": "payload.txt",
                        "size": local_file.stat().st_size,
                    }
                )

            self.assertEqual(fake_target.head_calls, 0)
            self.assertEqual(fake_target.put_calls, 1)

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["files_done"], 1)
            self.assertEqual(snapshot["done_bytes"], len("payload"))

            checkpoint.close()

    # ================================
    # 验证本地到本地复制
    # ================================
    def test_uploader_copies_local_source_to_local_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_file = root / "src" / "payload.txt"
            source_file.parent.mkdir(parents=True, exist_ok=True)
            source_file.write_text("payload", encoding="utf-8")
            target_root = root / "dst"

            progress = Progress()
            checkpoint = Checkpoint(str(root / "state" / "tasks.db"))

            uploader = OBSUploader(
                progress,
                checkpoint,
                failed_dir=str(root / "failed"),
                retry_limit=2,
            )

            with patch.object(uploader_module, "_target_type", "local"), \
                    patch.object(uploader_module, "_target_root", str(target_root)), \
                    patch.object(uploader_module, "_target_prefix", ""), \
                    patch.object(uploader_module, "_limiter", None):
                uploader.upload(
                    {
                        "source_type": "local",
                        "local": str(source_file),
                        "source_path": str(source_file),
                        "relative_path": "nested/payload.txt",
                        "size": source_file.stat().st_size,
                    }
                )

            copied = target_root / "nested" / "payload.txt"
            self.assertTrue(copied.exists())
            self.assertEqual(copied.read_text(encoding="utf-8"), "payload")

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["files_done"], 1)
            self.assertEqual(snapshot["done_bytes"], len("payload"))

            checkpoint.close()

    # ================================
    # 验证远端对象下载到本地
    # ================================
    def test_uploader_downloads_s3_source_to_local_target(self):
        # ================================
        # 模拟源端对象下载客户端
        # ================================
        class FakeSourceClient:
            # ================================
            # 返回对象响应流
            # ================================
            def getObject(self, bucket, key, headers=None):
                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(response=io.BytesIO(b"payload")),
                )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_root = root / "dst"

            progress = Progress()
            checkpoint = Checkpoint(str(root / "state" / "tasks.db"))

            uploader = OBSUploader(
                progress,
                checkpoint,
                failed_dir=str(root / "failed"),
                retry_limit=2,
            )

            with patch.object(uploader_module, "_target_type", "local"), \
                    patch.object(uploader_module, "_target_root", str(target_root)), \
                    patch.object(uploader_module, "_target_prefix", ""), \
                    patch.object(uploader_module, "_limiter", None), \
                    patch.object(uploader_module, "_source_client", FakeSourceClient()), \
                    patch.object(uploader_module, "_source_bucket", "src-bucket"):
                uploader.upload(
                    {
                        "source_type": "s3",
                        "source_bucket": "src-bucket",
                        "source_key": "path/payload.bin",
                        "source_path": "s3://src-bucket/path/payload.bin",
                        "relative_path": "nested/payload.bin",
                        "size": 7,
                        "mtime": 123.0,
                        "etag": "etag-a",
                    }
                )

            downloaded = target_root / "nested" / "payload.bin"
            self.assertTrue(downloaded.exists())
            self.assertEqual(downloaded.read_bytes(), b"payload")

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["files_done"], 1)
            self.assertEqual(snapshot["done_bytes"], 7)

            checkpoint.close()


# ================================
# 测试命令行与仪表盘辅助逻辑
# ================================
class EntryUiTests(unittest.TestCase):
    """测试命令行环境处理与仪表盘相关辅助逻辑。"""

    # ================================
    # 验证模式输入归一化
    # ================================
    def test_mode_normalization_supports_choices(self):
        self.assertEqual(obs_migrate._normalize_mode("local"), "local")
        self.assertEqual(obs_migrate._normalize_mode("LOCAL"), "local")
        self.assertEqual(obs_migrate._normalize_mode("1"), "local")
        self.assertEqual(obs_migrate._normalize_mode("s3"), "s3")
        self.assertEqual(obs_migrate._normalize_mode("2"), "s3")
        self.assertEqual(obs_migrate._normalize_mode("", default="local"), "local")

    # ================================
    # 验证兼容旧版配置迁移
    # ================================
    def test_load_config_migrates_legacy_obs_and_task_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "config.ini"
            cfg_path.write_text(
                "\n".join(
                    [
                        "[OBS]",
                        "ak = legacy-ak",
                        "sk = legacy-sk",
                        "endpoint = legacy-endpoint",
                        "bucket = legacy-bucket",
                        "",
                        "[TASK]",
                        "local_dir = .",
                        "obs_prefix = backup",
                        "",
                        "[SOURCE]",
                        "type = 2",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            with patch.object(obs_migrate, "CONFIG_FILE", str(cfg_path)), \
                    patch.object(obs_migrate, "should_prompt_config", return_value=False):
                cfg = obs_migrate.load_config()

            self.assertTrue(cfg.has_section("TARGET"))
            self.assertFalse(cfg.has_section("OBS"))
            self.assertFalse(cfg.has_section("TASK"))
            self.assertEqual(cfg.get("TARGET", "bucket"), "legacy-bucket")
            self.assertEqual(cfg.get("TARGET", "endpoint"), "legacy-endpoint")
            self.assertEqual(cfg.get("TARGET", "prefix"), "backup")
            self.assertEqual(cfg.get("SOURCE", "path"), ".")
            self.assertEqual(cfg.get("SOURCE", "type"), "s3")

    # ================================
    # 验证默认界面开关
    # ================================
    def test_ui_defaults_are_enabled(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertTrue(obs_migrate.should_prompt_config())
            self.assertTrue(obs_migrate.should_enable_dashboard())
            self.assertEqual(obs_migrate.should_force_terminal(), os.name == "nt")

    # ================================
    # 验证环境变量覆盖界面开关
    # ================================
    def test_ui_env_overrides_are_honored(self):
        with patch.dict(
            "os.environ",
            {
                "OBS_MIGRATE_INTERACTIVE": "0",
                "OBS_MIGRATE_DASHBOARD": "0",
                "OBS_MIGRATE_FORCE_TERMINAL": "1",
            },
            clear=True,
        ):
            self.assertFalse(obs_migrate.should_prompt_config())
            self.assertFalse(obs_migrate.should_enable_dashboard())
            self.assertTrue(obs_migrate.should_force_terminal())

    # ================================
    # 验证本地扫描线程上限
    # ================================
    def test_scan_workers_are_capped_to_reasonable_range(self):
        with patch("os.cpu_count", return_value=8):
            self.assertEqual(obs_migrate.resolve_scan_workers(1), 1)
            self.assertEqual(obs_migrate.resolve_scan_workers(8), 8)
            self.assertEqual(obs_migrate.resolve_scan_workers(128), 32)

    # ================================
    # 验证远端扫描线程上限
    # ================================
    def test_remote_scan_workers_keep_higher_parallelism(self):
        self.assertEqual(obs_migrate.resolve_remote_scan_workers(1), 1)
        self.assertEqual(obs_migrate.resolve_remote_scan_workers(64), 64)
        self.assertEqual(obs_migrate.resolve_remote_scan_workers(256), 128)

    # ================================
    # 验证动态扫描最小线程下限
    # ================================
    def test_min_scan_workers_leave_small_adaptive_floor(self):
        self.assertEqual(obs_migrate.resolve_min_scan_workers(1), 1)
        self.assertEqual(obs_migrate.resolve_min_scan_workers(8), 1)
        self.assertEqual(obs_migrate.resolve_min_scan_workers(16), 2)
        self.assertEqual(obs_migrate.resolve_min_scan_workers(128), 4)

    # ================================
    # 验证支持直接输入配置编号
    # ================================
    def test_prompt_config_action_supports_direct_index(self):
        with patch("builtins.input", side_effect=["7"]):
            self.assertEqual(obs_migrate._prompt_config_action({"7": ("SOURCE", "prefix")}), "7")

    # ================================
    # 验证顶层配置菜单为折叠视图
    # ================================
    def test_show_config_menu_displays_collapsed_groups(self):
        cfg = configparser.ConfigParser()
        for section, items in obs_migrate.DEFAULT_CONFIG.items():
            cfg[section] = dict(items)

        cfg.set("SOURCE", "type", "s3")
        cfg.set("SOURCE", "bucket", "src-bucket")
        cfg.set("SOURCE", "prefix", "src-prefix")
        cfg.set("TARGET", "type", "local")
        cfg.set("TARGET", "path", "/data/target")

        with patch("sys.stdout", new=io.StringIO()) as buffer:
            mapping = obs_migrate.show_config_menu(cfg)

        output = buffer.getvalue()
        self.assertEqual(mapping["1"], "source")
        self.assertEqual(mapping["8"], "ui")
        self.assertIn("配置菜单", output)
        self.assertIn("[源端配置]", output)
        self.assertIn("[目标端配置]", output)
        self.assertIn("[调度器配置]", output)
        self.assertNotIn("源端对象存储 AccessKey", output)
        self.assertNotIn("目标端对象存储 Endpoint", output)

    # ================================
    # 验证配置展示会折叠当前模式无关项
    # ================================
    def test_show_config_collapses_inactive_mode_specific_options(self):
        cfg = configparser.ConfigParser()
        for section, items in obs_migrate.DEFAULT_CONFIG.items():
            cfg[section] = dict(items)

        cfg.set("SOURCE", "type", "s3")
        cfg.set("TARGET", "type", "local")

        with patch("sys.stdout", new=io.StringIO()) as buffer:
            mapping = obs_migrate.show_config(cfg)

        output = buffer.getvalue()
        mapped_items = set(mapping.values())

        self.assertIn("源端配置", output)
        self.assertIn("目标端配置", output)
        self.assertIn("当前模式：s3", output)
        self.assertIn("当前模式：local", output)
        self.assertIn(("SOURCE", "bucket"), mapped_items)
        self.assertIn(("TARGET", "path"), mapped_items)
        self.assertNotIn(("SOURCE", "path"), mapped_items)
        self.assertNotIn(("TARGET", "endpoint"), mapped_items)

    # ================================
    # 验证分组详情展开后只显示该组配置项
    # ================================
    def test_show_config_group_only_expands_selected_group(self):
        cfg = configparser.ConfigParser()
        for section, items in obs_migrate.DEFAULT_CONFIG.items():
            cfg[section] = dict(items)

        with patch("sys.stdout", new=io.StringIO()) as buffer:
            mapping = obs_migrate.show_config_group(cfg, "scanner")

        output = buffer.getvalue()
        mapped_items = set(mapping.values())

        self.assertIn("扫描器配置", output)
        self.assertIn(("SCAN", "scan_workers"), mapped_items)
        self.assertIn(("SCAN", "batch_size"), mapped_items)
        self.assertNotIn(("UPLOAD", "workers"), mapped_items)
        self.assertNotIn("源端对象存储 AccessKey", output)

    # ================================
    # 验证折叠菜单支持直接启动
    # ================================
    def test_run_config_menu_can_start_directly(self):
        cfg = configparser.ConfigParser()
        for section, items in obs_migrate.DEFAULT_CONFIG.items():
            cfg[section] = dict(items)

        with patch("builtins.input", side_effect=["y"]), \
                patch("sys.stdout", new=io.StringIO()):
            result = obs_migrate.run_config_menu(cfg)

        self.assertIs(result, cfg)

    # ================================
    # 验证根据端点识别存储协议
    # ================================
    def test_detect_storage_scheme_for_obs_endpoint(self):
        scheme = detect_storage_scheme("obs.cn-south-1.myhuaweicloud.com")
        self.assertEqual(scheme, "obs")
        self.assertEqual(
            build_object_uri("bucket", "path/file.txt", scheme=scheme),
            "obs://bucket/path/file.txt",
        )

    # ================================
    # 验证运行时路径跟随配置目录
    # ================================
    def test_runtime_paths_resolve_from_config_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_dir = Path(tmp) / "conf"
            cfg_dir.mkdir(parents=True, exist_ok=True)
            cfg_path = cfg_dir / "config.ini"

            with patch.object(obs_migrate, "CONFIG_FILE", str(cfg_path)):
                self.assertEqual(obs_migrate.resolve_config_file(), str(cfg_path))
                self.assertEqual(
                    obs_migrate.resolve_runtime_path("./state"),
                    str(cfg_dir / "state"),
                )
                self.assertEqual(
                    obs_migrate.resolve_runtime_path("logs"),
                    str(cfg_dir / "logs"),
                )

    # ================================
    # 验证环境变量覆盖配置路径
    # ================================
    def test_config_path_can_be_overridden_by_env_var(self):
        with tempfile.TemporaryDirectory() as tmp:
            external_cfg = Path(tmp) / "secure" / "config.ini"
            with patch.dict(os.environ, {"OBS_MIGRATE_CONFIG": str(external_cfg)}, clear=False):
                self.assertEqual(obs_migrate.resolve_config_file(), str(external_cfg))
                self.assertEqual(
                    obs_migrate.resolve_key_file(),
                    str(external_cfg.parent / ".config.key"),
                )

    # ================================
    # 验证仪表盘展示活跃扫描线程
    # ================================
    def test_dashboard_shows_active_scan_workers(self):
        progress = Progress()
        progress.scan_worker_started()
        progress.scan_worker_started()

        dashboard = Dashboard(
            progress,
            queue.Queue(maxsize=10),
            SimpleNamespace(get_active_workers=lambda: 0, threads=[object(), object()]),
            scan_workers=8,
            enabled=False,
            status_provider=lambda: {"index": "done", "scan": "running"},
            scan_controller=SimpleNamespace(get_desired_workers=lambda: 4),
        )

        table = dashboard.build_table()
        rows = dict(zip(table.columns[0]._cells, table.columns[1]._cells))

        self.assertEqual(str(rows["Scan Status"]), "running (2 active, target 4)")
        self.assertEqual(rows["Queue Size"], "0/10")
        self.assertEqual(rows["Scan Workers"], "4/8")

    # ================================
    # 验证仪表盘构建进度组件
    # ================================
    def test_dashboard_builds_rich_progress_renderable(self):
        progress = Progress()
        progress.record_scan_file(200)
        progress.add_done(100)

        dashboard = Dashboard(
            progress,
            queue.Queue(maxsize=10),
            SimpleNamespace(get_active_workers=lambda: 1, threads=[object()]),
            scan_workers=4,
            enabled=False,
            status_provider=lambda: {"index": "done", "scan": "running"},
        )

        renderable = dashboard.build_renderable()
        self.assertIsInstance(renderable, Panel)
        self.assertEqual(str(renderable.title), "[bold bright_cyan]OBS Migration Dashboard[/bold bright_cyan]")
        self.assertGreaterEqual(dashboard.progress_bar_column.bar_width, 20)
        self.assertFalse(renderable.expand)

        table = dashboard.build_table()
        rows = dict(zip(table.columns[0]._cells, table.columns[1]._cells))
        self.assertEqual(rows["Progress"], "0.0MB / 0.0MB")

    # ================================
    # 验证大容量场景进度条显示精度
    # ================================
    def test_dashboard_progress_bar_keeps_large_total_precision(self):
        progress = Progress()
        progress.record_scan_file(2 * 1024 ** 4)
        progress.add_done(5 * 1024 ** 3)

        dashboard = Dashboard(
            progress,
            queue.Queue(maxsize=10),
            SimpleNamespace(get_active_workers=lambda: 1, threads=[object()]),
            scan_workers=4,
            enabled=False,
            status_provider=lambda: {"index": "done", "scan": "running"},
        )

        with patch("core.dashboard.time.time", return_value=progress.start_time + 10):
            dashboard.build_progress_renderable()

        task = dashboard.progress_bar.tasks[dashboard.progress_task_id]
        self.assertEqual(task.fields["progress_pct"], "0.24%")
        self.assertEqual(task.fields["progress_detail"], "5.0GB/2.0TB")
        self.assertEqual(task.fields["speed_detail"], "512.0MB/s")
        self.assertTrue(task.fields["eta_detail"].startswith("01"))


if __name__ == "__main__":
    unittest.main()

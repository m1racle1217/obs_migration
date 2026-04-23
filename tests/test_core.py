import io
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
from core.checkpoint import Checkpoint
from core.obs_index import build_obs_index
from core.progress import Progress
from core.s3_scanner import scan_s3_objects
from core.scanner import scan_directory
from core.uploader import OBSUploader
from core.utils import build_object_uri, detect_storage_scheme


class MemoryReporter:

    def __init__(self):
        self.rows = []

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


class CheckpointTests(unittest.TestCase):

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


class ObsIndexTests(unittest.TestCase):

    def test_build_obs_index_rebuilds_and_marks_ready(self):
        class FakeObsClient:
            def __init__(self, *args, **kwargs):
                self.calls = 0

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


class ScannerTests(unittest.TestCase):

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

    def test_scan_s3_objects_enqueues_remote_objects(self):
        class FakeSourceClient:
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


class UploaderTests(unittest.TestCase):

    def test_uploader_uses_configured_retry_limit_for_s3_target(self):
        class FakeTargetClient:
            def __init__(self):
                self.head_calls = 0
                self.put_calls = 0

            def getObjectMetadata(self, bucket, key):
                self.head_calls += 1
                return SimpleNamespace(status=404, body=SimpleNamespace())

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

    def test_uploader_streams_s3_source_for_small_s3_target_objects(self):
        class FakeTargetClient:
            def __init__(self):
                self.head_calls = 0
                self.put_content_calls = []

            def getObjectMetadata(self, bucket, key):
                self.head_calls += 1
                return SimpleNamespace(status=404, body=SimpleNamespace())

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

        class FakeSourceClient:
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

    def test_uploader_prefers_server_side_copy_for_small_s3_objects(self):
        class FakeTargetClient:
            def __init__(self):
                self.head_calls = 0
                self.copy_calls = []

            def copyObject(self, source_bucket, source_key, dest_bucket, dest_key):
                self.copy_calls.append((source_bucket, source_key, dest_bucket, dest_key))
                return SimpleNamespace(status=200)

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

    def test_uploader_prefers_server_side_multipart_copy_for_large_s3_objects(self):
        class FakeTargetClient:
            def __init__(self):
                self.copy_part_calls = []

            def initiateMultipartUpload(self, bucket, key):
                return SimpleNamespace(status=200, body=SimpleNamespace(uploadId="upload-1"))

            def copyPart(self, bucket, key, part_number, upload_id, copy_source, copySourceRange=None):
                self.copy_part_calls.append(
                    (bucket, key, part_number, upload_id, copy_source, copySourceRange)
                )
                return SimpleNamespace(status=200, body=SimpleNamespace(etag=f"etag-{part_number}"))

            def completeMultipartUpload(self, bucket, key, upload_id, request):
                return SimpleNamespace(status=200, body=SimpleNamespace(parts=request.parts))

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

    def test_uploader_skips_head_when_index_misses_target_key(self):
        class FakeTargetClient:
            def __init__(self):
                self.head_calls = 0
                self.put_calls = 0

            def getObjectMetadata(self, bucket, key):
                self.head_calls += 1
                return SimpleNamespace(status=404, body=SimpleNamespace())

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

    def test_uploader_downloads_s3_source_to_local_target(self):
        class FakeSourceClient:
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


class EntryUiTests(unittest.TestCase):

    def test_mode_normalization_supports_choices(self):
        self.assertEqual(obs_migrate._normalize_mode("local"), "local")
        self.assertEqual(obs_migrate._normalize_mode("LOCAL"), "local")
        self.assertEqual(obs_migrate._normalize_mode("1"), "local")
        self.assertEqual(obs_migrate._normalize_mode("s3"), "s3")
        self.assertEqual(obs_migrate._normalize_mode("2"), "s3")
        self.assertEqual(obs_migrate._normalize_mode("", default="local"), "local")

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

    def test_ui_defaults_are_enabled(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertTrue(obs_migrate.should_prompt_config())
            self.assertTrue(obs_migrate.should_enable_dashboard())
            self.assertEqual(obs_migrate.should_force_terminal(), os.name == "nt")

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

    def test_scan_workers_are_capped_to_reasonable_range(self):
        with patch("os.cpu_count", return_value=8):
            self.assertEqual(obs_migrate.resolve_scan_workers(1), 1)
            self.assertEqual(obs_migrate.resolve_scan_workers(8), 8)
            self.assertEqual(obs_migrate.resolve_scan_workers(128), 16)

    def test_remote_scan_workers_keep_higher_parallelism(self):
        self.assertEqual(obs_migrate.resolve_remote_scan_workers(1), 1)
        self.assertEqual(obs_migrate.resolve_remote_scan_workers(64), 64)
        self.assertEqual(obs_migrate.resolve_remote_scan_workers(256), 128)

    def test_prompt_config_action_supports_direct_index(self):
        with patch("builtins.input", side_effect=["7"]):
            self.assertEqual(obs_migrate._prompt_config_action({"7": ("SOURCE", "prefix")}), "7")

    def test_detect_storage_scheme_for_obs_endpoint(self):
        scheme = detect_storage_scheme("obs.cn-south-1.myhuaweicloud.com")
        self.assertEqual(scheme, "obs")
        self.assertEqual(
            build_object_uri("bucket", "path/file.txt", scheme=scheme),
            "obs://bucket/path/file.txt",
        )


if __name__ == "__main__":
    unittest.main()

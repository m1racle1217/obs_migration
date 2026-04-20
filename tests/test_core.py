import os
import queue
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import obs_migrate
from core.checkpoint import Checkpoint
from core.obs_index import build_obs_index
from core.progress import Progress
from core.scanner import scan_directory
from core.uploader import OBSUploader
import core.uploader as uploader_module


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
                "backup",
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
                sorted(task["obs"] for task in tasks),
                ["backup/file.txt", "backup/sub/nested.bin"],
            )

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


class UploaderTests(unittest.TestCase):

    def test_uploader_uses_configured_retry_limit(self):
        class FakeObsClient:
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
            fake_client = FakeObsClient()

            uploader = OBSUploader(
                progress,
                checkpoint,
                failed_dir=str(failed_dir),
                retry_limit=2,
            )

            with patch.object(uploader_module, "_client", fake_client), \
                    patch.object(uploader_module, "_bucket", "bucket"), \
                    patch.object(uploader_module, "_threshold", 1024 * 1024), \
                    patch.object(uploader_module, "_part_size", 1024 * 1024), \
                    patch.object(uploader_module, "_limiter", None), \
                    patch("core.uploader.time.sleep", return_value=None):
                uploader.upload(
                    {
                        "local": str(local_file),
                        "obs": "backup/payload.txt",
                        "size": local_file.stat().st_size,
                    }
                )

            self.assertEqual(fake_client.head_calls, 1)
            self.assertEqual(fake_client.put_calls, 2)

            snapshot = progress.snapshot()
            self.assertEqual(snapshot["upload_errors"], 2)

            failed_log = failed_dir / "failed.txt"
            self.assertTrue(failed_log.exists())
            self.assertIn(str(local_file), failed_log.read_text(encoding="utf-8"))

            checkpoint.close()


class EntryUiTests(unittest.TestCase):

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


if __name__ == "__main__":
    unittest.main()

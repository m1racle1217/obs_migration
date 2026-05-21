import queue
import json
import threading
import time
import unittest
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest.mock import patch

from core.progress import Progress
from core.scheduler import Scheduler
from core.scanner import scan_directory
import core.s3_scanner as s3_scanner_module
from core.s3_scanner import scan_s3_sources
from core.task_manager import MultiTaskManager, TaskControls, TaskManager
from core.uploader import TaskChecker


def wait_until(predicate, timeout=1.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


class TaskManagerTests(unittest.TestCase):
    def test_immediate_pause_during_start_is_not_overwritten_by_running(self):
        release = threading.Event()

        def runner(cfg, controls):
            release.wait(timeout=1)

        manager = TaskManager(runner)
        original_mark_started = manager._mark_started

        def pause_before_mark_started(controls):
            manager.pause()
            original_mark_started(controls)

        manager._mark_started = pause_before_mark_started
        self.addCleanup(lambda: release.set())
        self.addCleanup(lambda: manager.resume())
        self.addCleanup(lambda: manager.join(timeout=1))

        self.assertTrue(manager.start({}))
        self.assertTrue(wait_until(lambda: manager.controls is not None and manager.controls.pause_requested()))

        snapshot = manager.snapshot()
        self.assertTrue(manager.controls.pause_requested())
        self.assertIn(snapshot["state"], {"pausing", "paused"})

    def test_task_completes_and_exposes_status_snapshot(self):
        def runner(cfg, controls):
            controls.update_status(
                progress={"files_done": 1},
                pipeline={"scan": "done"},
                workers={"upload": {"active_workers": 0}},
                logs={"log_file": "logs/task.log"},
            )

        manager = TaskManager(runner)

        self.assertTrue(manager.start({"source": "unit"}))
        self.assertTrue(manager.join(timeout=1))

        snapshot = manager.snapshot()
        self.assertEqual(snapshot["state"], "completed")
        self.assertEqual(snapshot["progress"], {"files_done": 1})
        self.assertEqual(snapshot["pipeline"], {"scan": "done"})
        self.assertEqual(snapshot["workers"], {"upload": {"active_workers": 0}})
        self.assertEqual(snapshot["logs"], {"log_file": "logs/task.log"})
        self.assertIsNone(snapshot["error"])
        self.assertIsNotNone(snapshot["timestamps"]["started_at"])
        self.assertIsNotNone(snapshot["timestamps"]["finished_at"])

    def test_prevents_concurrent_start(self):
        release = threading.Event()

        def runner(cfg, controls):
            release.wait(timeout=1)

        manager = TaskManager(runner)

        self.assertTrue(manager.start({"run": 1}))
        self.assertFalse(manager.start({"run": 2}))
        release.set()
        self.assertTrue(manager.join(timeout=1))
        self.assertEqual(manager.snapshot()["state"], "completed")

    def test_pause_resume_stop_are_idempotent(self):
        started = threading.Event()

        def runner(cfg, controls):
            started.set()
            while not controls.stop_requested():
                controls.wait_if_paused(poll_interval=0.01)
                time.sleep(0.01)

        manager = TaskManager(runner)

        self.assertTrue(manager.start({}))
        self.assertTrue(started.wait(timeout=1))

        manager.pause()
        manager.pause()
        self.assertEqual(manager.snapshot()["state"], "paused")
        self.assertTrue(manager.controls.pause_requested())

        manager.resume()
        manager.resume()
        self.assertEqual(manager.snapshot()["state"], "running")
        self.assertFalse(manager.controls.pause_requested())

        manager.stop()
        manager.stop()
        self.assertTrue(manager.controls.stop_requested())
        self.assertTrue(manager.join(timeout=1))
        self.assertEqual(manager.snapshot()["state"], "stopped")

    def test_failure_state_captures_error(self):
        def runner(cfg, controls):
            raise RuntimeError("boom")

        manager = TaskManager(runner)

        self.assertTrue(manager.start({}))
        self.assertTrue(manager.join(timeout=1))

        snapshot = manager.snapshot()
        self.assertEqual(snapshot["state"], "failed")
        self.assertIn("boom", snapshot["error"])


class MultiTaskManagerTests(unittest.TestCase):
    def test_persists_tasks_and_restores_them_after_restart(self):
        with TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir, "web_tasks.json")
            manager = MultiTaskManager(lambda _cfg, _controls: None, persistence_path=store_path)

            task_id = manager.create_task(make_test_config("persisted"), name="Persisted Task")
            manager.update_concurrency(task_id, {"upload_workers": 9})

            self.assertTrue(store_path.exists())
            raw = json.loads(store_path.read_text(encoding="utf-8"))
            self.assertEqual(raw["tasks"][0]["task_id"], task_id)

            restored = MultiTaskManager(lambda _cfg, _controls: None, persistence_path=store_path)
            tasks = restored.list_tasks()

            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0]["task_id"], task_id)
            self.assertEqual(tasks[0]["name"], "Persisted Task")
            self.assertEqual(tasks[0]["state"], "idle")
            self.assertEqual(tasks[0]["concurrency"]["upload_workers"], 9)
            cfg = restored.get_task_config(task_id)
            self.assertEqual(cfg.get("TEST", "name"), "persisted")

    def test_restores_running_task_as_stopped_with_last_snapshot(self):
        with TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir, "web_tasks.json")
            release = threading.Event()
            started = threading.Event()

            def runner(_cfg, controls):
                controls.update_status(
                    progress={"files_done": 3},
                    logs={"log_file": "logs/task.log", "report_file": "check_report/task.csv"},
                )
                started.set()
                release.wait(timeout=2)

            manager = MultiTaskManager(runner, persistence_path=store_path)
            task_id = manager.create_task(make_test_config("running"), name="Running Task")
            self.assertTrue(manager.start(task_id))
            self.assertTrue(started.wait(timeout=1))

            restored = MultiTaskManager(lambda _cfg, _controls: None, persistence_path=store_path)
            snapshot = restored.snapshot(task_id)

            self.assertEqual(snapshot["state"], "stopped")
            self.assertEqual(snapshot["progress"], {"files_done": 3})
            self.assertEqual(snapshot["logs"]["log_file"], "logs/task.log")
            self.assertEqual(snapshot["logs"]["report_file"], "check_report/task.csv")
            self.assertIsNotNone(snapshot["timestamps"]["started_at"])
            self.assertIsNotNone(snapshot["timestamps"]["finished_at"])

            release.set()
            self.assertTrue(manager.join(task_id, timeout=1))

    def test_delete_task_updates_persistence_file(self):
        with TemporaryDirectory() as tmpdir:
            store_path = Path(tmpdir, "web_tasks.json")
            manager = MultiTaskManager(lambda _cfg, _controls: None, persistence_path=store_path)
            task_a = manager.create_task(make_test_config("a"), name="Task A")
            task_b = manager.create_task(make_test_config("b"), name="Task B")

            manager.delete_task(task_a)

            restored = MultiTaskManager(lambda _cfg, _controls: None, persistence_path=store_path)
            self.assertEqual([task["task_id"] for task in restored.list_tasks()], [task_b])
            with self.assertRaises(KeyError):
                restored.get_task_config(task_a)

    def test_creates_multiple_independent_tasks_and_runs_them_in_parallel(self):
        releases = {}
        started = {}

        def runner(cfg, controls):
            task_name = cfg.get("TEST", "name")
            started[task_name].set()
            controls.update_status(progress={"files_done": int(cfg.get("TEST", "done"))})
            releases[task_name].wait(timeout=2)

        manager = MultiTaskManager(runner)
        self.addCleanup(lambda: manager.stop_all())

        cfg_a = make_test_config("a", done=1)
        cfg_b = make_test_config("b", done=2)
        releases["a"] = threading.Event()
        releases["b"] = threading.Event()
        started["a"] = threading.Event()
        started["b"] = threading.Event()

        task_a = manager.create_task(cfg_a, name="Task A")
        task_b = manager.create_task(cfg_b, name="Task B")

        self.assertNotEqual(task_a, task_b)
        self.assertTrue(manager.start(task_a))
        self.assertTrue(manager.start(task_b))
        self.assertTrue(started["a"].wait(timeout=1))
        self.assertTrue(started["b"].wait(timeout=1))

        tasks = manager.list_tasks()
        self.assertEqual({task["task_id"] for task in tasks}, {task_a, task_b})
        self.assertEqual(manager.snapshot(task_a)["state"], "running")
        self.assertEqual(manager.snapshot(task_b)["state"], "running")
        self.assertEqual(manager.snapshot(task_a)["progress"], {"files_done": 1})
        self.assertEqual(manager.snapshot(task_b)["progress"], {"files_done": 2})

        releases["a"].set()
        releases["b"].set()
        self.assertTrue(manager.join(task_a, timeout=1))
        self.assertTrue(manager.join(task_b, timeout=1))

    def test_controls_only_target_task(self):
        release = threading.Event()
        started = threading.Event()

        def runner(_cfg, controls):
            started.set()
            while not controls.stop_requested():
                controls.wait_if_paused(poll_interval=0.01)
                time.sleep(0.01)
            release.set()

        manager = MultiTaskManager(runner)
        self.addCleanup(lambda: manager.stop_all())
        task_a = manager.create_task(make_test_config("a"), name="Task A")
        task_b = manager.create_task(make_test_config("b"), name="Task B")

        self.assertTrue(manager.start(task_a))
        self.assertTrue(started.wait(timeout=1))
        self.assertTrue(manager.pause(task_a))

        self.assertEqual(manager.snapshot(task_a)["state"], "paused")
        self.assertEqual(manager.snapshot(task_b)["state"], "idle")

        self.assertTrue(manager.stop(task_a))
        self.assertTrue(release.wait(timeout=1))
        self.assertTrue(manager.join(task_a, timeout=1))

    def test_updates_concurrency_in_task_config_and_controls(self):
        manager = MultiTaskManager(lambda _cfg, _controls: None)
        task_id = manager.create_task(make_test_config("a"), name="Task A")

        snapshot = manager.update_concurrency(
            task_id,
            {
                "upload_workers": 9,
                "check_workers": 4,
                "scan_workers": 3,
                "multipart_concurrency": 2,
                "max_connections": 80,
            },
        )

        self.assertEqual(snapshot["concurrency"]["upload_workers"], 9)
        self.assertEqual(snapshot["concurrency"]["check_workers"], 4)
        self.assertEqual(snapshot["concurrency"]["scan_workers"], 3)
        self.assertEqual(snapshot["concurrency"]["multipart_concurrency"], 2)
        self.assertEqual(snapshot["concurrency"]["max_connections"], 80)
        cfg = manager.get_task_config(task_id)
        self.assertEqual(cfg.get("UPLOAD", "workers"), "9")
        self.assertEqual(cfg.get("UPLOAD", "checkers"), "4")
        self.assertEqual(cfg.get("SCAN", "scan_workers"), "3")
        self.assertEqual(cfg.get("UPLOAD", "multipart_concurrency"), "2")
        self.assertEqual(cfg.get("UPLOAD", "max_connections"), "80")

    def test_checker_enqueue_unblocks_when_stop_requested(self):
        controls = TaskControls()
        transfer_queue = queue.Queue(maxsize=1)
        transfer_queue.put({"occupied": True})
        process_returned = threading.Event()

        class FakeUploader:
            def check_task(self, task, heartbeat=None, worker_name=None):
                return dict(task)

        checker = TaskChecker(FakeUploader(), transfer_queue, controls=controls)
        thread = threading.Thread(target=lambda: (checker.process({"source": "a"}), process_returned.set()))
        thread.start()
        self.addCleanup(lambda: controls.stop_event.set())
        self.addCleanup(lambda: thread.join(timeout=1))

        time.sleep(0.1)
        self.assertFalse(process_returned.is_set())
        controls.stop_event.set()

        self.assertTrue(process_returned.wait(timeout=1))
        self.assertTrue(transfer_queue.full())


def make_test_config(name, done=0):
    import configparser

    cfg = configparser.ConfigParser()
    cfg.add_section("TEST")
    cfg.set("TEST", "name", name)
    cfg.set("TEST", "done", str(done))
    cfg.add_section("UPLOAD")
    cfg.set("UPLOAD", "workers", "1")
    cfg.set("UPLOAD", "checkers", "1")
    cfg.set("UPLOAD", "multipart_concurrency", "1")
    cfg.set("UPLOAD", "max_connections", "16")
    cfg.add_section("SCAN")
    cfg.set("SCAN", "scan_workers", "1")
    return cfg


class SchedulerControlsTests(unittest.TestCase):
    def test_scheduler_resize_adds_workers_for_future_tasks(self):
        processed = []
        release = threading.Event()
        task_queue = queue.Queue()
        task_queue.put({"source_path": "a.txt"})
        task_queue.put({"source_path": "b.txt"})

        class Handler:
            def process(self, task, heartbeat=None, worker_name=None):
                processed.append(worker_name)
                release.wait(timeout=1)

        scheduler = Scheduler(task_queue, Handler(), workers=1, stage_name="upload")
        scheduler.start()
        self.assertTrue(wait_until(lambda: len(processed) == 1))

        scheduler.resize(2)

        self.assertTrue(wait_until(lambda: len(processed) == 2))
        self.assertGreaterEqual(len(set(processed)), 2)
        release.set()
        scheduler.stop()

    def test_scheduler_resize_down_exits_extra_worker_after_current_task(self):
        processed = []
        release = threading.Event()
        task_queue = queue.Queue()
        task_queue.put({"source_path": "a.txt"})
        task_queue.put({"source_path": "b.txt"})

        class Handler:
            def process(self, task, heartbeat=None, worker_name=None):
                processed.append(worker_name)
                release.wait(timeout=1)

        scheduler = Scheduler(task_queue, Handler(), workers=2, stage_name="upload")
        scheduler.start()
        self.assertTrue(wait_until(lambda: len(processed) == 2))

        scheduler.resize(1)
        release.set()
        self.assertTrue(wait_until(lambda: len([thread for thread in scheduler.threads if thread.is_alive()]) <= 1))
        scheduler.stop()

    def test_scheduler_balances_claimed_task_when_stop_arrives_before_dispatch(self):
        processed = []
        controls = TaskControls()

        class StopAfterGetQueue(queue.Queue):
            def get(self, *args, **kwargs):
                task = super().get(*args, **kwargs)
                controls.stop_event.set()
                return task

        task_queue = StopAfterGetQueue()
        task_queue.put({"source_path": "a.txt"})

        class Handler:
            def process(self, task, heartbeat=None, worker_name=None):
                processed.append(task)

        scheduler = Scheduler(
            task_queue,
            Handler(),
            workers=1,
            stage_name="check",
            controls=controls,
        )

        scheduler.start()
        self.assertTrue(wait_until(lambda: controls.stop_requested()))
        scheduler.stop()

        self.assertEqual(processed, [])
        self.assertEqual(task_queue.unfinished_tasks, 0)
        self.assertTrue(task_queue.empty())

    def test_scheduler_stop_does_not_hang_when_requeue_slot_is_refilled(self):
        processed = []
        controls = TaskControls()

        class RefillAfterGetQueue(queue.Queue):
            def get(self, *args, **kwargs):
                task = super().get(*args, **kwargs)
                super().put({"source_path": "filler.txt"}, block=False)
                controls.stop_event.set()
                return task

        task_queue = RefillAfterGetQueue(maxsize=1)
        task_queue.put({"source_path": "claimed.txt"})

        class Handler:
            def process(self, task, heartbeat=None, worker_name=None):
                processed.append(task)

        scheduler = Scheduler(
            task_queue,
            Handler(),
            workers=1,
            stage_name="check",
            controls=controls,
        )

        scheduler.start()
        self.assertTrue(wait_until(lambda: controls.stop_requested()))

        stop_thread = threading.Thread(target=scheduler.stop, daemon=True)
        stop_thread.start()
        stop_thread.join(timeout=0.5)

        self.assertFalse(stop_thread.is_alive())
        self.assertEqual(processed, [])
        self.assertEqual(task_queue.unfinished_tasks, task_queue.qsize())

    def test_scheduler_resume_dispatches_claimed_task_when_pause_slot_is_refilled(self):
        processed = []
        controls = TaskControls()

        class PauseAndRefillAfterGetQueue(queue.Queue):
            def get(self, *args, **kwargs):
                task = super().get(*args, **kwargs)
                super().put({"source_path": "filler.txt"}, block=False)
                controls.pause_event.set()
                return task

        task_queue = PauseAndRefillAfterGetQueue(maxsize=1)
        claimed_task = {"source_path": "claimed.txt"}
        task_queue.put(claimed_task)

        class Handler:
            def process(self, task, heartbeat=None, worker_name=None):
                processed.append(task)

        scheduler = Scheduler(
            task_queue,
            Handler(),
            workers=1,
            stage_name="check",
            controls=controls,
        )

        scheduler.start()
        self.assertTrue(wait_until(lambda: controls.pause_requested()))
        time.sleep(0.2)
        self.assertEqual(processed, [])

        controls.pause_event.clear()
        self.assertTrue(wait_until(lambda: processed == [claimed_task]))
        scheduler.stop()

        self.assertEqual(processed, [claimed_task])
        self.assertEqual(task_queue.qsize(), 1)
        self.assertEqual(task_queue.unfinished_tasks, 1)

    def test_scheduler_waits_while_paused_before_claiming_task(self):
        processed = []
        task_queue = queue.Queue()
        task_queue.put({"source_path": "a.txt"})
        controls = TaskControls()
        controls.pause_event.set()

        class Handler:
            def process(self, task, heartbeat=None, worker_name=None):
                processed.append(task)

        scheduler = Scheduler(
            task_queue,
            Handler(),
            workers=1,
            stage_name="check",
            controls=controls,
        )

        scheduler.start()
        time.sleep(0.2)
        self.assertEqual(processed, [])
        self.assertEqual(task_queue.unfinished_tasks, 1)

        controls.pause_event.clear()
        self.assertTrue(wait_until(lambda: len(processed) == 1))
        scheduler.stop()
        self.assertEqual(processed, [{"source_path": "a.txt"}])

    def test_scheduler_exits_when_stopped_before_claiming_task(self):
        processed = []
        task_queue = queue.Queue()
        task_queue.put({"source_path": "a.txt"})
        controls = TaskControls()
        controls.stop_event.set()

        class Handler:
            def process(self, task, heartbeat=None, worker_name=None):
                processed.append(task)

        scheduler = Scheduler(
            task_queue,
            Handler(),
            workers=1,
            stage_name="check",
            controls=controls,
        )

        scheduler.start()
        time.sleep(0.2)
        scheduler.stop()

        self.assertEqual(processed, [])
        self.assertEqual(task_queue.unfinished_tasks, 1)

    def test_scheduler_discards_pending_tasks_for_immediate_stop_snapshot(self):
        task_queue = queue.Queue()
        for index in range(5):
            task_queue.put({"source_path": f"{index}.txt"})

        scheduler = Scheduler(
            task_queue,
            handler=SimpleNamespace(process=lambda task, heartbeat=None, worker_name=None: None),
            workers=1,
            stage_name="upload",
        )

        discarded = scheduler.discard_pending_tasks()

        self.assertEqual(discarded, 5)
        self.assertEqual(task_queue.qsize(), 0)
        self.assertEqual(task_queue.unfinished_tasks, 0)


class ScannerControlsTests(unittest.TestCase):
    def test_paused_local_scanner_does_not_enqueue_until_resumed(self):
        controls = TaskControls()
        controls.pause_event.set()
        task_queue = queue.Queue()
        progress = Progress()

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for index in range(3):
                (root / f"file-{index}.txt").write_text("x", encoding="utf-8")

            thread = threading.Thread(
                target=scan_directory,
                args=(str(root), task_queue, progress, None),
                kwargs={"scan_workers": 1, "controls": controls},
                daemon=True,
            )
            thread.start()
            time.sleep(0.2)

            self.assertEqual(task_queue.qsize(), 0)
            self.assertEqual(progress.snapshot()["scan_active_workers"], 0)
            self.assertTrue(thread.is_alive())

            controls.pause_event.clear()
            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(task_queue.qsize(), 3)

    def test_local_scanner_requeues_when_pause_arrives_after_claim(self):
        controls = TaskControls()
        task_queue = queue.Queue()
        progress = Progress()

        class PauseAfterGetQueue(queue.Queue):
            paused_once = False

            def get(self, *args, **kwargs):
                item = super().get(*args, **kwargs)
                if isinstance(item, tuple) and item[0] == "dir" and not self.paused_once:
                    self.paused_once = True
                    controls.pause_event.set()
                return item

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "file.txt").write_text("x", encoding="utf-8")

            with patch("core.scanner.queue.Queue", PauseAfterGetQueue):
                thread = threading.Thread(
                    target=scan_directory,
                    args=(str(root), task_queue, progress, None),
                    kwargs={"scan_workers": 1, "controls": controls},
                    daemon=True,
                )
                thread.start()
                self.assertTrue(wait_until(lambda: controls.pause_requested()))
                time.sleep(0.2)

                self.assertEqual(task_queue.qsize(), 0)
                self.assertEqual(progress.snapshot()["scan_active_workers"], 0)
                self.assertTrue(thread.is_alive())

                controls.pause_event.clear()
                thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(task_queue.qsize(), 1)

    def test_paused_s3_scanner_does_not_enqueue_until_resumed(self):
        controls = TaskControls()
        controls.pause_event.set()
        task_queue = queue.Queue()
        progress = Progress()

        class FakeClient:
            def listObjects(self, bucket, delimiter="/", prefix="", marker=None, max_keys=1000):
                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(
                        commonPrefixes=[],
                        contents=[
                            SimpleNamespace(key="root/a.txt", size=1, lastModified=None, etag="a"),
                            SimpleNamespace(key="root/b.txt", size=1, lastModified=None, etag="b"),
                        ],
                        is_truncated=False,
                        next_marker=None,
                    ),
                )

        thread = threading.Thread(
            target=scan_s3_sources,
            args=(
                [{"bucket": "bucket", "prefix": "root"}],
                FakeClient(),
                "bucket",
                task_queue,
                progress,
            ),
            kwargs={"scan_workers": 1, "controls": controls},
            daemon=True,
        )
        thread.start()
        time.sleep(0.2)

        self.assertEqual(task_queue.qsize(), 0)
        self.assertEqual(progress.snapshot()["scan_active_workers"], 0)
        self.assertTrue(thread.is_alive())

        controls.pause_event.clear()
        thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(task_queue.qsize(), 2)

    def test_s3_scanner_does_not_claim_next_prefix_while_paused(self):
        controls = TaskControls()
        task_queue = queue.Queue()
        progress = Progress()

        class PauseAfterChildPrefixQueue(queue.Queue):
            def put(self, item, *args, **kwargs):
                result = super().put(item, *args, **kwargs)
                if item == "root/child/":
                    controls.pause_event.set()
                return result

        class FakeClient:
            def listObjects(self, bucket, delimiter="/", prefix="", marker=None, max_keys=1000):
                if prefix == "root":
                    return SimpleNamespace(
                        status=200,
                        body=SimpleNamespace(
                            commonPrefixes=[SimpleNamespace(prefix="root/child/")],
                            contents=[],
                            is_truncated=False,
                            next_marker=None,
                        ),
                    )
                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(
                        commonPrefixes=[],
                        contents=[SimpleNamespace(key="root/child/a.txt", size=1, lastModified=None, etag="a")],
                        is_truncated=False,
                        next_marker=None,
                    ),
                )

        with patch.object(s3_scanner_module.queue, "Queue", PauseAfterChildPrefixQueue):
            thread = threading.Thread(
                target=scan_s3_sources,
                args=(
                    [{"bucket": "bucket", "prefix": "root"}],
                    FakeClient(),
                    "bucket",
                    task_queue,
                    progress,
                ),
                kwargs={"scan_workers": 1, "controls": controls},
                daemon=True,
            )
            thread.start()
            self.assertTrue(wait_until(lambda: controls.pause_requested()))
            time.sleep(0.2)

            self.assertEqual(task_queue.qsize(), 0)
            self.assertEqual(progress.snapshot()["scan_active_workers"], 0)
            self.assertTrue(thread.is_alive())

            controls.pause_event.clear()
            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(task_queue.qsize(), 1)

    def test_s3_scanner_requeues_when_pause_arrives_after_claim(self):
        controls = TaskControls()
        task_queue = queue.Queue()
        progress = Progress()

        class PauseAfterGetQueue(queue.Queue):
            paused_once = False

            def get(self, *args, **kwargs):
                prefix = super().get(*args, **kwargs)
                if prefix == "root" and not self.paused_once:
                    self.paused_once = True
                    controls.pause_event.set()
                return prefix

        class FakeClient:
            def listObjects(self, bucket, delimiter="/", prefix="", marker=None, max_keys=1000):
                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(
                        commonPrefixes=[],
                        contents=[SimpleNamespace(key="root/a.txt", size=1, lastModified=None, etag="a")],
                        is_truncated=False,
                        next_marker=None,
                    ),
                )

        with patch.object(s3_scanner_module.queue, "Queue", PauseAfterGetQueue):
            thread = threading.Thread(
                target=scan_s3_sources,
                args=(
                    [{"bucket": "bucket", "prefix": "root"}],
                    FakeClient(),
                    "bucket",
                    task_queue,
                    progress,
                ),
                kwargs={"scan_workers": 1, "controls": controls},
                daemon=True,
            )
            thread.start()
            self.assertTrue(wait_until(lambda: controls.pause_requested()))
            time.sleep(0.2)

            self.assertEqual(task_queue.qsize(), 0)
            self.assertEqual(progress.snapshot()["scan_active_workers"], 0)
            self.assertTrue(thread.is_alive())

            controls.pause_event.clear()
            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(task_queue.qsize(), 1)

    def test_local_scanner_stops_before_enqueueing_further_tasks(self):
        controls = TaskControls()
        task_queue = queue.Queue()

        class StopAfterFirstPutQueue(queue.Queue):
            def put(self, item, *args, **kwargs):
                result = super().put(item, *args, **kwargs)
                controls.stop_event.set()
                return result

        task_queue = StopAfterFirstPutQueue()
        progress = Progress()

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for index in range(5):
                (root / f"file-{index}.txt").write_text("x", encoding="utf-8")

            scan_directory(
                str(root),
                task_queue,
                progress,
                checkpoint=None,
                scan_workers=1,
                controls=controls,
            )

        self.assertEqual(task_queue.qsize(), 1)

    def test_local_scanner_exits_when_stopped_while_output_queue_full(self):
        controls = TaskControls()
        task_queue = queue.Queue(maxsize=1)
        task_queue.put({"source_path": "prefill.txt"})
        progress = Progress()

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "file.txt").write_text("x", encoding="utf-8")

            thread = threading.Thread(
                target=scan_directory,
                args=(str(root), task_queue, progress, None),
                kwargs={"scan_workers": 1, "controls": controls},
                daemon=True,
            )
            thread.start()
            time.sleep(0.2)
            controls.stop_event.set()
            thread.join(timeout=0.5)
            exited_before_drain = not thread.is_alive()

            if thread.is_alive():
                task_queue.get_nowait()
                task_queue.task_done()
                thread.join(timeout=1)

        self.assertTrue(exited_before_drain)

    def test_s3_scanner_stops_before_enqueueing_further_tasks(self):
        controls = TaskControls()

        class StopAfterFirstPutQueue(queue.Queue):
            def put(self, item, *args, **kwargs):
                result = super().put(item, *args, **kwargs)
                controls.stop_event.set()
                return result

        class FakeClient:
            def listObjects(self, bucket, delimiter="/", prefix="", marker=None, max_keys=1000):
                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(
                        commonPrefixes=[],
                        contents=[
                            SimpleNamespace(key="root/a.txt", size=1, lastModified=None, etag="a"),
                            SimpleNamespace(key="root/b.txt", size=1, lastModified=None, etag="b"),
                            SimpleNamespace(key="root/c.txt", size=1, lastModified=None, etag="c"),
                        ],
                        is_truncated=False,
                        next_marker=None,
                    ),
                )

        task_queue = StopAfterFirstPutQueue()

        scan_s3_sources(
            [{"bucket": "bucket", "prefix": "root"}],
            FakeClient(),
            "bucket",
            task_queue,
            Progress(),
            scan_workers=1,
            controls=controls,
        )

        self.assertEqual(task_queue.qsize(), 1)

    def test_s3_scanner_exits_when_stopped_while_output_queue_full(self):
        controls = TaskControls()
        task_queue = queue.Queue(maxsize=1)
        task_queue.put({"source_path": "prefill.txt"})

        class FakeClient:
            def listObjects(self, bucket, delimiter="/", prefix="", marker=None, max_keys=1000):
                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(
                        commonPrefixes=[],
                        contents=[SimpleNamespace(key="root/a.txt", size=1, lastModified=None, etag="a")],
                        is_truncated=False,
                        next_marker=None,
                    ),
                )

        thread = threading.Thread(
            target=scan_s3_sources,
            args=(
                [{"bucket": "bucket", "prefix": "root"}],
                FakeClient(),
                "bucket",
                task_queue,
                Progress(),
            ),
            kwargs={"scan_workers": 1, "controls": controls},
            daemon=True,
        )
        thread.start()
        time.sleep(0.2)
        controls.stop_event.set()
        thread.join(timeout=0.5)
        exited_before_drain = not thread.is_alive()

        if thread.is_alive():
            task_queue.get_nowait()
            task_queue.task_done()
            thread.join(timeout=1)

        self.assertTrue(exited_before_drain)


if __name__ == "__main__":
    unittest.main()

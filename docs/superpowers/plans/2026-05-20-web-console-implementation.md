# Web Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the standard-library Web console described in `docs/superpowers/specs/2026-05-20-web-console-design.md` while preserving the existing CLI workflow.

**Architecture:** Add focused Web modules under `core/`, then refactor `obs_migrate.py` so CLI and Web task control share one migration runner. The Web server uses `http.server`, server-side sessions, polling APIs, existing object-browser helpers, and a process-local `TaskManager`.

**Tech Stack:** Python standard library `http.server`, `threading`, `configparser`, plain HTML/CSS/JavaScript, existing `unittest` tests.

---

## File Structure

- Modify: `obs_migrate.py` — add `[WEB_UI]` defaults, CLI `--web`, Web startup, config helpers, and `run_migration(cfg, controls=None)`.
- Modify: `config.example.ini` — add `[WEB_UI]` documented defaults.
- Modify: `core/__init__.py` — export `TaskManager`, `TaskControls`, and Web server helpers.
- Modify: `core/scheduler.py` — add optional pause/stop controls before workers claim new queue items.
- Modify: `core/scanner.py` — accept optional controls and pause before enqueueing new tasks.
- Modify: `core/s3_scanner.py` — accept optional controls and pause before enqueueing new tasks.
- Modify: `core/dashboard.py` — expose reusable status snapshot data for Web dashboard polling.
- Create: `core/task_manager.py` — own task state, background thread, pause/resume/stop events, and snapshots.
- Create: `core/web_config.py` — mask, validate, patch, save, reload, and lock configuration safely.
- Create: `core/web_ui.py` — embedded HTTP server, auth/session handling, JSON routes, static page.
- Create: `tests/test_web_config.py` — config defaults, auth enforcement, masking, preservation, locks.
- Create: `tests/test_task_manager.py` — task state transitions and idempotent controls.
- Create: `tests/test_web_ui.py` — handler-level API tests with direct HTTP requests against localhost.

---

### Task 1: Add Web Config Defaults

**Files:**
- Modify: `obs_migrate.py`
- Modify: `config.example.ini`
- Test: `tests/test_web_config.py`

- [ ] **Step 1: Write failing tests for `[WEB_UI]` defaults**

Add `tests/test_web_config.py`:

```python
"""Tests for Web console configuration helpers."""

import configparser
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import obs_migrate


class WebConfigDefaultTests(unittest.TestCase):
    def test_default_config_contains_web_ui_section(self):
        self.assertIn("WEB_UI", obs_migrate.DEFAULT_CONFIG)
        self.assertEqual(obs_migrate.DEFAULT_CONFIG["WEB_UI"]["enabled"], "false")
        self.assertEqual(obs_migrate.DEFAULT_CONFIG["WEB_UI"]["host"], "127.0.0.1")
        self.assertEqual(obs_migrate.DEFAULT_CONFIG["WEB_UI"]["port"], "8765")
        self.assertEqual(obs_migrate.DEFAULT_CONFIG["WEB_UI"]["require_login"], "true")
        self.assertEqual(obs_migrate.DEFAULT_CONFIG["WEB_UI"]["username"], "admin")
        self.assertEqual(obs_migrate.DEFAULT_CONFIG["WEB_UI"]["password"], "admin")
        self.assertEqual(obs_migrate.DEFAULT_CONFIG["WEB_UI"]["auto_open"], "false")
        self.assertIn(("WEB_UI", "password"), obs_migrate.SENSITIVE_FIELDS)

    def test_load_config_adds_missing_web_ui_section(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "config.ini"
            cfg_path.write_text("[UI]\nprompt_config = false\nshow_dashboard = true\n", encoding="utf-8")

            with patch.object(obs_migrate, "CONFIG_FILE", str(cfg_path)), \
                    patch.object(obs_migrate, "should_prompt_config", return_value=False):
                cfg = obs_migrate.load_config()

            self.assertTrue(cfg.has_section("WEB_UI"))
            self.assertEqual(cfg.get("WEB_UI", "username"), "admin")
            self.assertEqual(cfg.get("WEB_UI", "password"), "admin")
            self.assertIn("[WEB_UI]", cfg_path.read_text(encoding="utf-8"))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_web_config.WebConfigDefaultTests -v`

Expected: FAIL because `WEB_UI` is not in `DEFAULT_CONFIG`.

- [ ] **Step 3: Implement Web defaults**

In `obs_migrate.py`, extend the existing constants:

```python
WEB_UI_SECTION = "WEB_UI"

SENSITIVE_FIELDS = {
    (SOURCE_SECTION, "ak"),
    (SOURCE_SECTION, "sk"),
    (TARGET_SECTION, "ak"),
    (TARGET_SECTION, "sk"),
    (WEB_UI_SECTION, "password"),
}
```

Add config descriptions and section title:

```python
CONFIG_DESC.update({
    "WEB_UI.enabled": "是否启动内置 Web 控制台",
    "WEB_UI.host": "Web 控制台监听地址",
    "WEB_UI.port": "Web 控制台监听端口",
    "WEB_UI.require_login": "访问 Web 控制台时是否要求登录",
    "WEB_UI.username": "Web 控制台管理员用户名",
    "WEB_UI.password": "Web 控制台管理员密码",
    "WEB_UI.auto_open": "启动 Web 控制台后是否自动打开浏览器",
})

SECTION_TITLES[WEB_UI_SECTION] = "Web 控制台配置"
```

Add defaults after the existing `UI` section:

```python
DEFAULT_CONFIG[WEB_UI_SECTION] = {
    "enabled": "false",
    "host": "127.0.0.1",
    "port": "8765",
    "require_login": "true",
    "username": "admin",
    "password": "admin",
    "auto_open": "false",
}
```

Add a menu group:

```python
CONFIG_MENU_GROUPS.append({"id": "web_ui", "title": "Web 控制台配置", "sections": [WEB_UI_SECTION]})
CONFIG_MENU_GROUP_INDEX = _build_config_menu_group_index(CONFIG_MENU_GROUPS)
```

- [ ] **Step 4: Update example config**

Append to `config.example.ini`:

```ini

# ------------------------------
# WEB_UI
# ------------------------------
[WEB_UI]

# 是否启动内置 Web 控制台
enabled = false

# Web 控制台监听地址
host = 127.0.0.1

# Web 控制台监听端口
port = 8765

# 访问 Web 控制台时是否要求登录
require_login = true

# Web 控制台管理员用户名
username = admin

# Web 控制台管理员密码
password = admin

# 启动 Web 控制台后是否自动打开浏览器
auto_open = false
```

- [ ] **Step 5: Run tests**

Run: `python -m unittest tests.test_web_config.WebConfigDefaultTests -v`

Expected: PASS.

---

### Task 2: Add Config API Helpers

**Files:**
- Create: `core/web_config.py`
- Test: `tests/test_web_config.py`

- [ ] **Step 1: Write failing tests for masking and preservation**

Append to `tests/test_web_config.py`:

```python
from core.web_config import (
    LOCKED_RUNNING_SECTIONS,
    apply_config_payload,
    config_to_payload,
    validate_web_access,
)


class WebConfigHelperTests(unittest.TestCase):
    def make_cfg(self):
        cfg = configparser.ConfigParser()
        for section, items in obs_migrate.DEFAULT_CONFIG.items():
            cfg[section] = dict(items)
        cfg.set("SOURCE", "ak", "source-ak")
        cfg.set("SOURCE", "sk", "source-sk")
        cfg.set("WEB_UI", "password", "admin")
        return cfg

    def test_config_payload_masks_sensitive_fields(self):
        payload = config_to_payload(self.make_cfg(), decrypt_secret=obs_migrate.decrypt_value)
        source = payload["sections"]["SOURCE"]
        web_ui = payload["sections"]["WEB_UI"]

        self.assertEqual(source["ak"]["value"], "********")
        self.assertTrue(source["ak"]["sensitive"])
        self.assertEqual(web_ui["password"]["value"], "********")
        self.assertTrue(web_ui["password"]["sensitive"])

    def test_blank_sensitive_value_preserves_existing_config(self):
        cfg = self.make_cfg()
        apply_config_payload(
            cfg,
            {"SOURCE": {"ak": "********"}, "WEB_UI": {"password": ""}},
            encrypt_secret=lambda value: f"encrypted:{value}",
            task_running=False,
        )

        self.assertEqual(cfg.get("SOURCE", "ak"), "source-ak")
        self.assertEqual(cfg.get("WEB_UI", "password"), "admin")

    def test_changed_sensitive_value_is_encrypted(self):
        cfg = self.make_cfg()
        apply_config_payload(
            cfg,
            {"WEB_UI": {"password": "new-secret"}},
            encrypt_secret=lambda value: f"encrypted:{value}",
            task_running=False,
        )

        self.assertEqual(cfg.get("WEB_UI", "password"), "encrypted:new-secret")

    def test_running_task_locks_migration_semantics(self):
        cfg = self.make_cfg()
        with self.assertRaises(ValueError) as caught:
            apply_config_payload(
                cfg,
                {"SOURCE": {"path": "/other"}},
                encrypt_secret=lambda value: value,
                task_running=True,
            )

        self.assertIn("SOURCE.path", str(caught.exception))
        self.assertIn("SOURCE", LOCKED_RUNNING_SECTIONS)

    def test_external_host_requires_login(self):
        cfg = self.make_cfg()
        cfg.set("WEB_UI", "host", "0.0.0.0")
        cfg.set("WEB_UI", "require_login", "false")

        with self.assertRaises(ValueError) as caught:
            validate_web_access(cfg)

        self.assertIn("require_login", str(caught.exception))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_web_config.WebConfigHelperTests -v`

Expected: FAIL because `core.web_config` does not exist.

- [ ] **Step 3: Implement `core/web_config.py`**

Create `core/web_config.py`:

```python
# -*- coding: utf-8 -*-
"""Configuration helpers for the embedded Web console."""

import ipaddress


MASKED_SECRET = "********"
WEB_UI_SECTION = "WEB_UI"
SENSITIVE_FIELDS = {
    ("SOURCE", "ak"),
    ("SOURCE", "sk"),
    ("TARGET", "ak"),
    ("TARGET", "sk"),
    (WEB_UI_SECTION, "password"),
}
LOCKED_RUNNING_SECTIONS = {"SOURCE", "TARGET", "PATH"}
LOCKED_RUNNING_KEYS = {
    ("CHECK", "enable_etag_check"),
    ("CHECK", "enable_head_check"),
    ("CHECK", "strict_client_check"),
    ("CHECK", "target_compare_mode"),
    ("CHECK", "verify_after_upload"),
}


def is_sensitive(section, key):
    return (section, key) in SENSITIVE_FIELDS


def is_loopback_host(host):
    normalized = (host or "").strip().lower()
    if normalized in {"", "localhost", "127.0.0.1", "::1"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def validate_web_access(cfg):
    host = cfg.get(WEB_UI_SECTION, "host", fallback="127.0.0.1")
    require_login = cfg.getboolean(WEB_UI_SECTION, "require_login", fallback=True)
    if not is_loopback_host(host) and not require_login:
        raise ValueError("WEB_UI.require_login must be true when WEB_UI.host is not loopback")


def config_to_payload(cfg, decrypt_secret):
    sections = {}
    for section in cfg.sections():
        section_payload = {}
        for key, value in cfg.items(section):
            sensitive = is_sensitive(section, key)
            section_payload[key] = {
                "value": MASKED_SECRET if sensitive and value else value,
                "sensitive": sensitive,
            }
        sections[section] = section_payload
    return {"sections": sections}


def _is_locked_while_running(section, key):
    return section in LOCKED_RUNNING_SECTIONS or (section, key) in LOCKED_RUNNING_KEYS


def apply_config_payload(cfg, payload, encrypt_secret, task_running=False):
    changed = []
    for section, items in (payload or {}).items():
        if not cfg.has_section(section):
            raise ValueError(f"unknown config section: {section}")
        for key, value in (items or {}).items():
            if not cfg.has_option(section, key):
                raise ValueError(f"unknown config key: {section}.{key}")
            if task_running and _is_locked_while_running(section, key):
                raise ValueError(f"{section}.{key} is locked while a task is running")
            text = "" if value is None else str(value)
            if is_sensitive(section, key):
                if text in {"", MASKED_SECRET}:
                    continue
                text = encrypt_secret(text)
            cfg.set(section, key, text)
            changed.append(f"{section}.{key}")
    validate_web_access(cfg)
    return changed
```

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests.test_web_config -v`

Expected: PASS.

---

### Task 3: Add Task Manager

**Files:**
- Create: `core/task_manager.py`
- Modify: `core/__init__.py`
- Test: `tests/test_task_manager.py`

- [ ] **Step 1: Write failing state-transition tests**

Add `tests/test_task_manager.py`:

```python
"""Tests for process-local Web task management."""

import threading
import time
import unittest

from core.task_manager import TaskManager


class TaskManagerTests(unittest.TestCase):
    def test_task_runs_to_completion(self):
        def runner(cfg, controls):
            controls.update_status(progress={"files_done": 1})

        manager = TaskManager(runner, config_loader=lambda: {"name": "cfg"})
        result = manager.start()
        self.assertTrue(result["ok"])
        manager.join(timeout=2)

        snapshot = manager.snapshot()
        self.assertEqual(snapshot["state"], "completed")
        self.assertEqual(snapshot["progress"]["files_done"], 1)

    def test_only_one_task_runs_at_a_time(self):
        gate = threading.Event()

        def runner(cfg, controls):
            gate.wait(timeout=2)

        manager = TaskManager(runner, config_loader=lambda: {})
        self.assertTrue(manager.start()["ok"])
        second = manager.start()
        gate.set()
        manager.join(timeout=2)

        self.assertFalse(second["ok"])
        self.assertIn("already", second["error"])

    def test_pause_resume_stop_are_idempotent(self):
        release = threading.Event()

        def runner(cfg, controls):
            controls.wait_if_paused()
            while not controls.stop_requested():
                if release.wait(timeout=0.01):
                    break

        manager = TaskManager(runner, config_loader=lambda: {})
        self.assertTrue(manager.start()["ok"])
        self.assertTrue(manager.pause()["ok"])
        self.assertEqual(manager.snapshot()["state"], "pausing")
        self.assertTrue(manager.resume()["ok"])
        self.assertEqual(manager.snapshot()["state"], "running")
        self.assertTrue(manager.stop()["ok"])
        release.set()
        manager.join(timeout=2)
        self.assertIn(manager.snapshot()["state"], {"stopped", "completed"})

    def test_failure_state_records_error(self):
        def runner(cfg, controls):
            raise RuntimeError("boom")

        manager = TaskManager(runner, config_loader=lambda: {})
        self.assertTrue(manager.start()["ok"])
        manager.join(timeout=2)

        snapshot = manager.snapshot()
        self.assertEqual(snapshot["state"], "failed")
        self.assertIn("boom", snapshot["error"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_task_manager -v`

Expected: FAIL because `core.task_manager` does not exist.

- [ ] **Step 3: Implement task controls and manager**

Create `core/task_manager.py`:

```python
# -*- coding: utf-8 -*-
"""Process-local migration task manager for Web and CLI control surfaces."""

import threading
import time


ACTIVE_STATES = {"starting", "running", "pausing", "paused", "stopping"}


class TaskControls:
    def __init__(self):
        self.pause_event = threading.Event()
        self.stop_event = threading.Event()
        self.status_lock = threading.Lock()
        self.status = {}

    def pause_requested(self):
        return self.pause_event.is_set()

    def stop_requested(self):
        return self.stop_event.is_set()

    def wait_if_paused(self):
        while self.pause_event.is_set() and not self.stop_event.is_set():
            time.sleep(0.05)

    def update_status(self, **values):
        with self.status_lock:
            self.status.update(values)

    def snapshot(self):
        with self.status_lock:
            return dict(self.status)


class TaskManager:
    def __init__(self, runner, config_loader):
        self.runner = runner
        self.config_loader = config_loader
        self.lock = threading.Lock()
        self.state = "idle"
        self.error = ""
        self.thread = None
        self.controls = TaskControls()
        self.started_at = None
        self.finished_at = None

    def start(self):
        with self.lock:
            if self.state in ACTIVE_STATES:
                return {"ok": False, "error": "task already running"}
            self.state = "starting"
            self.error = ""
            self.finished_at = None
            self.started_at = time.time()
            self.controls = TaskControls()
            cfg = self.config_loader()
            self.thread = threading.Thread(target=self._run, args=(cfg, self.controls), daemon=True)
            self.thread.start()
            return {"ok": True}

    def _run(self, cfg, controls):
        with self.lock:
            self.state = "running"
        try:
            self.runner(cfg, controls)
        except Exception as exc:
            with self.lock:
                self.state = "failed"
                self.error = str(exc)
                self.finished_at = time.time()
        else:
            with self.lock:
                self.state = "stopped" if controls.stop_requested() else "completed"
                self.finished_at = time.time()

    def pause(self):
        with self.lock:
            if self.state not in {"running", "pausing", "paused"}:
                return {"ok": True}
            self.controls.pause_event.set()
            self.state = "pausing"
            return {"ok": True}

    def resume(self):
        with self.lock:
            if self.state in {"pausing", "paused", "running"}:
                self.controls.pause_event.clear()
                self.state = "running"
            return {"ok": True}

    def stop(self):
        with self.lock:
            if self.state in ACTIVE_STATES:
                self.controls.stop_event.set()
                self.controls.pause_event.clear()
                self.state = "stopping"
            return {"ok": True}

    def mark_paused_if_waiting(self):
        with self.lock:
            if self.state == "pausing":
                self.state = "paused"

    def snapshot(self):
        with self.lock:
            state = self.state
            error = self.error
            started_at = self.started_at
            finished_at = self.finished_at
            alive = bool(self.thread and self.thread.is_alive())
        status = self.controls.snapshot()
        return {
            "state": state,
            "error": error,
            "started_at": started_at,
            "finished_at": finished_at,
            "alive": alive,
            "progress": status.get("progress", {}),
            "pipeline": status.get("pipeline", {}),
            "workers": status.get("workers", {}),
        }

    def join(self, timeout=None):
        thread = self.thread
        if thread is not None:
            thread.join(timeout=timeout)
```

- [ ] **Step 4: Export task manager**

In `core/__init__.py`, add:

```python
from .task_manager import TaskControls, TaskManager
```

Add to `__all__`:

```python
"TaskControls",
"TaskManager",
```

- [ ] **Step 5: Run tests**

Run: `python -m unittest tests.test_task_manager -v`

Expected: PASS.

---

### Task 4: Refactor Migration Runner Controls

**Files:**
- Modify: `obs_migrate.py`
- Modify: `core/scheduler.py`
- Modify: `core/scanner.py`
- Modify: `core/s3_scanner.py`
- Test: `tests/test_task_manager.py`

- [ ] **Step 1: Write failing controls tests**

Append to `tests/test_task_manager.py`:

```python
import queue
from unittest.mock import Mock

from core.scheduler import Scheduler
from core.task_manager import TaskControls


class SchedulerControlTests(unittest.TestCase):
    def test_scheduler_waits_while_paused_before_claiming_task(self):
        task_queue = queue.Queue()
        task_queue.put({"relative_path": "a.txt"})
        handler = Mock()
        controls = TaskControls()
        controls.pause_event.set()

        scheduler = Scheduler(task_queue, handler, workers=1, controls=controls)
        scheduler.start()
        time.sleep(0.1)
        self.assertEqual(handler.process.call_count, 0)

        controls.pause_event.clear()
        task_queue.join()
        scheduler.stop()
        self.assertEqual(handler.process.call_count, 1)

    def test_scheduler_stop_control_exits_without_claiming_new_task(self):
        task_queue = queue.Queue()
        task_queue.put({"relative_path": "a.txt"})
        handler = Mock()
        controls = TaskControls()
        controls.stop_event.set()

        scheduler = Scheduler(task_queue, handler, workers=1, controls=controls)
        scheduler.start()
        time.sleep(0.1)
        scheduler.stop()

        self.assertEqual(handler.process.call_count, 0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_task_manager.SchedulerControlTests -v`

Expected: FAIL because `Scheduler.__init__` does not accept `controls`.

- [ ] **Step 3: Add scheduler controls**

In `core/scheduler.py`, change constructor:

```python
def __init__(self, task_queue, handler, workers=32, stage_name="worker", stall_timeout=300, controls=None):
    self.task_queue = task_queue
    self.handler = handler
    self.workers = max(1, int(workers or 1))
    self.stage_name = str(stage_name or "worker")
    self.stall_timeout = max(float(stall_timeout or 0), 1.0)
    self.controls = controls
```

Add helper:

```python
def _wait_until_claim_allowed(self):
    while self.running:
        if self.controls is not None and self.controls.stop_requested():
            return False
        if self.controls is None or not self.controls.pause_requested():
            return True
        if hasattr(self.controls, "update_status"):
            self.controls.update_status(paused_stage=self.stage_name)
        time.sleep(0.05)
    return False
```

At the top of `_worker()` loop, before `self.task_queue.get(timeout=1)`, add:

```python
if not self._wait_until_claim_allowed():
    break
```

- [ ] **Step 4: Thread controls into migration runner**

In `obs_migrate.py`, extract current `main()` body into:

```python
def run_migration(cfg, controls=None):
    validate_config(cfg)
    _ensure_secret_fields_encrypted(cfg)
    ...
```

Update scheduler construction:

```python
checker_scheduler = Scheduler(
    check_queue,
    checker_handler,
    workers=checker_workers,
    stage_name="check",
    stall_timeout=worker_stall_timeout,
    controls=controls,
)
scheduler = Scheduler(
    task_queue,
    transfer_handler,
    workers=workers,
    stage_name="upload",
    stall_timeout=worker_stall_timeout,
    controls=controls,
)
```

Update `work_finished()`:

```python
if controls is not None and controls.stop_requested():
    if index_stop_event is not None:
        index_stop_event.set()
    scan_done_event.set()
```

Inside status updates, publish snapshots:

```python
def publish_status():
    if controls is None:
        return
    controls.update_status(
        progress=progress.snapshot(),
        pipeline=get_status(),
        workers={
            "check": checker_scheduler.get_status_snapshot(),
            "upload": scheduler.get_status_snapshot(),
        },
    )
```

Call `publish_status()` inside `work_finished()` before returning.

Leave `main()` as:

```python
def main(argv=None):
    ensure_dirs()
    cfg = load_config()
    run_migration(cfg)
```

- [ ] **Step 5: Run tests**

Run: `python -m unittest tests.test_task_manager -v`

Expected: PASS.

---

### Task 5: Add Web Server and Auth Shell

**Files:**
- Create: `core/web_ui.py`
- Modify: `core/__init__.py`
- Test: `tests/test_web_ui.py`

- [ ] **Step 1: Write failing HTTP auth tests**

Add `tests/test_web_ui.py`:

```python
"""HTTP-level tests for the embedded Web console."""

import configparser
import http.client
import json
import threading
import unittest

import obs_migrate
from core.task_manager import TaskManager
from core.web_ui import WebConsoleServer


class WebUiServerTests(unittest.TestCase):
    def make_cfg(self):
        cfg = configparser.ConfigParser()
        for section, items in obs_migrate.DEFAULT_CONFIG.items():
            cfg[section] = dict(items)
        cfg.set("WEB_UI", "host", "127.0.0.1")
        cfg.set("WEB_UI", "port", "0")
        cfg.set("WEB_UI", "require_login", "true")
        cfg.set("WEB_UI", "username", "admin")
        cfg.set("WEB_UI", "password", "admin")
        return cfg

    def start_server(self, cfg=None):
        manager = TaskManager(lambda cfg, controls: None, config_loader=lambda: cfg or self.make_cfg())
        server = WebConsoleServer(
            cfg or self.make_cfg(),
            task_manager=manager,
            config_loader=lambda: cfg or self.make_cfg(),
            config_saver=lambda cfg: None,
            decrypt_secret=obs_migrate.decrypt_value,
            encrypt_secret=lambda value: f"encrypted:{value}",
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(thread.join, 2)
        return server

    def request(self, server, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection(server.host, server.port, timeout=3)
        payload = None if body is None else json.dumps(body).encode("utf-8")
        request_headers = {"Content-Type": "application/json"}
        request_headers.update(headers or {})
        conn.request(method, path, body=payload, headers=request_headers)
        response = conn.getresponse()
        data = response.read().decode("utf-8")
        conn.close()
        return response.status, response.getheaders(), data

    def test_login_required_for_config_api(self):
        server = self.start_server()
        status, headers, data = self.request(server, "GET", "/api/config")
        self.assertEqual(status, 401)

    def test_login_sets_session_cookie(self):
        server = self.start_server()
        status, headers, data = self.request(
            server,
            "POST",
            "/api/login",
            {"username": "admin", "password": "admin"},
        )

        self.assertEqual(status, 200)
        self.assertIn('"ok": true', data)
        self.assertTrue(any(name.lower() == "set-cookie" for name, value in headers))

    def test_config_api_works_after_login(self):
        server = self.start_server()
        status, headers, data = self.request(
            server,
            "POST",
            "/api/login",
            {"username": "admin", "password": "admin"},
        )
        cookie = next(value for name, value in headers if name.lower() == "set-cookie").split(";", 1)[0]

        status, headers, data = self.request(server, "GET", "/api/config", headers={"Cookie": cookie})

        self.assertEqual(status, 200)
        self.assertIn("WEB_UI", data)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_web_ui.WebUiServerTests -v`

Expected: FAIL because `core.web_ui` does not exist.

- [ ] **Step 3: Implement `WebConsoleServer`**

Create `core/web_ui.py` with:

```python
# -*- coding: utf-8 -*-
"""Embedded standard-library Web console."""

import json
import secrets
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .web_config import apply_config_payload, config_to_payload, validate_web_access


INDEX_HTML = """<!doctype html>
<html lang="zh">
<head><meta charset="utf-8"><title>OBS Sync Web Console</title></head>
<body>
<h1>OBS Sync Web Console</h1>
<nav><button data-view="config">配置</button><button data-view="browser">目录浏览</button><button data-view="dashboard">任务仪表盘</button><button data-view="logs">日志/报告</button></nav>
<main id="app">Loading...</main>
<script>document.getElementById('app').textContent = 'Web 控制台已启动';</script>
</body>
</html>
"""


class WebConsoleServer:
    def __init__(self, cfg, task_manager, config_loader, config_saver, decrypt_secret, encrypt_secret):
        validate_web_access(cfg)
        self.cfg = cfg
        self.task_manager = task_manager
        self.config_loader = config_loader
        self.config_saver = config_saver
        self.decrypt_secret = decrypt_secret
        self.encrypt_secret = encrypt_secret
        self.sessions = set()
        self.session_lock = threading.Lock()
        self.host = cfg.get("WEB_UI", "host", fallback="127.0.0.1")
        requested_port = cfg.getint("WEB_UI", "port", fallback=8765)
        self.httpd = ThreadingHTTPServer((self.host, requested_port), self._handler_class())
        self.port = self.httpd.server_address[1]

    def _handler_class(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                return

            def do_GET(self):
                if self.path == "/" or self.path == "/index.html":
                    self._send(HTTPStatus.OK, INDEX_HTML, "text/html; charset=utf-8")
                    return
                if self.path == "/api/config":
                    if not self._require_auth():
                        return
                    payload = config_to_payload(server.config_loader(), server.decrypt_secret)
                    self._json(HTTPStatus.OK, payload)
                    return
                if self.path == "/api/task/status":
                    if not self._require_auth():
                        return
                    self._json(HTTPStatus.OK, server.task_manager.snapshot())
                    return
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

            def do_POST(self):
                if self.path == "/api/login":
                    self._login()
                    return
                if not self._require_auth():
                    return
                if self.path == "/api/config":
                    cfg = server.config_loader()
                    body = self._read_json()
                    changed = apply_config_payload(
                        cfg,
                        body.get("sections", body),
                        server.encrypt_secret,
                        task_running=server.task_manager.snapshot()["state"] in {"starting", "running", "pausing", "paused", "stopping"},
                    )
                    server.config_saver(cfg)
                    self._json(HTTPStatus.OK, {"ok": True, "changed": changed})
                    return
                actions = {
                    "/api/task/start": server.task_manager.start,
                    "/api/task/pause": server.task_manager.pause,
                    "/api/task/resume": server.task_manager.resume,
                    "/api/task/stop": server.task_manager.stop,
                }
                if self.path in actions:
                    self._json(HTTPStatus.OK, actions[self.path]())
                    return
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

            def _login(self):
                body = self._read_json()
                expected_user = server.cfg.get("WEB_UI", "username", fallback="admin")
                expected_password = server.decrypt_secret(server.cfg.get("WEB_UI", "password", fallback="admin"))
                if body.get("username") != expected_user or body.get("password") != expected_password:
                    self._json(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "invalid credentials"})
                    return
                token = secrets.token_urlsafe(32)
                with server.session_lock:
                    server.sessions.add(token)
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Set-Cookie", f"obs_web_session={token}; HttpOnly; SameSite=Strict; Path=/")
                self.end_headers()
                self.wfile.write(b'{"ok": true}')

            def _require_auth(self):
                if not server.cfg.getboolean("WEB_UI", "require_login", fallback=True):
                    return True
                cookie = self.headers.get("Cookie", "")
                token = ""
                for part in cookie.split(";"):
                    name, _, value = part.strip().partition("=")
                    if name == "obs_web_session":
                        token = value
                        break
                with server.session_lock:
                    ok = token in server.sessions
                if not ok:
                    self._json(HTTPStatus.UNAUTHORIZED, {"error": "login required"})
                return ok

            def _read_json(self):
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0:
                    return {}
                return json.loads(self.rfile.read(length).decode("utf-8"))

            def _json(self, status, payload):
                self._send(status, json.dumps(payload, ensure_ascii=False), "application/json; charset=utf-8")

            def _send(self, status, text, content_type):
                data = text.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        return Handler

    def serve_forever(self):
        self.httpd.serve_forever()

    def shutdown(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    @property
    def url(self):
        return f"http://{self.host}:{self.port}"
```

- [ ] **Step 4: Export Web server**

In `core/__init__.py`, add:

```python
from .web_ui import WebConsoleServer
```

Add to `__all__`:

```python
"WebConsoleServer",
```

- [ ] **Step 5: Run tests**

Run: `python -m unittest tests.test_web_ui -v`

Expected: PASS.

---

### Task 6: Add Browser and Source-List APIs

**Files:**
- Modify: `core/web_ui.py`
- Test: `tests/test_web_ui.py`

- [ ] **Step 1: Write failing browser API tests**

Append to `tests/test_web_ui.py`:

```python
import tempfile
from pathlib import Path


class WebUiBrowserApiTests(WebUiServerTests):
    def login_cookie(self, server):
        status, headers, data = self.request(
            server,
            "POST",
            "/api/login",
            {"username": "admin", "password": "admin"},
        )
        return next(value for name, value in headers if name.lower() == "set-cookie").split(";", 1)[0]

    def test_local_browser_api_lists_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "a.txt").write_text("a", encoding="utf-8")
            server = self.start_server()
            cookie = self.login_cookie(server)

            status, headers, data = self.request(
                server,
                "GET",
                f"/api/browser/local?path={tmp}",
                headers={"Cookie": cookie},
            )

        self.assertEqual(status, 200)
        self.assertIn("a.txt", data)

    def test_source_list_api_appends_unique_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self.make_cfg()
            cfg.set("PATH", "migration_list_file", str(Path(tmp) / "migration_list.txt"))
            server = self.start_server(cfg)
            cookie = self.login_cookie(server)

            status, headers, data = self.request(
                server,
                "POST",
                "/api/source-list",
                {"items": ["a", "a", "b"]},
                headers={"Cookie": cookie},
            )

            self.assertEqual(status, 200)
            self.assertEqual(Path(tmp, "migration_list.txt").read_text(encoding="utf-8").splitlines(), ["a", "b"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_web_ui.WebUiBrowserApiTests -v`

Expected: FAIL because routes are missing.

- [ ] **Step 3: Add serialization helper**

In `core/web_ui.py`, add:

```python
from urllib.parse import parse_qs, urlparse
from .object_browser import create_obs_client, list_local_path, list_remote_buckets, list_remote_prefix


def browser_page_to_dict(page):
    return {
        "scope": page.scope,
        "bucket": page.bucket,
        "prefix": page.prefix,
        "path": page.path,
        "page": page.page,
        "page_size": page.page_size,
        "total_known": page.total_known,
        "next_marker": page.next_marker,
        "has_next": page.has_next,
        "items": [
            {
                "name": item.name,
                "kind": item.kind,
                "path": item.path,
                "size": item.size,
                "mtime": item.mtime,
                "etag": item.etag,
                "storage_class": item.storage_class,
            }
            for item in page.items
        ],
    }
```

- [ ] **Step 4: Add local/source-list routes**

In `do_GET`, parse URL once:

```python
parsed = urlparse(self.path)
path = parsed.path
query = parse_qs(parsed.query)
```

Add route:

```python
if path == "/api/browser/local":
    if not self._require_auth():
        return
    page = list_local_path(
        query.get("path", [""])[0],
        page=int(query.get("page", ["1"])[0] or "1"),
        page_size=int(query.get("page_size", ["50"])[0] or "50"),
        filters=query.get("filter", []),
    )
    self._json(HTTPStatus.OK, browser_page_to_dict(page))
    return
```

In `do_POST`, add source list route:

```python
if self.path == "/api/source-list":
    cfg = server.config_loader()
    body = self._read_json()
    list_file = body.get("file") or cfg.get("PATH", "migration_list_file", fallback="./migration_list.txt")
    existing = []
    try:
        with open(list_file, "r", encoding="utf-8") as handle:
            existing = [line.strip() for line in handle if line.strip()]
    except FileNotFoundError:
        existing = []
    merged = list(existing)
    for item in body.get("items", []):
        text = str(item).strip()
        if text and text not in merged:
            merged.append(text)
    with open(list_file, "w", encoding="utf-8") as handle:
        handle.write("\n".join(merged) + ("\n" if merged else ""))
    self._json(HTTPStatus.OK, {"ok": True, "count": len(merged)})
    return
```

- [ ] **Step 5: Add remote route with fakeable client factory**

Extend `WebConsoleServer.__init__`:

```python
def __init__(..., obs_client_factory=create_obs_client):
    ...
    self.obs_client_factory = obs_client_factory
```

Add `GET /api/browser/remote` route:

```python
if path == "/api/browser/remote":
    if not self._require_auth():
        return
    cfg = server.config_loader()
    section = query.get("section", ["SOURCE"])[0]
    client = server.obs_client_factory(
        server.decrypt_secret(cfg.get(section, "ak", fallback="")),
        server.decrypt_secret(cfg.get(section, "sk", fallback="")),
        cfg.get(section, "endpoint", fallback=""),
        request_timeout=cfg.getint("UPLOAD", "request_timeout", fallback=60),
    )
    bucket = query.get("bucket", [cfg.get(section, "bucket", fallback="")])[0]
    if bucket:
        page = list_remote_prefix(
            client,
            bucket,
            prefix=query.get("prefix", [""])[0],
            marker=query.get("marker", [None])[0],
            page_size=int(query.get("page_size", ["50"])[0] or "50"),
            low_level_retries=cfg.getint("UPLOAD", "low_level_retries", fallback=5),
            low_level_retry_sleep=cfg.getfloat("UPLOAD", "low_level_retry_sleep", fallback=0.5),
            filters=query.get("filter", []),
        )
    else:
        page = list_remote_buckets(client)
    self._json(HTTPStatus.OK, browser_page_to_dict(page))
    return
```

- [ ] **Step 6: Run tests**

Run: `python -m unittest tests.test_web_ui -v`

Expected: PASS.

---

### Task 7: Add CLI `--web` Startup

**Files:**
- Modify: `obs_migrate.py`
- Test: `tests/test_web_ui.py`

- [ ] **Step 1: Write failing startup tests**

Append to `tests/test_web_ui.py`:

```python
class WebStartupTests(unittest.TestCase):
    def test_parse_args_accepts_web_flag(self):
        args = obs_migrate.parse_args(["--web"])
        self.assertTrue(args.web)

    def test_should_start_web_ui_honors_config_and_flag(self):
        cfg = configparser.ConfigParser()
        for section, items in obs_migrate.DEFAULT_CONFIG.items():
            cfg[section] = dict(items)
        self.assertFalse(obs_migrate.should_start_web_ui(cfg, web_flag=False))
        self.assertTrue(obs_migrate.should_start_web_ui(cfg, web_flag=True))
        cfg.set("WEB_UI", "enabled", "true")
        self.assertTrue(obs_migrate.should_start_web_ui(cfg, web_flag=False))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_web_ui.WebStartupTests -v`

Expected: FAIL because `parse_args` and `should_start_web_ui` are missing.

- [ ] **Step 3: Add argparse helpers**

In `obs_migrate.py`, import argparse:

```python
import argparse
import webbrowser
```

Add helpers:

```python
def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="OBS / S3 migration tool")
    parser.add_argument("--web", action="store_true", help="start embedded Web console for this run")
    return parser.parse_args(argv)


def should_start_web_ui(cfg, web_flag=False):
    return bool(web_flag or cfg.getboolean(WEB_UI_SECTION, "enabled", fallback=False))
```

- [ ] **Step 4: Start Web server from `main()`**

In `obs_migrate.py`, import:

```python
from core import TaskManager, WebConsoleServer
```

Add:

```python
def _start_web_console(cfg):
    manager = TaskManager(run_migration, config_loader=load_config)
    server = WebConsoleServer(
        cfg,
        task_manager=manager,
        config_loader=load_config,
        config_saver=write_config_with_comments,
        decrypt_secret=decrypt_value,
        encrypt_secret=encrypt_value,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Web 控制台: {server.url}")
    if cfg.getboolean(WEB_UI_SECTION, "auto_open", fallback=False):
        try:
            webbrowser.open(server.url)
        except Exception as exc:
            print(f"⚠️ 自动打开浏览器失败: {exc}")
    return server, thread
```

Update `main()`:

```python
def main(argv=None):
    ensure_dirs()
    args = parse_args(argv)
    cfg = load_config()
    if should_start_web_ui(cfg, web_flag=args.web):
        server, thread = _start_web_console(cfg)
        try:
            run_migration(cfg)
        finally:
            server.shutdown()
            thread.join(timeout=2)
        return
    run_migration(cfg)
```

- [ ] **Step 5: Run tests**

Run: `python -m unittest tests.test_web_ui.WebStartupTests -v`

Expected: PASS.

---

### Task 8: Fill Operations Shell Frontend

**Files:**
- Modify: `core/web_ui.py`
- Test: `tests/test_web_ui.py`

- [ ] **Step 1: Write failing static page test**

Append to `tests/test_web_ui.py`:

```python
class WebUiStaticPageTests(WebUiServerTests):
    def test_index_contains_operations_shell_sections(self):
        server = self.start_server()
        status, headers, data = self.request(server, "GET", "/")

        self.assertEqual(status, 200)
        self.assertIn("配置", data)
        self.assertIn("目录浏览", data)
        self.assertIn("任务仪表盘", data)
        self.assertIn("日志/报告", data)
        self.assertIn("/api/task/status", data)
```

- [ ] **Step 2: Run test to verify it fails or is incomplete**

Run: `python -m unittest tests.test_web_ui.WebUiStaticPageTests -v`

Expected: FAIL until the page includes polling and all sections.

- [ ] **Step 3: Replace `INDEX_HTML` with self-contained UI**

In `core/web_ui.py`, replace `INDEX_HTML` with a plain HTML/CSS/JS page that:

```html
<nav>
  <button data-view="config">配置</button>
  <button data-view="browser">目录浏览</button>
  <button data-view="dashboard">任务仪表盘</button>
  <button data-view="logs">日志/报告</button>
</nav>
<section id="status-bar"></section>
<main>
  <section id="view-config"></section>
  <section id="view-browser" hidden></section>
  <section id="view-dashboard" hidden></section>
  <section id="view-logs" hidden></section>
</main>
<script>
async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: 'same-origin',
    headers: {'Content-Type': 'application/json'},
    ...options
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}
async function refreshStatus() {
  const status = await api('/api/task/status');
  document.getElementById('status-bar').textContent = `任务状态: ${status.state}`;
}
setInterval(refreshStatus, 1500);
refreshStatus().catch(() => {});
</script>
```

Include buttons that call:

```javascript
api('/api/task/start', {method: 'POST', body: '{}'});
api('/api/task/pause', {method: 'POST', body: '{}'});
api('/api/task/resume', {method: 'POST', body: '{}'});
api('/api/task/stop', {method: 'POST', body: '{}'});
```

- [ ] **Step 4: Run static page test**

Run: `python -m unittest tests.test_web_ui.WebUiStaticPageTests -v`

Expected: PASS.

---

### Task 9: Validate Whole Slice

**Files:**
- Modify: `README.md`
- Test: all Web-related tests plus existing core tests.

- [ ] **Step 1: Document Web startup**

Add to `README.md` near configuration/startup docs:

```markdown
### Web 控制台

工具默认保持 CLI 行为。要启动内置 Web 控制台，可以设置：

```ini
[WEB_UI]
enabled = true
host = 127.0.0.1
port = 8765
require_login = true
username = admin
password = admin
auto_open = false
```

也可以单次使用：

```bash
python obs_migrate.py --web
```

启动后终端会打印 `Web 控制台: http://127.0.0.1:8765`。如果 `host` 配置为非本机地址，必须开启 `require_login`。
```

- [ ] **Step 2: Run focused tests**

Run:

```powershell
python -m unittest tests.test_web_config tests.test_task_manager tests.test_web_ui -v
```

Expected: PASS.

- [ ] **Step 3: Run existing core tests**

Run:

```powershell
python -m unittest tests.test_core -v
```

Expected: PASS. If unrelated existing tests fail, capture the failure and do not broaden the Web patch to fix unrelated behavior.

- [ ] **Step 4: Manual smoke test**

Run:

```powershell
python obs_migrate.py --web
```

Expected:

- Terminal prints `Web 控制台: http://127.0.0.1:<port>`.
- Browser is not opened unless `auto_open = true`.
- `GET /` shows the Operations Shell.
- Login works with `admin/admin`.
- `GET /api/config` returns masked sensitive values.
- Starting a task from Web uses the same migration behavior as CLI.

---

## Self-Review

- **Spec coverage:** The plan covers `[WEB_UI]` defaults, standard-library Web server, login/session cookie, config masking and save behavior, local/remote browsing APIs, source-list updates, task start/pause/resume/stop, polling dashboard status, CLI `--web`, README docs, and validation.
- **Intentional first-slice constraint:** Graceful pause/stop is implemented at worker claim boundaries and scan enqueue boundaries; it does not hard-kill in-flight object storage calls, matching the spec.
- **Red-flag scan:** The plan does not rely on unspecified follow-up work or empty implementation notes.
- **Type consistency:** `TaskManager`, `TaskControls`, `WebConsoleServer`, `config_to_payload`, and `apply_config_payload` are introduced before dependent tasks use them.

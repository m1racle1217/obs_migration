import configparser
import http.client
import io
import json
import os
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode
from unittest import mock

import obs_migrate
from core.web_config import MASKED_SECRET
from core.task_manager import MultiTaskManager
from core.web_ui import WebConsoleServer


def make_config(require_login=True):
    cfg = configparser.ConfigParser()
    for section, items in obs_migrate.DEFAULT_CONFIG.items():
        cfg.add_section(section)
        for key, value in items.items():
            cfg.set(section, key, value)
    cfg.set("WEB_UI", "host", "127.0.0.1")
    cfg.set("WEB_UI", "port", "0")
    cfg.set("WEB_UI", "require_login", "true" if require_login else "false")
    cfg.set("WEB_UI", "username", "admin")
    cfg.set("WEB_UI", "password", "enc:secret")
    return cfg


class FakeTaskManager:
    def __init__(self, state="idle", start_result=True):
        self.calls = []
        self.state = state
        self.start_result = start_result

    def snapshot(self):
        return {"state": self.state, "progress": {"done": 1}}

    def start(self, cfg):
        self.calls.append(("start", cfg))
        return self.start_result

    def pause(self):
        self.calls.append(("pause",))
        return True

    def resume(self):
        self.calls.append(("resume",))
        return True

    def stop(self):
        self.calls.append(("stop",))
        return True


class WebClient:
    def __init__(self, server):
        self.server = server
        self.cookie = None

    def request(self, method, path, body=None, headers=None):
        headers = dict(headers or {})
        payload = None
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")
        if self.cookie:
            headers.setdefault("Cookie", self.cookie)

        conn = http.client.HTTPConnection(self.server.host, self.server.port, timeout=2)
        try:
            conn.request(method, path, body=payload, headers=headers)
            response = conn.getresponse()
            raw = response.read()
            set_cookie = response.getheader("Set-Cookie")
            if set_cookie:
                self.cookie = set_cookie.split(";", 1)[0]
            content_type = response.getheader("Content-Type") or ""
            if "application/json" in content_type:
                data = json.loads(raw.decode("utf-8"))
            else:
                data = raw.decode("utf-8")
            return response.status, data, dict(response.getheaders())
        finally:
            conn.close()

    def raw_request(self, method, path, body, headers=None):
        headers = dict(headers or {})
        conn = http.client.HTTPConnection(self.server.host, self.server.port, timeout=2)
        try:
            conn.request(method, path, body=body, headers=headers)
            response = conn.getresponse()
            raw = response.read()
            data = json.loads(raw.decode("utf-8"))
            return response.status, data, dict(response.getheaders())
        finally:
            conn.close()


class WebConsoleServerTests(unittest.TestCase):
    def make_server(self, cfg=None, task_manager=None, obs_client_factory=None, runtime_path_resolver=None):
        cfg = cfg or make_config()
        current_cfg = cfg
        saved = []

        def load_config():
            return current_cfg

        def save_config(next_cfg):
            saved.append(next_cfg)

        server = WebConsoleServer(
            cfg,
            task_manager or FakeTaskManager(),
            load_config,
            save_config,
            decrypt_secret=lambda value: value.removeprefix("enc:"),
            encrypt_secret=lambda value: f"enc:{value}",
            obs_client_factory=obs_client_factory,
            runtime_path_resolver=runtime_path_resolver,
        )
        self.addCleanup(server.stop)
        server.start()
        return server, WebClient(server), saved, cfg

    def test_login_required_blocks_api_until_valid_cookie_is_set(self):
        server, client, _saved, _cfg = self.make_server()

        status, data, _headers = client.request("GET", "/api/config")
        self.assertEqual(status, 401)
        self.assertFalse(data["ok"])

        status, data, headers = client.request(
            "POST",
            "/api/login",
            {"username": "admin", "password": "secret"},
        )

        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])
        self.assertIn("obs_web_session=", headers["Set-Cookie"])
        self.assertIn("HttpOnly", headers["Set-Cookie"])
        self.assertIn("SameSite=Strict", headers["Set-Cookie"])
        self.assertIn("Path=/", headers["Set-Cookie"])
        self.assertIn("Max-Age=43200", headers["Set-Cookie"])
        self.assertEqual(data["expires_in"], 43200)

        status, data, _headers = client.request("GET", "/api/config")
        self.assertEqual(status, 200)
        self.assertEqual(data["config"]["WEB_UI"]["password"]["value"], MASKED_SECRET)

    def test_require_login_false_allows_config_without_cookie(self):
        _server, client, _saved, _cfg = self.make_server(make_config(require_login=False))

        status, data, _headers = client.request("GET", "/api/config")

        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])
        self.assertIn("SOURCE", data["config"])

    def test_config_save_encrypts_changed_password_and_preserves_masked_value(self):
        _server, client, saved, cfg = self.make_server()
        client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

        status, data, _headers = client.request(
            "POST",
            "/api/config",
            {"WEB_UI": {"password": {"value": "new-secret"}, "auto_open": {"value": "true"}}},
        )

        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])
        self.assertEqual(
            data["changed"],
            ["WEB_UI.password", "WEB_UI.auto_open"],
        )
        self.assertEqual(cfg.get("WEB_UI", "password"), "enc:new-secret")
        self.assertEqual(len(saved), 1)

        status, data, _headers = client.request(
            "POST",
            "/api/config",
            {"WEB_UI": {"password": {"value": MASKED_SECRET}}},
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["changed"], [])
        self.assertEqual(cfg.get("WEB_UI", "password"), "enc:new-secret")

    def test_task_endpoints_call_manager(self):
        manager = FakeTaskManager(state="running")
        _server, client, _saved, _cfg = self.make_server(task_manager=manager)
        client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

        status, data, _headers = client.request("GET", "/api/task/status")
        self.assertEqual(status, 200)
        self.assertEqual(data["status"]["state"], "running")

        for action in ("start", "pause", "resume", "stop"):
            with self.subTest(action=action):
                status, data, _headers = client.request("POST", f"/api/task/{action}")
                self.assertEqual(status, 200)
                self.assertTrue(data["ok"])

        self.assertEqual([call[0] for call in manager.calls], ["start", "pause", "resume", "stop"])

    def test_task_start_reports_existing_running_task(self):
        manager = FakeTaskManager(state="running", start_result=False)
        _server, client, _saved, _cfg = self.make_server(task_manager=manager)
        client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

        status, data, _headers = client.request("POST", "/api/task/start")

        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])
        self.assertFalse(data["result"])
        self.assertEqual(manager.calls[0][0], "start")

    def test_local_browser_lists_files(self):
        _server, client, _saved, _cfg = self.make_server()
        client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "path with space"
            root.mkdir()
            Path(root, "alpha.txt").write_text("x", encoding="utf-8")
            Path(root, "folder").mkdir()

            query = urlencode({"path": str(root), "page_size": "20"})
            status, data, _headers = client.request("GET", f"/api/browser/local?{query}")

        self.assertEqual(status, 200)
        self.assertEqual(data["page"]["scope"], "local")
        self.assertEqual(
            [(item["kind"], item["name"]) for item in data["page"]["items"]],
            [("dir", "folder"), ("file", "alpha.txt")],
        )

    def test_source_list_appends_unique_items_preserving_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            list_file = os.path.join(temp_dir, "migration_list.txt")
            Path(list_file).write_text("existing\n", encoding="utf-8")
            cfg = make_config()
            cfg.set("PATH", "migration_list_file", list_file)
            _server, client, _saved, _cfg = self.make_server(cfg)
            client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

            status, data, _headers = client.request(
                "POST",
                "/api/source-list",
                {"items": ["existing", "new-one", "", "new-two", "new-one"]},
            )

            self.assertEqual(status, 200)
            self.assertTrue(data["ok"])
            self.assertEqual(data["added"], ["new-one", "new-two"])
            self.assertEqual(
                Path(list_file).read_text(encoding="utf-8").splitlines(),
                ["existing", "new-one", "new-two"],
            )

    def test_source_list_ignores_client_file_override(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            configured_file = os.path.join(temp_dir, "configured", "migration_list.txt")
            override_file = os.path.join(temp_dir, "override", "evil.txt")
            cfg = make_config()
            cfg.set("PATH", "migration_list_file", configured_file)
            _server, client, _saved, _cfg = self.make_server(cfg)
            client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

            status, data, _headers = client.request(
                "POST",
                "/api/source-list",
                {"file": override_file, "items": ["configured-only"]},
            )

            self.assertEqual(status, 200)
            self.assertEqual(data["file"], os.path.abspath(configured_file))
            self.assertEqual(Path(configured_file).read_text(encoding="utf-8").splitlines(), ["configured-only"])
            self.assertFalse(Path(override_file).exists())
            self.assertFalse(Path(override_file).parent.exists())

    def test_cross_origin_source_list_is_rejected_before_writing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            configured_file = os.path.join(temp_dir, "migration_list.txt")
            cfg = make_config()
            cfg.set("PATH", "migration_list_file", configured_file)
            _server, client, _saved, _cfg = self.make_server(cfg)
            client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

            status, data, _headers = client.request(
                "POST",
                "/api/source-list",
                {"items": ["must-not-write"]},
                headers={"Origin": "http://evil.example"},
            )

            self.assertEqual(status, 403)
            self.assertEqual(data, {"ok": False, "error": "forbidden"})
            self.assertFalse(Path(configured_file).exists())

    def test_same_origin_source_list_succeeds(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            configured_file = os.path.join(temp_dir, "migration_list.txt")
            cfg = make_config()
            cfg.set("PATH", "migration_list_file", configured_file)
            server, client, _saved, _cfg = self.make_server(cfg)
            client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

            status, data, _headers = client.request(
                "POST",
                "/api/source-list",
                {"items": ["same-origin"]},
                headers={
                    "Host": f"localhost:{server.port}",
                    "Origin": f"http://localhost:{server.port}",
                },
            )

            self.assertEqual(status, 200)
            self.assertEqual(data["added"], ["same-origin"])
            self.assertEqual(Path(configured_file).read_text(encoding="utf-8").splitlines(), ["same-origin"])

    def test_source_list_without_origin_or_referer_still_succeeds(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            configured_file = os.path.join(temp_dir, "migration_list.txt")
            cfg = make_config()
            cfg.set("PATH", "migration_list_file", configured_file)
            _server, client, _saved, _cfg = self.make_server(cfg)
            client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

            status, data, _headers = client.request(
                "POST",
                "/api/source-list",
                {"items": ["local-client"]},
            )

            self.assertEqual(status, 200)
            self.assertEqual(data["added"], ["local-client"])
            self.assertEqual(Path(configured_file).read_text(encoding="utf-8").splitlines(), ["local-client"])

    def test_source_list_uses_injected_runtime_path_resolver_for_relative_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_base = Path(temp_dir) / "config-base"
            cwd_base = Path(temp_dir) / "cwd"
            config_base.mkdir()
            cwd_base.mkdir()
            cfg = make_config()
            cfg.set("PATH", "migration_list_file", "./lists/migration_list.txt")

            original_cwd = os.getcwd()
            os.chdir(cwd_base)
            try:
                def resolve_runtime_path(value):
                    raw = str(value or "")
                    if os.path.isabs(raw):
                        return raw
                    return str((config_base / raw).resolve())

                _server, client, _saved, _cfg = self.make_server(
                    cfg,
                    runtime_path_resolver=resolve_runtime_path,
                )
                client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

                status, data, _headers = client.request("POST", "/api/source-list", {"items": ["relative-entry"]})
            finally:
                os.chdir(original_cwd)

            expected_file = config_base / "lists" / "migration_list.txt"
            cwd_file = cwd_base / "lists" / "migration_list.txt"
            self.assertEqual(status, 200)
            self.assertEqual(data["file"], str(expected_file.resolve()))
            self.assertEqual(expected_file.read_text(encoding="utf-8").splitlines(), ["relative-entry"])
            self.assertFalse(cwd_file.exists())

    def test_invalid_json_returns_generic_bad_request(self):
        _server, client, _saved, _cfg = self.make_server()

        status, data, _headers = client.raw_request(
            "POST",
            "/api/login",
            b'{"username":',
            {"Content-Type": "application/json"},
        )

        self.assertEqual(status, 400)
        self.assertEqual(data, {"ok": False, "error": "invalid JSON"})

    def test_internal_exception_returns_generic_error(self):
        class BrokenTaskManager(FakeTaskManager):
            def snapshot(self):
                raise RuntimeError("secret stack detail")

        _server, client, _saved, _cfg = self.make_server(
            make_config(require_login=False),
            task_manager=BrokenTaskManager(),
        )

        with self.assertLogs("core.web_ui", level="ERROR") as logs:
            status, data, _headers = client.request("GET", "/api/task/status")

        self.assertEqual(status, 500)
        self.assertEqual(data, {"ok": False, "error": "internal server error"})
        self.assertTrue(any("Unhandled Web console request error" in message for message in logs.output))

    def test_expired_session_cookie_is_rejected(self):
        server, client, _saved, _cfg = self.make_server()
        status, _data, _headers = client.request(
            "POST",
            "/api/login",
            {"username": "admin", "password": "secret"},
        )
        self.assertEqual(status, 200)
        token = client.cookie.split("=", 1)[1]

        with server._sessions_lock:
            server.sessions[token] = 0

        status, data, _headers = client.request("GET", "/api/config")

        self.assertEqual(status, 401)
        self.assertFalse(data["ok"])
        with server._sessions_lock:
            self.assertNotIn(token, server.sessions)

    def test_logout_clears_session_cookie_and_blocks_next_api_call(self):
        server, client, _saved, _cfg = self.make_server()
        status, _data, _headers = client.request(
            "POST",
            "/api/login",
            {"username": "admin", "password": "secret"},
        )
        self.assertEqual(status, 200)
        token = client.cookie.split("=", 1)[1]
        with server._sessions_lock:
            self.assertIn(token, server.sessions)

        status, data, headers = client.request("POST", "/api/logout")

        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])
        self.assertIn("obs_web_session=", headers["Set-Cookie"])
        self.assertIn("Max-Age=0", headers["Set-Cookie"])
        with server._sessions_lock:
            self.assertNotIn(token, server.sessions)

        client.cookie = None
        status, data, _headers = client.request("GET", "/api/config")
        self.assertEqual(status, 401)
        self.assertFalse(data["ok"])

    def test_multi_task_api_creates_starts_and_controls_independent_tasks(self):
        releases = {}
        started = {}

        def runner(cfg, controls):
            task_name = cfg.get("WEB_TASK", "name")
            started[task_name].set()
            controls.update_status(
                progress={
                    "done_bytes": int(cfg.get("WEB_TASK", "done_bytes")),
                    "total_bytes": 100,
                    "files_done": int(cfg.get("WEB_TASK", "files_done")),
                    "scan_files": int(cfg.get("WEB_TASK", "files_done")),
                },
                pipeline={"scan": "running", "check": "running", "index": "n/a"},
                workers={"upload": {"active_workers": 1, "workers": []}},
                queues={"transfer": {"current": 0, "max": 10}},
            )
            releases[task_name].wait(timeout=2)

        manager = MultiTaskManager(runner)
        _server, client, _saved, _cfg = self.make_server(task_manager=manager)
        client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

        for name, done_bytes, files_done in (("a", 20, 1), ("b", 40, 2)):
            releases[name] = threading.Event()
            started[name] = threading.Event()
            status, data, _headers = client.request(
                "POST",
                "/api/tasks",
                {
                    "name": f"Task {name.upper()}",
                    "config": {
                        "WEB_TASK": {
                            "name": {"value": name},
                            "done_bytes": {"value": str(done_bytes)},
                            "files_done": {"value": str(files_done)},
                        }
                    },
                    "concurrency": {"upload_workers": 3, "check_workers": 2, "scan_workers": 1},
                },
            )
            self.assertEqual(status, 200)
            self.assertTrue(data["ok"])
            self.assertIn("task_id", data)

        status, data, _headers = client.request("GET", "/api/tasks")
        self.assertEqual(status, 200)
        task_ids = [task["task_id"] for task in data["tasks"]]
        self.assertEqual(len(task_ids), 2)

        for task_id in task_ids:
            status, data, _headers = client.request("POST", f"/api/tasks/{task_id}/start")
            self.assertEqual(status, 200)
            self.assertTrue(data["ok"])

        self.assertTrue(started["a"].wait(timeout=1))
        self.assertTrue(started["b"].wait(timeout=1))

        status, data, _headers = client.request("GET", f"/api/tasks/{task_ids[0]}")
        self.assertEqual(status, 200)
        self.assertIn(data["task"]["state"], {"starting", "running"})
        for field in ("percent", "eta_seconds", "process_speed", "net_upload_speed", "hit_rate"):
            self.assertIn(field, data["task"]["dashboard"])

        status, data, _headers = client.request(
            "PATCH",
            f"/api/tasks/{task_ids[0]}/concurrency",
            {"upload_workers": 7, "check_workers": 4, "scan_workers": 2, "multipart_concurrency": 3},
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["task"]["concurrency"]["upload_workers"], 7)

        status, data, _headers = client.request("POST", f"/api/tasks/{task_ids[0]}/pause")
        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])
        self.assertEqual(data["task"]["state"], "paused")
        status, data, _headers = client.request("GET", f"/api/tasks/{task_ids[1]}")
        self.assertEqual(status, 200)
        self.assertIn(data["task"]["state"], {"starting", "running"})

        releases["a"].set()
        releases["b"].set()

    def test_remote_browser_lists_buckets_and_prefix(self):
        class FakeClient:
            def listBuckets(self):
                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(buckets=[SimpleNamespace(name="bucket-b"), SimpleNamespace(name="bucket-a")]),
                )

            def listObjects(self, bucket, delimiter="/", prefix="", marker=None, max_keys=1000):
                self.last_call = (bucket, delimiter, prefix, marker, max_keys)
                return SimpleNamespace(
                    status=200,
                    body=SimpleNamespace(
                        commonPrefixes=[SimpleNamespace(prefix="root/sub/")],
                        contents=[SimpleNamespace(key="root/file.txt", size=3, etag="etag")],
                        is_truncated=False,
                        next_marker=None,
                    ),
                )

        fake_client = FakeClient()
        factory_calls = []

        def factory(section, cfg):
            factory_calls.append(section)
            return fake_client

        _server, client, _saved, _cfg = self.make_server(obs_client_factory=factory)
        client.request("POST", "/api/login", {"username": "admin", "password": "secret"})

        status, data, _headers = client.request("GET", "/api/browser/remote?section=SOURCE&page_size=10")
        self.assertEqual(status, 200)
        self.assertEqual([item["name"] for item in data["page"]["items"]], ["bucket-a", "bucket-b"])

        status, data, _headers = client.request(
            "GET",
            "/api/browser/remote?section=TARGET&bucket=my-bucket&prefix=root&page_size=10",
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["page"]["bucket"], "my-bucket")
        self.assertEqual([(item["kind"], item["name"]) for item in data["page"]["items"]], [("dir", "sub"), ("file", "file.txt")])
        self.assertEqual(factory_calls, ["SOURCE", "TARGET"])

    def test_static_page_contains_shell_labels(self):
        _server, client, _saved, _cfg = self.make_server()

        status, html, headers = client.request("GET", "/")

        self.assertEqual(status, 200)
        self.assertIn("text/html", headers["Content-Type"])
        self.assertNotIn("body::after", html)
        for label in (
            "配置中心",
            "目录浏览",
            "任务仪表盘",
            "日志 / 报告",
            "<h2>登录</h2>",
            "默认：admin",
            "默认是 admin / admin",
            "新增任务",
            "退出登录",
            "启动任务",
            "后退",
            "前进",
            "上一级",
            "快速访问",
            "此电脑",
            "转到",
            "加入迁移列表",
            "填入任务配置",
            "实时上传速度",
            "活跃 Worker",
        ):
            self.assertIn(label, html)
        for endpoint in (
            "/api/tasks",
            "/api/task/status",
            "/api/task/start",
            "/api/task/pause",
            "/api/task/resume",
            "/api/task/stop",
        ):
            self.assertIn(endpoint, html)
        for marker in (
            'id="config-form"',
            'id="save-config"',
            'id="login-view"',
            'id="app-shell"',
            'id="logout-button"',
            'id="task-list"',
            'id="browser-table"',
            'id="browser-go"',
            'id="browser-breadcrumbs"',
            'id="browser-status"',
            'class="explorer-tree"',
            'data-browser-scope="local"',
            'data-browser-scope="SOURCE"',
            'data-page="dashboard"',
            'data-page="config"',
            'data-page="browser"',
            'data-page="logs"',
            'name="config-field"',
            'obsWebConsole.authenticated',
            'localStorage.setItem',
            'localStorage.removeItem',
            'function showPage',
            'function renderBreadcrumbs',
            'function browserKindLabel',
            'window.addEventListener("hashchange"',
            'api("/api/logout"',
            'api("/api/config", {',
            'method: "POST"',
        ):
            self.assertIn(marker, html)

    def test_config_reload_returns_current_payload(self):
        _server, client, _saved, cfg = self.make_server()
        client.request("POST", "/api/login", {"username": "admin", "password": "secret"})
        cfg.set("WEB_UI", "username", "changed")

        status, data, _headers = client.request("POST", "/api/config/reload")

        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])
        self.assertEqual(data["config"]["WEB_UI"]["username"]["value"], "changed")


class CliStartupTests(unittest.TestCase):
    def test_parse_args_web_flag(self):
        self.assertFalse(obs_migrate.parse_args([]).web)
        self.assertTrue(obs_migrate.parse_args(["--web"]).web)

    def test_should_start_web_ui_honors_flag_and_config(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "enabled", "false")
        self.assertFalse(obs_migrate.should_start_web_ui(cfg))
        self.assertTrue(obs_migrate.should_start_web_ui(cfg, web_flag=True))

        cfg.set("WEB_UI", "enabled", "true")
        self.assertTrue(obs_migrate.should_start_web_ui(cfg))

    def test_start_web_console_starts_port_zero_without_auto_open(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "port", "0")
        cfg.set("WEB_UI", "auto_open", "false")
        task_manager = FakeTaskManager()

        output = io.StringIO()
        with mock.patch.object(obs_migrate.webbrowser, "open") as browser_open:
            with redirect_stdout(output):
                server = obs_migrate._start_web_console(cfg, task_manager)
        self.addCleanup(server.stop)

        self.assertTrue(server._thread.is_alive())
        self.assertIn("Web 控制台:", output.getvalue())
        self.assertIn(server.url, output.getvalue())
        browser_open.assert_not_called()

    def test_web_console_config_loader_is_non_interactive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.ini"
            cfg = make_config(require_login=False)
            cfg.set("WEB_UI", "port", "0")
            cfg.set("UI", "prompt_config", "true")
            with config_path.open("w", encoding="utf-8") as handle:
                cfg.write(handle)

            with mock.patch.object(obs_migrate, "CONFIG_FILE", str(config_path)):
                with mock.patch.object(obs_migrate, "run_config_menu", side_effect=AssertionError("interactive prompt")):
                    with redirect_stdout(io.StringIO()):
                        server = obs_migrate._start_web_console(cfg, FakeTaskManager())
                    self.addCleanup(server.stop)
                    client = WebClient(server)
                    status, data, _headers = client.request("GET", "/api/config")

            self.assertEqual(status, 200)
            self.assertTrue(data["ok"])
            self.assertIn("WEB_UI", data["config"])

    def test_start_web_console_auto_open_opens_browser(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "port", "0")
        cfg.set("WEB_UI", "auto_open", "true")
        task_manager = FakeTaskManager()

        with mock.patch.object(obs_migrate.webbrowser, "open") as browser_open:
            with redirect_stdout(io.StringIO()):
                server = obs_migrate._start_web_console(cfg, task_manager)
        self.addCleanup(server.stop)

        browser_open.assert_called_once_with(server.url)

    def test_start_web_console_auto_open_warning_keeps_server_running(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "port", "0")
        cfg.set("WEB_UI", "auto_open", "true")
        task_manager = FakeTaskManager()

        output = io.StringIO()
        with mock.patch.object(obs_migrate.webbrowser, "open", side_effect=RuntimeError("browser failed")):
            with redirect_stdout(output):
                server = obs_migrate._start_web_console(cfg, task_manager)
        self.addCleanup(server.stop)

        self.assertTrue(server._thread.is_alive())
        self.assertIn("浏览器", output.getvalue())
        self.assertIn("browser failed", output.getvalue())

    def test_start_web_console_error_mentions_host_and_port(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "host", "127.0.0.1")
        cfg.set("WEB_UI", "port", "54321")
        task_manager = FakeTaskManager()

        class BrokenServer:
            def __init__(self, *args, **kwargs):
                raise OSError("address already in use")

        with mock.patch.object(obs_migrate, "WebConsoleServer", BrokenServer):
            with self.assertRaisesRegex(RuntimeError, "127\\.0\\.0\\.1.*54321"):
                obs_migrate._start_web_console(cfg, task_manager)

    def test_main_disabled_runs_migration_without_web_server(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "enabled", "false")
        cfg.set("UI", "prompt_config", "true")

        with mock.patch.object(obs_migrate, "ensure_dirs") as ensure_dirs:
            with mock.patch.object(obs_migrate, "load_config", return_value=cfg) as load_config:
                with mock.patch.object(obs_migrate, "validate_config") as validate_config:
                    with mock.patch.object(obs_migrate, "_ensure_secret_fields_encrypted") as encrypt_fields:
                        with mock.patch.object(obs_migrate, "run_config_menu") as run_config_menu:
                            with mock.patch.object(obs_migrate, "run_migration", return_value="done") as run_migration:
                                with mock.patch.object(obs_migrate, "WebConsoleServer") as web_server:
                                    result = obs_migrate.main([])

        self.assertEqual(result, "done")
        ensure_dirs.assert_called_once_with()
        load_config.assert_called_once_with(prompt=False)
        run_config_menu.assert_called_once_with(cfg)
        validate_config.assert_called_once_with(cfg)
        encrypt_fields.assert_called_once_with(cfg)
        run_migration.assert_called_once_with(cfg)
        web_server.assert_not_called()

    def test_main_web_flag_loads_config_without_interactive_prompt(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "enabled", "false")
        cfg.set("UI", "prompt_config", "true")
        task_manager = mock.Mock()
        server = mock.Mock(url="http://127.0.0.1:8765/")

        with mock.patch.object(obs_migrate, "ensure_dirs"):
            with mock.patch.object(obs_migrate, "load_config", return_value=cfg) as load_config:
                with mock.patch.object(obs_migrate, "run_config_menu", side_effect=AssertionError("interactive prompt")):
                    with mock.patch.object(obs_migrate, "validate_config"):
                        with mock.patch.object(obs_migrate, "_ensure_secret_fields_encrypted"):
                            with mock.patch.object(obs_migrate, "TaskManager", return_value=task_manager):
                                with mock.patch.object(obs_migrate, "WebConsoleServer", return_value=server):
                                    with mock.patch.object(obs_migrate, "_wait_for_web_console") as wait_for_web:
                                        with redirect_stdout(io.StringIO()):
                                            obs_migrate.main(["--web"])

        load_config.assert_called_once_with(prompt=False)
        task_manager.start.assert_not_called()
        wait_for_web.assert_called_once_with(server)
        server.stop.assert_called_once_with()

    def test_main_web_enabled_config_load_skips_interactive_prompt(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "enabled", "true")
        cfg.set("UI", "prompt_config", "true")
        task_manager = mock.Mock()
        server = mock.Mock(url="http://127.0.0.1:8765/")

        with mock.patch.object(obs_migrate, "ensure_dirs"):
            with mock.patch.object(obs_migrate, "load_config", return_value=cfg) as load_config:
                with mock.patch.object(obs_migrate, "run_config_menu", side_effect=AssertionError("interactive prompt")):
                    with mock.patch.object(obs_migrate, "validate_config"):
                        with mock.patch.object(obs_migrate, "_ensure_secret_fields_encrypted"):
                            with mock.patch.object(obs_migrate, "TaskManager", return_value=task_manager):
                                with mock.patch.object(obs_migrate, "WebConsoleServer", return_value=server):
                                    with mock.patch.object(obs_migrate, "_wait_for_web_console") as wait_for_web:
                                        with redirect_stdout(io.StringIO()):
                                            obs_migrate.main([])

        load_config.assert_called_once_with(prompt=False)
        task_manager.start.assert_not_called()
        wait_for_web.assert_called_once_with(server)
        server.stop.assert_called_once_with()

    def test_main_web_enabled_starts_console_without_auto_starting_task(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "enabled", "true")
        task_manager = mock.Mock()
        server = mock.Mock(url="http://127.0.0.1:8765/")

        with mock.patch.object(obs_migrate, "ensure_dirs"):
            with mock.patch.object(obs_migrate, "load_config", return_value=cfg):
                with mock.patch.object(obs_migrate, "validate_config"):
                    with mock.patch.object(obs_migrate, "_ensure_secret_fields_encrypted"):
                        with mock.patch.object(obs_migrate, "TaskManager", return_value=task_manager) as task_manager_cls:
                            with mock.patch.object(obs_migrate, "WebConsoleServer", return_value=server) as web_server_cls:
                                with mock.patch.object(obs_migrate, "run_migration") as run_migration:
                                    with mock.patch.object(obs_migrate, "_wait_for_web_console") as wait_for_web:
                                        with redirect_stdout(io.StringIO()):
                                            obs_migrate.main([])

        task_manager_cls.assert_called_once_with(run_migration)
        web_server_cls.assert_called_once()
        self.assertIs(web_server_cls.call_args.args[1], task_manager)
        task_manager.start.assert_not_called()
        wait_for_web.assert_called_once_with(server)
        run_migration.assert_not_called()
        task_manager.stop.assert_called_once_with()
        server.stop.assert_called_once_with()

    def test_main_web_enabled_wait_keyboard_interrupt_still_shuts_down(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "enabled", "true")
        task_manager = mock.Mock()
        server = mock.Mock(url="http://127.0.0.1:8765/")

        with mock.patch.object(obs_migrate, "ensure_dirs"):
            with mock.patch.object(obs_migrate, "load_config", return_value=cfg):
                with mock.patch.object(obs_migrate, "validate_config"):
                    with mock.patch.object(obs_migrate, "_ensure_secret_fields_encrypted"):
                        with mock.patch.object(obs_migrate, "TaskManager", return_value=task_manager):
                            with mock.patch.object(obs_migrate, "WebConsoleServer", return_value=server):
                                with mock.patch.object(obs_migrate, "run_migration") as run_migration:
                                    with mock.patch.object(obs_migrate, "_wait_for_web_console", side_effect=KeyboardInterrupt):
                                        with redirect_stdout(io.StringIO()):
                                            obs_migrate.main([])

        task_manager.start.assert_not_called()
        run_migration.assert_not_called()
        task_manager.stop.assert_called_once_with()
        server.stop.assert_called_once_with()

    def test_main_web_enabled_shutdown_stops_task_and_server_after_wait_error(self):
        cfg = make_config(require_login=False)
        cfg.set("WEB_UI", "enabled", "true")
        task_manager = mock.Mock()
        server = mock.Mock(url="http://127.0.0.1:8765/")

        with mock.patch.object(obs_migrate, "ensure_dirs"):
            with mock.patch.object(obs_migrate, "load_config", return_value=cfg):
                with mock.patch.object(obs_migrate, "validate_config"):
                    with mock.patch.object(obs_migrate, "_ensure_secret_fields_encrypted"):
                        with mock.patch.object(obs_migrate, "TaskManager", return_value=task_manager):
                            with mock.patch.object(obs_migrate, "WebConsoleServer", return_value=server):
                                with mock.patch.object(obs_migrate, "_wait_for_web_console", side_effect=RuntimeError("wait failed")):
                                    with self.assertRaisesRegex(RuntimeError, "wait failed"):
                                        with redirect_stdout(io.StringIO()):
                                            obs_migrate.main([])

        task_manager.start.assert_not_called()
        task_manager.stop.assert_called_once_with()
        server.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

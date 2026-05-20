import json
import logging
import os
import secrets
import threading
import time
from dataclasses import asdict, is_dataclass
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .object_browser import (
    BrowserPage,
    create_obs_client,
    list_local_path,
    list_remote_buckets,
    list_remote_prefix,
)
from .web_config import apply_config_payload, config_to_payload, validate_web_access


INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OBS Migration Operations Shell</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f8fb;
      --panel: #ffffff;
      --text: #172033;
      --muted: #637083;
      --line: #dbe3ef;
      --accent: #246bfe;
      --danger: #cf2e2e;
      --ok: #107c41;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", sans-serif;
    }
    header {
      padding: 22px 28px;
      background: #10213f;
      color: #fff;
    }
    header p { margin: 6px 0 0; color: #c8d4ea; }
    nav {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      padding: 12px 28px;
      background: var(--panel);
      border-bottom: 1px solid var(--line);
      position: sticky;
      top: 0;
      z-index: 2;
    }
    nav a {
      color: var(--accent);
      font-weight: 700;
      text-decoration: none;
    }
    main {
      display: grid;
      gap: 18px;
      padding: 20px 28px 84px;
    }
    section, aside {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 18px;
      box-shadow: 0 8px 24px rgba(15, 32, 60, 0.06);
    }
    h1, h2, h3 { margin: 0 0 10px; }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 14px;
    }
    label {
      display: block;
      margin: 10px 0 4px;
      color: var(--muted);
      font-size: 13px;
    }
    input, textarea, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      font: inherit;
    }
    textarea { min-height: 94px; resize: vertical; }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      background: var(--accent);
      color: #fff;
      font-weight: 700;
      cursor: pointer;
    }
    button.secondary { background: #59677c; }
    button.danger { background: var(--danger); }
    button:disabled { cursor: not-allowed; opacity: .55; }
    pre {
      overflow: auto;
      background: #0c1729;
      color: #dce8ff;
      border-radius: 10px;
      padding: 12px;
      min-height: 72px;
    }
    .status-bar {
      position: fixed;
      left: 0;
      right: 0;
      bottom: 0;
      display: flex;
      gap: 16px;
      align-items: center;
      justify-content: space-between;
      padding: 12px 28px;
      background: #10213f;
      color: #fff;
      box-shadow: 0 -8px 24px rgba(15, 32, 60, .2);
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 6px 10px;
      background: #eaf1ff;
      color: #1f4ba8;
      font-weight: 700;
    }
    .message { color: var(--muted); }
    .error { color: var(--danger); }
    .ok { color: var(--ok); }
  </style>
</head>
<body>
  <header>
    <h1>OBS Migration Operations Shell</h1>
    <p>本地静态控制台，用于配置、目录浏览、任务仪表盘与日志/报告查看。</p>
  </header>
  <nav aria-label="Operations Shell sections">
    <a href="#config">配置</a>
    <a href="#browser">目录浏览</a>
    <a href="#dashboard">任务仪表盘</a>
    <a href="#logs">日志/报告</a>
  </nav>
  <main>
    <aside id="login-panel">
      <h2>登录</h2>
      <p class="message" id="auth-message">如果 API 返回 401，请在这里登录后继续操作。</p>
      <div class="grid">
        <label>用户名
          <input id="login-username" autocomplete="username" placeholder="admin">
        </label>
        <label>密码
          <input id="login-password" type="password" autocomplete="current-password">
        </label>
      </div>
      <button id="login-button" type="button">登录</button>
    </aside>

    <section id="config">
      <h2>配置</h2>
      <p class="message">读取当前配置并可直接保存；敏感字段保持掩码时不会覆盖原值。</p>
      <button id="reload-config" type="button">重新加载配置</button>
      <button id="save-config" type="button">保存配置</button>
      <form id="config-form" class="grid" aria-label="配置编辑器"></form>
      <pre id="config-output">等待加载配置…</pre>
    </section>

    <section id="browser">
      <h2>目录浏览</h2>
      <div class="grid">
        <label>本地路径
          <input id="local-path" placeholder="例如 D:\\data 或 /data">
        </label>
        <label>远端 Section
          <select id="remote-section">
            <option value="SOURCE">SOURCE</option>
            <option value="TARGET">TARGET</option>
          </select>
        </label>
        <label>Bucket
          <input id="remote-bucket" placeholder="可选">
        </label>
        <label>Prefix
          <input id="remote-prefix" placeholder="可选">
        </label>
      </div>
      <button id="browse-local" type="button">浏览本地</button>
      <button id="browse-remote" type="button" class="secondary">浏览远端</button>
      <pre id="browser-output">目录浏览结果会显示在这里。</pre>
    </section>

    <section id="dashboard">
      <h2>任务仪表盘</h2>
      <p><span class="pill">状态 <span id="task-state">unknown</span></span></p>
      <div>
        <button type="button" data-task-action="start" data-endpoint="/api/task/start">启动</button>
        <button type="button" data-task-action="pause" data-endpoint="/api/task/pause" class="secondary">暂停</button>
        <button type="button" data-task-action="resume" data-endpoint="/api/task/resume" class="secondary">继续</button>
        <button type="button" data-task-action="stop" data-endpoint="/api/task/stop" class="danger">停止</button>
      </div>
      <pre id="task-output">正在轮询 /api/task/status …</pre>
    </section>

    <section id="logs">
      <h2>日志/报告</h2>
      <p class="message">迁移任务结束后，请在配置的 logs、state 与 check_report 目录查看详细日志和报告。</p>
      <pre id="log-output">前端不直接读取本地日志文件；请通过后端报告路径或本机文件系统查看。</pre>
    </section>
  </main>
  <div class="status-bar" role="status" aria-live="polite">
    <span id="status-text">Operations Shell 就绪</span>
    <span>API: <code>/api/task/status</code></span>
  </div>
  <script>
    const statusText = document.getElementById("status-text");
    const authMessage = document.getElementById("auth-message");
    const taskOutput = document.getElementById("task-output");
    const taskState = document.getElementById("task-state");
    const configOutput = document.getElementById("config-output");
    const configForm = document.getElementById("config-form");
    const browserOutput = document.getElementById("browser-output");

    function setStatus(message, kind) {
      statusText.textContent = message;
      statusText.className = kind || "";
    }

    async function api(path, options) {
      const response = await fetch(path, Object.assign({ credentials: "same-origin" }, options || {}));
      let data = {};
      try { data = await response.json(); } catch (_) { data = {}; }
      if (response.status === 401) {
        authMessage.textContent = "API 返回 401：请登录后重试。";
        authMessage.className = "error";
      }
      if (!response.ok) {
        const error = new Error(data.error || response.statusText || "request failed");
        error.data = data;
        throw error;
      }
      return data;
    }

    async function login() {
      try {
        await api("/api/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            username: document.getElementById("login-username").value,
            password: document.getElementById("login-password").value
          })
        });
        authMessage.textContent = "登录成功。";
        authMessage.className = "ok";
        await refreshStatus();
        await loadConfig();
      } catch (error) {
        authMessage.textContent = "登录失败：" + error.message;
        authMessage.className = "error";
      }
    }

    async function refreshStatus() {
      try {
        const data = await api("/api/task/status");
        taskOutput.textContent = JSON.stringify(data.status, null, 2);
        taskState.textContent = (data.status && data.status.state) || "unknown";
        setStatus("任务状态已更新", "ok");
      } catch (error) {
        taskOutput.textContent = "无法读取任务状态：" + error.message;
        setStatus("任务状态读取失败", "error");
      }
    }

    async function taskAction(endpoint) {
      try {
        const data = await api(endpoint, { method: "POST" });
        taskOutput.textContent = JSON.stringify(data.status || data, null, 2);
        taskState.textContent = (data.status && data.status.state) || taskState.textContent;
        setStatus("任务指令已发送: " + endpoint, "ok");
      } catch (error) {
        setStatus("任务指令失败: " + error.message, "error");
      }
    }

    async function loadConfig() {
      try {
        const data = await api("/api/config");
        renderConfigEditor(data.config);
        configOutput.textContent = JSON.stringify(data.config, null, 2);
      } catch (error) {
        configOutput.textContent = "无法加载配置：" + error.message;
      }
    }

    function renderConfigEditor(config) {
      configForm.innerHTML = "";
      Object.entries(config || {}).forEach(([section, values]) => {
        const fieldset = document.createElement("fieldset");
        fieldset.dataset.section = section;
        const legend = document.createElement("legend");
        legend.textContent = section;
        fieldset.appendChild(legend);

        Object.entries(values || {}).forEach(([key, meta]) => {
          const label = document.createElement("label");
          label.textContent = section + "." + key;
          const input = document.createElement("input");
          input.name = "config-field";
          input.dataset.section = section;
          input.dataset.key = key;
          input.dataset.sensitive = meta && meta.sensitive ? "true" : "false";
          input.value = meta && meta.value !== undefined ? meta.value : "";
          label.appendChild(input);
          fieldset.appendChild(label);
        });

        configForm.appendChild(fieldset);
      });
    }

    function collectConfigPayload() {
      const payload = {};
      configForm.querySelectorAll('[name="config-field"]').forEach(input => {
        const section = input.dataset.section;
        const key = input.dataset.key;
        if (!payload[section]) payload[section] = {};
        payload[section][key] = { value: input.value };
      });
      return payload;
    }

    async function saveConfig() {
      try {
        const data = await api("/api/config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(collectConfigPayload())
        });
        setStatus("配置已保存", "ok");
        configOutput.textContent = JSON.stringify(data, null, 2);
        await loadConfig();
      } catch (error) {
        setStatus("配置保存失败: " + error.message, "error");
        configOutput.textContent = "无法保存配置：" + error.message;
      }
    }

    async function browseLocal() {
      const path = encodeURIComponent(document.getElementById("local-path").value || ".");
      const data = await api("/api/browser/local?path=" + path + "&page_size=50");
      browserOutput.textContent = JSON.stringify(data.page, null, 2);
    }

    async function browseRemote() {
      const params = new URLSearchParams({
        section: document.getElementById("remote-section").value,
        bucket: document.getElementById("remote-bucket").value,
        prefix: document.getElementById("remote-prefix").value,
        page_size: "50"
      });
      const data = await api("/api/browser/remote?" + params.toString());
      browserOutput.textContent = JSON.stringify(data.page, null, 2);
    }

    document.getElementById("login-button").addEventListener("click", login);
    document.getElementById("reload-config").addEventListener("click", loadConfig);
    document.getElementById("save-config").addEventListener("click", saveConfig);
    document.getElementById("browse-local").addEventListener("click", () => browseLocal().catch(error => browserOutput.textContent = error.message));
    document.getElementById("browse-remote").addEventListener("click", () => browseRemote().catch(error => browserOutput.textContent = error.message));
    document.querySelectorAll("[data-task-action]").forEach(button => {
      button.addEventListener("click", () => taskAction(button.dataset.endpoint));
    });

    refreshStatus();
    loadConfig();
    setInterval(refreshStatus, 3000);
  </script>
</body>
</html>
"""


ACTIVE_TASK_STATES = {"starting", "running", "pausing", "paused", "stopping"}
SESSION_TTL_SECONDS = 12 * 60 * 60
LOGGER = logging.getLogger(__name__)


class WebConsoleServer:
    def __init__(
        self,
        cfg,
        task_manager,
        config_loader,
        config_saver,
        decrypt_secret,
        encrypt_secret,
        obs_client_factory=None,
        runtime_path_resolver=None,
    ):
        self.cfg = cfg
        self.task_manager = task_manager
        self.config_loader = config_loader
        self.config_saver = config_saver
        self.decrypt_secret = decrypt_secret or (lambda value: value)
        self.encrypt_secret = encrypt_secret or (lambda value: value)
        self.obs_client_factory = obs_client_factory
        self.runtime_path_resolver = runtime_path_resolver or _default_runtime_path_resolver
        self.sessions = {}
        self._sessions_lock = threading.Lock()
        self.session_ttl_seconds = SESSION_TTL_SECONDS
        self._thread = None
        self._validate_startup_config(cfg)

        host = cfg.get("WEB_UI", "host", fallback="127.0.0.1")
        port = cfg.getint("WEB_UI", "port", fallback=8765)
        self._httpd = ThreadingHTTPServer((host, port), self._make_handler())
        self.host = self._httpd.server_address[0]
        self.port = self._httpd.server_address[1]
        self.url = f"http://{self.host}:{self.port}/"
        if str(cfg.get("WEB_UI", "port", fallback="")).strip() == "0":
            cfg.set("WEB_UI", "port", str(self.port))

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="WebConsoleServer")
        self._thread.start()

    def stop(self):
        self._stop_task_manager()
        if self._thread is not None and self._thread.is_alive():
            self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def _stop_task_manager(self):
        stop = getattr(self.task_manager, "stop", None)
        if callable(stop):
            try:
                stop()
            except Exception:
                LOGGER.exception("Failed to stop Web console task")
        join = getattr(self.task_manager, "join", None)
        if callable(join):
            try:
                join(timeout=2)
            except Exception:
                LOGGER.exception("Failed to join Web console task")

    def _validate_startup_config(self, cfg):
        if cfg.get("WEB_UI", "port", fallback="8765").strip() != "0":
            validate_web_access(cfg)
            return

        copied = _copy_config(cfg)
        copied.set("WEB_UI", "port", "1")
        validate_web_access(copied)

    def _make_handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                server._handle(self)

            def do_POST(self):
                server._handle(self)

            def log_message(self, _format, *args):
                return

        return Handler

    def _handle(self, request):
        parsed = urlparse(request.path)
        path = parsed.path
        try:
            if request.command == "GET" and path in {"/", "/index.html"}:
                self._send_html(request, INDEX_HTML)
                return
            if path.startswith("/api/") and request.command == "POST" and not self._is_same_origin_request(request):
                self._send_json(request, {"ok": False, "error": "forbidden"}, HTTPStatus.FORBIDDEN)
                return
            if request.command == "POST" and path == "/api/login":
                self._handle_login(request)
                return
            if path.startswith("/api/") and not self._is_authorized(request):
                self._send_json(request, {"ok": False, "error": "unauthorized"}, HTTPStatus.UNAUTHORIZED)
                return

            if request.command == "GET" and path == "/api/config":
                self._send_json(request, {"ok": True, "config": self._config_payload()})
            elif request.command == "POST" and path == "/api/config":
                self._handle_save_config(request)
            elif request.command == "POST" and path == "/api/config/reload":
                self._send_json(request, {"ok": True, "config": self._config_payload()})
            elif request.command == "GET" and path == "/api/task/status":
                self._send_json(request, {"ok": True, "status": self.task_manager.snapshot()})
            elif request.command == "POST" and path.startswith("/api/task/"):
                self._handle_task_action(request, path.rsplit("/", 1)[-1])
            elif request.command == "GET" and path == "/api/browser/local":
                self._handle_local_browser(request, parsed)
            elif request.command == "GET" and path == "/api/browser/remote":
                self._handle_remote_browser(request, parsed)
            elif request.command == "POST" and path == "/api/source-list":
                self._handle_source_list(request)
            else:
                self._send_json(request, {"ok": False, "error": "not found"}, HTTPStatus.NOT_FOUND)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json(request, {"ok": False, "error": "invalid JSON"}, HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self._send_json(request, {"ok": False, "error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except Exception:
            LOGGER.exception("Unhandled Web console request error")
            self._send_json(request, {"ok": False, "error": "internal server error"}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def _handle_login(self, request):
        payload = self._read_json(request)
        cfg = self.config_loader()
        expected_username = cfg.get("WEB_UI", "username", fallback="")
        expected_password = self.decrypt_secret(cfg.get("WEB_UI", "password", fallback=""))
        username = str(payload.get("username", ""))
        password = str(payload.get("password", ""))
        if not secrets.compare_digest(username, expected_username) or not secrets.compare_digest(password, expected_password):
            self._send_json(request, {"ok": False, "error": "invalid credentials"}, HTTPStatus.UNAUTHORIZED)
            return

        token = secrets.token_urlsafe(32)
        now = time.time()
        with self._sessions_lock:
            self._cleanup_sessions_locked(now)
            self.sessions[token] = now + self.session_ttl_seconds
        headers = {"Set-Cookie": f"obs_web_session={token}; HttpOnly; SameSite=Strict; Path=/"}
        self._send_json(request, {"ok": True}, headers=headers)

    def _handle_save_config(self, request):
        payload = self._read_json(request)
        cfg = self.config_loader()
        changed = apply_config_payload(
            cfg,
            payload,
            self.encrypt_secret,
            task_running=self._task_running(),
        )
        self.config_saver(cfg)
        self._send_json(request, {"ok": True, "changed": changed})

    def _handle_task_action(self, request, action):
        if action == "start":
            result = self.task_manager.start(self.config_loader())
        elif action in {"pause", "resume", "stop"}:
            result = getattr(self.task_manager, action)()
        else:
            self._send_json(request, {"ok": False, "error": "not found"}, HTTPStatus.NOT_FOUND)
            return
        self._send_json(request, {"ok": True, "result": result, "status": self.task_manager.snapshot()})

    def _handle_local_browser(self, request, parsed):
        query = _query(parsed)
        path = _first(query, "path", "")
        page = _int_param(query, "page", 1)
        page_size = _int_param(query, "page_size", 50)
        filters = _first(query, "filter", "")
        self._send_json(request, {"ok": True, "page": _serialize_page(list_local_path(path, page, page_size, filters))})

    def _handle_remote_browser(self, request, parsed):
        query = _query(parsed)
        section = _first(query, "section", "SOURCE").upper()
        if section not in {"SOURCE", "TARGET"}:
            raise ValueError("section must be SOURCE or TARGET")
        cfg = self.config_loader()
        client = self._make_obs_client(section, cfg)
        bucket = _first(query, "bucket", "")
        page = _int_param(query, "page", 1)
        page_size = _int_param(query, "page_size", 50)
        marker = _first(query, "marker", None)
        filters = _first(query, "filter", "")

        if bucket:
            browser_page = list_remote_prefix(
                client,
                bucket,
                prefix=_first(query, "prefix", ""),
                marker=marker,
                page=page,
                page_size=page_size,
                filters=filters,
            )
        else:
            browser_page = list_remote_buckets(client, page=page, page_size=page_size)
        self._send_json(request, {"ok": True, "page": _serialize_page(browser_page)})

    def _handle_source_list(self, request):
        payload = self._read_json(request)
        items = payload.get("items", payload.get("paths", []))
        if isinstance(items, str):
            items = [items]
        if not isinstance(items, list):
            raise ValueError("items must be a list")

        cfg = self.config_loader()
        file_path = cfg.get("PATH", "migration_list_file", fallback="migration_list.txt")
        file_path = os.path.abspath(self.runtime_path_resolver(str(file_path)))
        os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

        existing = []
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as handle:
                existing = [line.rstrip("\n") for line in handle]

        seen = set(existing)
        added = []
        for item in items:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            added.append(text)

        if added:
            with open(file_path, "a", encoding="utf-8") as handle:
                needs_newline = bool(existing) and not _file_ends_with_newline(file_path)
                if needs_newline:
                    handle.write("\n")
                for item in added:
                    handle.write(f"{item}\n")

        self._send_json(request, {"ok": True, "file": file_path, "added": added})

    def _make_obs_client(self, section, cfg):
        if self.obs_client_factory is not None:
            return self._call_obs_client_factory(section, cfg)
        ak = self.decrypt_secret(cfg.get(section, "ak", fallback=""))
        sk = self.decrypt_secret(cfg.get(section, "sk", fallback=""))
        endpoint = cfg.get(section, "endpoint", fallback="")
        timeout = cfg.getint(section, "request_timeout", fallback=60)
        return create_obs_client(ak, sk, endpoint, timeout)

    def _call_obs_client_factory(self, section, cfg):
        try:
            return self.obs_client_factory(section, cfg)
        except TypeError:
            try:
                return self.obs_client_factory(cfg, section)
            except TypeError:
                try:
                    return self.obs_client_factory(section)
                except TypeError:
                    return self.obs_client_factory()

    def _is_authorized(self, request):
        cfg = self.config_loader()
        if not cfg.getboolean("WEB_UI", "require_login", fallback=True):
            return True
        cookie = SimpleCookie(request.headers.get("Cookie", ""))
        session = cookie.get("obs_web_session")
        if session is None:
            with self._sessions_lock:
                self._cleanup_sessions_locked(time.time())
            return False

        now = time.time()
        with self._sessions_lock:
            self._cleanup_sessions_locked(now)
            expiry = self.sessions.get(session.value)
            if expiry is None or expiry <= now:
                self.sessions.pop(session.value, None)
                return False
            return True

    def _is_same_origin_request(self, request):
        source = request.headers.get("Origin") or request.headers.get("Referer")
        if not source:
            return True

        parsed = urlparse(source)
        if parsed.scheme.lower() != "http" or not parsed.netloc:
            return False

        host = request.headers.get("Host") or f"{self.host}:{self.port}"
        return _normalize_netloc(parsed.netloc) == _normalize_netloc(host)

    def _cleanup_sessions_locked(self, now):
        expired = [token for token, expiry in self.sessions.items() if expiry <= now]
        for token in expired:
            self.sessions.pop(token, None)

    def _task_running(self):
        state = str((self.task_manager.snapshot() or {}).get("state", ""))
        return state in ACTIVE_TASK_STATES

    def _config_payload(self):
        return config_to_payload(self.config_loader(), self.decrypt_secret)

    def _read_json(self, request):
        length = int(request.headers.get("Content-Length", "0") or 0)
        raw = request.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def _send_html(self, request, html):
        body = html.encode("utf-8")
        request.send_response(HTTPStatus.OK)
        request.send_header("Content-Type", "text/html; charset=utf-8")
        request.send_header("Content-Length", str(len(body)))
        request.end_headers()
        request.wfile.write(body)

    def _send_json(self, request, payload, status=HTTPStatus.OK, headers=None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request.send_response(status)
        request.send_header("Content-Type", "application/json; charset=utf-8")
        request.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            request.send_header(key, value)
        request.end_headers()
        request.wfile.write(body)


def _serialize_page(page):
    if not isinstance(page, BrowserPage):
        return _serialize(page)
    return _serialize(page)


def _serialize(value):
    if is_dataclass(value):
        return {key: _serialize(item) for key, item in asdict(value).items() if key != "raw"}
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items() if key != "raw"}
    return value


def _query(parsed):
    return parse_qs(parsed.query, keep_blank_values=True)


def _first(query, key, default):
    values = query.get(key)
    if not values:
        return default
    return values[0]


def _int_param(query, key, default):
    value = _first(query, key, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be an integer")


def _file_ends_with_newline(file_path):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return True
    with open(file_path, "rb") as handle:
        handle.seek(-1, os.SEEK_END)
        return handle.read(1) == b"\n"


def _default_runtime_path_resolver(path_value):
    return os.path.abspath(os.path.expanduser(str(path_value or "")))


def _normalize_netloc(netloc):
    return str(netloc or "").strip().lower()


def _copy_config(cfg):
    copied = type(cfg)()
    for section in cfg.sections():
        copied.add_section(section)
        for key, value in cfg[section].items():
            copied.set(section, key, value)
    return copied

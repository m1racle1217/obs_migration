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
      const sections = Object.entries(config || {});
      const tabList = document.createElement("div");
      tabList.className = "config-tabs";
      tabList.setAttribute("role", "tablist");
      configForm.appendChild(tabList);
      sections.forEach(([section, values], index) => {
        const tab = document.createElement("button");
        tab.type = "button";
        tab.className = "config-tab" + (index === 0 ? " active" : "");
        tab.dataset.section = section;
        tab.setAttribute("role", "tab");
        tab.setAttribute("aria-selected", index === 0 ? "true" : "false");
        tab.setAttribute("aria-controls", "config-panel-" + section);
        tab.textContent = section;
        tab.addEventListener("click", () => selectConfigTab(section));
        tabList.appendChild(tab);

        const fieldset = document.createElement("fieldset");
        fieldset.id = "config-panel-" + section;
        fieldset.className = "config-panel";
        fieldset.dataset.section = section;
        fieldset.setAttribute("role", "tabpanel");
        if (index !== 0) fieldset.hidden = true;
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

INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OBS Migration 蓝色控制台</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #070b14;
      --panel: rgba(12, 22, 40, .82);
      --panel-strong: rgba(18, 32, 56, .92);
      --line: rgba(96, 165, 250, .24);
      --line-strong: rgba(147, 197, 253, .42);
      --text: #f8fbff;
      --soft: #dbeafe;
      --muted: #9fb3cc;
      --primary: #60a5fa;
      --primary-strong: #bfdbfe;
      --cyan: #67e8f9;
      --danger: #fb7185;
      --radius: 18px;
      --font: Inter, "Noto Sans SC", "Microsoft YaHei", sans-serif;
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      min-height: 100dvh;
      background:
        radial-gradient(circle at 16% 6%, rgba(96, 165, 250, .2), transparent 29%),
        radial-gradient(circle at 62% -10%, rgba(37, 99, 235, .34), transparent 38%),
        radial-gradient(circle at 92% 18%, rgba(129, 140, 248, .14), transparent 25%),
        linear-gradient(180deg, #0b1324 0%, #050814 100%);
      color: var(--text);
      font-family: var(--font);
      font-size: 14px;
      overflow-x: hidden;
    }
    body::before {
      content: "";
      position: fixed;
      inset: 0;
      z-index: -2;
      pointer-events: none;
      opacity: .34;
      background-image:
        linear-gradient(rgba(96, 165, 250, .15) 1px, transparent 1px),
        linear-gradient(90deg, rgba(96, 165, 250, .15) 1px, transparent 1px);
      background-size: 44px 44px;
      mask-image: linear-gradient(to bottom, #000 0, #000 60%, transparent 92%);
    }
    button, input, select, textarea { font: inherit; }
    button {
      min-height: 42px;
      border: 1px solid transparent;
      border-radius: 999px;
      padding: 0 16px;
      font-weight: 760;
      cursor: pointer;
      color: var(--text);
      background: rgba(96, 165, 250, .1);
      border-color: var(--line);
    }
    button.primary {
      background: linear-gradient(135deg, #eff6ff, #93c5fd 60%, #60a5fa);
      color: #06111f;
      box-shadow: 0 12px 34px rgba(96,165,250,.28);
    }
    button.danger {
      background: rgba(251, 113, 133, .14);
      border-color: rgba(251, 113, 133, .42);
      color: #ffe4e9;
    }
    button:disabled { opacity: .48; cursor: not-allowed; }
    input, select, textarea {
      width: 100%;
      min-height: 44px;
      border: 1px solid var(--line);
      border-radius: 13px;
      padding: 11px 13px;
      background: rgba(3, 8, 20, .58);
      color: var(--text);
      outline: none;
    }
    textarea { min-height: 92px; resize: vertical; }
    input:focus, select:focus, textarea:focus {
      border-color: rgba(147,197,253,.9);
      box-shadow: 0 0 0 3px rgba(96,165,250,.2);
    }
    label { display: grid; gap: 8px; color: var(--soft); font-weight: 700; }
    .hidden { display: none !important; }
    .login-view { min-height: 100dvh; display: grid; place-items: center; padding: 28px; position: relative; }
    .login-card {
      width: min(390px, calc(100vw - 32px));
      display: block;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: linear-gradient(180deg, rgba(18,32,56,.9), rgba(6,12,26,.88));
      box-shadow: 0 20px 64px rgba(2,6,23,.4);
      overflow: hidden;
      backdrop-filter: blur(24px);
    }
    .login-hero, .login-form { padding: 56px; }
    .login-hero { display: none; }
    .mark {
      width: 48px;
      height: 48px;
      border: 1px solid var(--line-strong);
      border-radius: 15px;
      display: grid;
      place-items: center;
      background: linear-gradient(145deg, rgba(96,165,250,.16), rgba(37,99,235,.12));
      color: var(--primary-strong);
      font-weight: 900;
    }
    .eyebrow { color: var(--cyan); font-size: 12px; font-weight: 850; letter-spacing: .12em; text-transform: uppercase; }
    .login-hero h1 { font-size: clamp(38px, 5vw, 54px); line-height: 1.04; letter-spacing: -.047em; margin: 16px 0 18px; }
    .muted { color: var(--muted); line-height: 1.75; }
    .signal { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
    .fact, .panel, .task-card, .metric-card {
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: linear-gradient(180deg, rgba(18,32,56,.82), rgba(7,15,31,.78));
      box-shadow: 0 24px 80px rgba(2,6,23,.28);
      position: relative;
      overflow: hidden;
    }
    .fact::before, .panel::before, .task-card::before, .metric-card::before {
      content: "";
      position: absolute;
      left: 0;
      right: 0;
      top: 0;
      height: 1px;
      background: linear-gradient(90deg, transparent, var(--primary), var(--cyan), transparent);
      opacity: .72;
    }
    .fact { padding: 15px; }
    .fact span, .metric-card span { display: block; color: var(--muted); font-size: 12px; }
    .fact strong, .metric-card strong { display: block; margin-top: 8px; font-size: 24px; letter-spacing: -.035em; }
    .login-form { display: flex; flex-direction: column; justify-content: center; gap: 16px; padding: 34px; }
    .login-form .status-pill { display: none; }
    .login-form h2 { font-size: 26px; letter-spacing: -.035em; margin: 0 0 6px; text-align: center; }
    .login-form .muted { margin: -4px 0 2px; text-align: center; }
    .login-form .muted:empty { display: none; }
    .app-shell { min-height: 100dvh; display: grid; grid-template-columns: 282px minmax(0, 1fr); position: relative; }
    .sidebar {
      height: 100dvh;
      position: sticky;
      top: 0;
      padding: 20px 16px;
      border-right: 1px solid var(--line);
      background: rgba(6,13,28,.78);
      backdrop-filter: blur(20px);
      display: flex;
      flex-direction: column;
      gap: 18px;
    }
    .brand { display: flex; gap: 12px; align-items: center; }
    .brand strong, .brand small { display: block; }
    .brand small { color: var(--muted); margin-top: 3px; }
    .nav { display: grid; gap: 6px; }
    .nav a {
      min-height: 42px;
      border-radius: 12px;
      padding: 11px 12px;
      color: var(--soft);
      text-decoration: none;
      font-weight: 700;
      border: 1px solid transparent;
    }
    .nav a:hover { background: rgba(96,165,250,.11); border-color: rgba(96,165,250,.22); }
    .nav a.active {
      background: linear-gradient(135deg, rgba(96,165,250,.2), rgba(37,99,235,.12));
      border-color: rgba(147,197,253,.38);
      color: var(--text);
      box-shadow: inset 0 0 0 1px rgba(96,165,250,.08), 0 10px 30px rgba(37,99,235,.14);
    }
    .main { padding: 30px 34px 92px; }
    .top { display: flex; align-items: flex-start; justify-content: space-between; gap: 18px; margin-bottom: 20px; }
    .top h1 { font-size: 40px; letter-spacing: -.045em; line-height: 1.06; margin: 12px 0 0; }
    .actions, .toolbar { display: flex; gap: 9px; align-items: center; flex-wrap: wrap; }
    .status-pill {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      border: 1px solid rgba(147,197,253,.4);
      background: rgba(37,99,235,.18);
      color: var(--primary-strong);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 12px;
      font-weight: 800;
    }
    .task-grid { display: grid; grid-template-columns: 320px minmax(0, 1fr); gap: 14px; align-items: start; }
    .task-list { display: grid; gap: 10px; }
    .task-editor {
      margin-top: 14px;
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: rgba(7,15,31,.72);
    }
    .task-editor h2 { margin: 0 0 14px; font-size: 18px; }
    .task-card { padding: 15px; cursor: pointer; }
    .task-card.selected { border-color: var(--primary-strong); box-shadow: 0 0 0 2px rgba(96,165,250,.18); }
    .task-card h3 { margin: 0 0 8px; }
    .progress-line { height: 9px; border-radius: 999px; background: rgba(96,165,250,.14); overflow: hidden; margin: 12px 0; }
    .progress-line span { display: block; height: 100%; width: 0%; background: linear-gradient(90deg, var(--primary), var(--cyan)); }
    .panel { padding: 18px; margin-bottom: 14px; }
    .panel h2 { margin: 0 0 14px; font-size: 18px; letter-spacing: -.025em; }
    .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; }
    .metric-card { padding: 16px; }
    .split { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
    .config-grid { display: block; }
    .config-tabs {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 14px;
      padding: 8px;
      border: 1px solid rgba(96,165,250,.18);
      border-radius: 16px;
      background: rgba(3,9,22,.42);
    }
    .config-tab {
      min-height: 38px;
      border-radius: 11px;
      padding: 0 14px;
      color: var(--soft);
      background: rgba(96,165,250,.07);
      border-color: rgba(96,165,250,.16);
    }
    .config-tab.active {
      color: #06111f;
      background: linear-gradient(135deg, #eff6ff, #93c5fd 60%, #60a5fa);
      box-shadow: 0 10px 26px rgba(96,165,250,.2);
    }
    .config-panel {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
    }
    .config-panel[hidden] { display: none; }
    fieldset { border: 1px solid var(--line); border-radius: 16px; padding: 14px; }
    legend { color: var(--primary-strong); font-weight: 800; }
    .config-panel legend { grid-column: 1 / -1; }
    pre, .code {
      overflow: auto;
      min-height: 100px;
      border: 1px solid rgba(96,165,250,.2);
      background: rgba(2,8,23,.76);
      border-radius: 14px;
      color: #e0f2fe;
      padding: 14px;
      font-family: ui-monospace, Consolas, monospace;
      white-space: pre-wrap;
    }
    .browser-window {
      min-height: calc(100dvh - 190px);
      display: grid;
      grid-template-rows: auto auto minmax(360px, 1fr) auto;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(4,10,24,.5);
      overflow: hidden;
    }
    .explorer-titlebar, .explorer-commandbar, .explorer-addressbar, .explorer-footer {
      display: flex;
      align-items: center;
      gap: 8px;
      border-bottom: 1px solid rgba(96,165,250,.16);
      padding: 10px 12px;
      background: rgba(9,18,35,.76);
    }
    .explorer-titlebar { justify-content: space-between; }
    .explorer-titlebar h2 { margin: 0; }
    .explorer-commandbar { flex-wrap: wrap; }
    .explorer-commandbar button {
      min-height: 34px;
      border-radius: 9px;
      padding: 0 12px;
      font-weight: 750;
    }
    .explorer-addressbar { display: grid; grid-template-columns: auto minmax(180px, 1fr) minmax(120px, 210px) auto; }
    .explorer-addressbar input, .explorer-commandbar input, .explorer-commandbar select { min-height: 36px; border-radius: 9px; }
    .explorer-commandbar select { width: auto; min-width: 140px; }
    .explorer-search { width: min(260px, 32vw); margin-left: auto; }
    .explorer-addressbar input { width: 100%; }
    .explorer-layout { display: grid; grid-template-columns: 220px minmax(0, 1fr); min-height: 0; }
    .explorer-tree {
      border-right: 1px solid rgba(96,165,250,.16);
      background: rgba(6,13,28,.52);
      padding: 10px;
      overflow: auto;
    }
    .explorer-tree h3 {
      margin: 12px 8px 8px;
      color: var(--muted);
      font-size: 12px;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .explorer-tree button {
      width: 100%;
      min-height: 34px;
      display: flex;
      align-items: center;
      justify-content: flex-start;
      border-radius: 9px;
      padding: 0 10px;
      text-align: left;
      background: transparent;
      border-color: transparent;
      color: var(--soft);
    }
    .explorer-tree button:hover, .explorer-tree button.active { background: rgba(96,165,250,.13); border-color: rgba(96,165,250,.2); }
    .explorer-main { min-width: 0; overflow: auto; background: rgba(2,8,20,.38); }
    .explorer-breadcrumbs {
      display: flex;
      align-items: center;
      gap: 5px;
      min-height: 34px;
      padding: 0 12px;
      color: var(--muted);
      border-bottom: 1px solid rgba(96,165,250,.12);
      background: rgba(3,9,22,.45);
      white-space: nowrap;
      overflow: auto;
    }
    .explorer-breadcrumbs span {
      border: 1px solid rgba(96,165,250,.18);
      border-radius: 8px;
      padding: 4px 8px;
      color: var(--soft);
      background: rgba(96,165,250,.07);
    }
    .browser-table { width: 100%; border-collapse: collapse; table-layout: fixed; }
    .browser-table th {
      position: sticky;
      top: 0;
      z-index: 1;
      padding: 9px 12px;
      border-bottom: 1px solid rgba(96,165,250,.2);
      background: rgba(10,20,38,.96);
      color: var(--muted);
      text-align: left;
      font-size: 12px;
      font-weight: 800;
    }
    .browser-table td { padding: 8px 12px; border-bottom: 1px solid rgba(96,165,250,.08); text-align: left; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .browser-table tr { cursor: default; }
    .browser-table tr:hover, .browser-table tr.selected { background: rgba(96,165,250,.14); }
    .file-name { display: flex; align-items: center; gap: 9px; }
    .file-icon {
      width: 18px;
      height: 14px;
      flex: 0 0 auto;
      border: 1px solid rgba(147,197,253,.46);
      border-radius: 3px;
      background: linear-gradient(180deg, rgba(147,197,253,.26), rgba(59,130,246,.18));
      box-shadow: inset 0 1px 0 rgba(255,255,255,.18);
    }
    .file-icon.file { height: 18px; border-radius: 4px; background: linear-gradient(180deg, rgba(226,232,240,.18), rgba(96,165,250,.1)); }
    .file-icon.bucket { border-radius: 999px; background: linear-gradient(180deg, rgba(103,232,249,.18), rgba(37,99,235,.2)); }
    .explorer-footer {
      justify-content: space-between;
      border-top: 1px solid rgba(96,165,250,.16);
      border-bottom: 0;
      color: var(--muted);
      font-size: 12px;
    }
    .explorer-details summary { cursor: pointer; color: var(--muted); margin: 10px 0; }
    .worker-list { display: grid; gap: 8px; }
    .worker-item { border: 1px solid var(--line); border-radius: 12px; padding: 10px; color: var(--soft); background: rgba(96,165,250,.07); }
    .status-bar {
      position: fixed;
      left: 282px;
      right: 0;
      bottom: 0;
      border-top: 1px solid var(--line);
      background: rgba(6,13,28,.88);
      backdrop-filter: blur(18px);
      padding: 14px 34px;
      display: flex;
      justify-content: space-between;
      color: var(--muted);
    }
    @media (max-width: 980px) {
      .login-card, .app-shell, .task-grid, .split { grid-template-columns: 1fr; }
      .login-hero { min-height: auto; border-right: 0; border-bottom: 1px solid var(--line); }
      .sidebar { position: relative; height: auto; }
      .status-bar { position: static; }
      .browser-window { min-height: 560px; }
      .explorer-addressbar, .explorer-layout { grid-template-columns: 1fr; }
      .explorer-tree { border-right: 0; border-bottom: 1px solid rgba(96,165,250,.16); }
    }
  </style>
</head>
<body>
  <section id="login-view" class="login-view">
    <div class="login-card">
      <div class="login-form">
        <h2>登录</h2>
        <p id="auth-message" class="muted"></p>
        <label>用户名 <input id="login-username" autocomplete="username" value="admin"></label>
        <label>密码 <input id="login-password" type="password" autocomplete="current-password" placeholder="默认：admin"></label>
        <button id="login-button" class="primary" type="button">登录</button>
      </div>
    </div>
  </section>

  <section id="app-shell" class="app-shell hidden">
    <aside class="sidebar">
      <div class="brand"><div class="mark">OBS</div><div><strong>OBS Migration</strong><small>Blue Operations Node</small></div></div>
      <nav class="nav" aria-label="主导航">
        <a href="#dashboard">任务仪表盘</a>
        <a href="#config">配置中心</a>
        <a href="#browser">目录浏览</a>
        <a href="#logs">日志 / 报告</a>
      </nav>
      <p class="muted">控制台已启动，但迁移核心保持待命。点击“启动任务”才会领取迁移任务。</p>
    </aside>
    <main class="main">
      <div class="top">
        <div>
          <span class="status-pill">Standby · idle</span>
          <h1>并行多任务控制台</h1>
          <p class="muted">暗蓝科技风控制面：新增任务、独立启动、独立暂停、运行中调并发。</p>
        </div>
        <div class="actions">
          <button id="refresh-tasks" type="button">刷新状态</button>
          <button id="logout-button" class="danger" type="button">退出登录</button>
        </div>
      </div>

      <section id="dashboard" class="task-grid" data-page="dashboard">
        <aside class="panel">
          <div class="toolbar">
            <button id="new-task-button" class="primary" type="button">新增任务</button>
            <button id="start-selected-task" class="primary" type="button">启动任务</button>
            <button id="pause-selected-task" type="button">暂停</button>
            <button id="resume-selected-task" type="button">继续</button>
            <button id="stop-selected-task" class="danger" type="button">停止</button>
          </div>
          <div id="task-editor" class="task-editor hidden" data-page="dashboard">
            <h2>新增任务配置</h2>
            <div class="split">
              <label>任务名 <input id="new-task-name" placeholder="例如：客户 A 迁移"></label>
              <label>源路径/Prefix <input id="new-task-source" placeholder="可由目录浏览填入"></label>
              <label>目标 Prefix <input id="new-task-target" placeholder="可选"></label>
              <label>上传线程 <input id="new-task-upload-workers" type="number" min="1" value="32"></label>
            </div>
            <button id="create-task" class="primary" type="button">保存到任务列表</button>
          </div>
          <div id="task-list" class="task-list" aria-label="任务列表"></div>
        </aside>
        <section class="panel">
          <h2>任务仪表盘</h2>
          <div class="metric-grid" id="dashboard-metrics"></div>
          <h2>活跃 Worker</h2>
          <div id="worker-list" class="worker-list"></div>
          <pre id="task-output">等待任务状态...</pre>
          <div class="split">
            <label>上传线程 <input id="concurrency-upload" type="number" min="1" value="32"></label>
            <label>检查线程 <input id="concurrency-check" type="number" min="1" value="16"></label>
            <label>扫描线程 <input id="concurrency-scan" type="number" min="1" value="4"></label>
            <label>分片并发 <input id="concurrency-multipart" type="number" min="1" value="4"></label>
          </div>
          <button id="save-concurrency" type="button">应用并发设置</button>
        </section>
      </section>

      <section id="config" class="panel" data-page="config">
        <h2>配置中心</h2>
        <div class="toolbar">
          <button id="reload-config" type="button">重新加载配置</button>
          <button id="save-config" class="primary" type="button">保存配置</button>
        </div>
        <form id="config-form" class="config-grid" aria-label="配置编辑器"></form>
        <pre id="config-output">等待加载配置...</pre>
      </section>

      <section id="browser" class="panel" data-page="browser">
        <div class="browser-window">
          <div class="explorer-titlebar">
            <h2>目录浏览</h2>
            <div class="actions">
              <button id="browser-add-list" class="primary" type="button">加入迁移列表</button>
              <button id="browser-fill-task" type="button">填入任务配置</button>
            </div>
          </div>
          <div class="explorer-commandbar">
            <button id="browser-back" type="button" title="后退">后退</button>
            <button id="browser-forward" type="button" title="前进">前进</button>
            <button id="browser-up" type="button" title="上一级">上一级</button>
            <button id="browser-refresh" type="button" title="刷新">刷新</button>
            <select id="browser-scope" aria-label="位置">
              <option value="local">本地</option>
              <option value="SOURCE">SOURCE 远端</option>
              <option value="TARGET">TARGET 远端</option>
            </select>
            <input id="browser-filter" class="explorer-search" placeholder="搜索当前文件夹">
          </div>
          <div class="explorer-addressbar">
            <span>地址</span>
            <input id="browser-path" aria-label="路径 / Prefix" placeholder="D:\\data 或 root/prefix">
            <input id="browser-bucket" aria-label="Bucket" placeholder="Bucket（远端）">
            <button id="browser-go" type="button">转到</button>
          </div>
          <div class="explorer-layout">
            <aside class="explorer-tree" aria-label="资源位置">
              <h3>快速访问</h3>
              <button type="button" data-browser-scope="local" data-browser-path=".">本地文件</button>
              <button type="button" data-browser-scope="local" data-browser-path="..">上级目录</button>
              <h3>对象存储</h3>
              <button type="button" data-browser-scope="SOURCE" data-browser-path="">SOURCE 远端</button>
              <button type="button" data-browser-scope="TARGET" data-browser-path="">TARGET 远端</button>
            </aside>
            <div class="explorer-main">
              <div id="browser-breadcrumbs" class="explorer-breadcrumbs" aria-label="当前位置"></div>
              <table id="browser-table" class="browser-table">
                <thead><tr><th>名称</th><th>修改时间 / etag</th><th>类型</th><th>大小</th></tr></thead>
                <tbody id="browser-body"></tbody>
              </table>
            </div>
          </div>
          <div class="explorer-footer">
            <span id="browser-status">等待浏览...</span>
            <span id="browser-selected">未选择项目</span>
          </div>
        </div>
        <details class="explorer-details">
          <summary>原始响应</summary>
          <pre id="browser-output">等待浏览...</pre>
        </details>
      </section>

      <section id="logs" class="panel" data-page="logs">
        <h2>日志 / 报告</h2>
        <p class="muted">迁移任务结束后，请在 logs、state 和 check_report 目录查看详细日志与报告。</p>
      </section>
    </main>
    <div class="status-bar"><span id="status-text">Operations Shell 就绪</span><span>API /api/tasks · /api/task/status · /api/task/start · /api/task/pause · /api/task/resume · /api/task/stop</span></div>
  </section>

  <script>
    const AUTH_KEY = "obsWebConsole.authenticated";
    const statusText = document.getElementById("status-text");
    const authMessage = document.getElementById("auth-message");
    const NAV_PAGES = new Set(["dashboard", "config", "browser", "logs"]);
    const PAGE_TITLES = {
      dashboard: "任务仪表盘",
      config: "配置中心",
      browser: "目录浏览",
      logs: "日志 / 报告"
    };
    const taskList = document.getElementById("task-list");
    const taskOutput = document.getElementById("task-output");
    const dashboardMetrics = document.getElementById("dashboard-metrics");
    const workerList = document.getElementById("worker-list");
    const configForm = document.getElementById("config-form");
    const configOutput = document.getElementById("config-output");
    const browserOutput = document.getElementById("browser-output");
    const browserBody = document.getElementById("browser-body");
    const browserStatus = document.getElementById("browser-status");
    const browserSelected = document.getElementById("browser-selected");
    const browserBreadcrumbs = document.getElementById("browser-breadcrumbs");
    let selectedTaskId = null;
    let selectedBrowserItem = null;
    let browserHistory = [];
    let browserForward = [];

    function showLogin(message) {
      localStorage.removeItem(AUTH_KEY);
      document.getElementById("login-view").classList.remove("hidden");
      document.getElementById("app-shell").classList.add("hidden");
      if (message) authMessage.textContent = message;
    }
    function showApp() {
      document.getElementById("login-view").classList.add("hidden");
      document.getElementById("app-shell").classList.remove("hidden");
      showPage();
    }
    function setStatus(message) { if (statusText) statusText.textContent = message; }
    function currentPage() {
      const page = (window.location.hash || "#dashboard").slice(1);
      return NAV_PAGES.has(page) ? page : "dashboard";
    }
    function showPage(page = currentPage()) {
      document.querySelectorAll("[data-page]").forEach(section => {
        section.style.display = section.dataset.page === page ? "" : "none";
      });
      document.querySelectorAll(".nav a[href^='#']").forEach(link => {
        const active = link.getAttribute("href") === "#" + page;
        link.classList.toggle("active", active);
        if (active) {
          link.setAttribute("aria-current", "page");
        } else {
          link.removeAttribute("aria-current");
        }
      });
      if (page !== "dashboard") document.getElementById("task-editor").classList.add("hidden");
      setStatus((PAGE_TITLES[page] || "页面") + " 已打开");
    }
    async function api(path, options) {
      const response = await fetch(path, Object.assign({ credentials: "same-origin" }, options || {}));
      let data = {};
      try { data = await response.json(); } catch (_) { data = {}; }
      if (response.status === 401) showLogin("登录已过期，请重新登录。");
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
        localStorage.setItem(AUTH_KEY, "true");
        showApp();
        await bootApp();
      } catch (error) {
        authMessage.textContent = "登录失败：用户名或密码错误。默认是 admin / admin；如果已修改，请查看 config.ini 的 [WEB_UI]。";
      }
    }
    async function logout() {
      try { await api("/api/logout", { method: "POST" }); } catch (_) {}
      showLogin("已注销，请重新登录。");
    }
    function pct(value) { return Math.max(0, Math.min(Number(value || 0), 100)); }
    function bytes(value) {
      let size = Math.max(Number(value || 0), 0);
      const units = ["B", "KB", "MB", "GB", "TB"];
      for (const unit of units) {
        if (size < 1024 || unit === units[units.length - 1]) return unit === "B" ? size.toFixed(0) + unit : size.toFixed(1) + unit;
        size /= 1024;
      }
    }
    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, char => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[char]));
    }
    function browserKindLabel(kind) {
      if (kind === "bucket") return "存储桶";
      if (kind === "dir") return "文件夹";
      return "文件";
    }
    function browserIconClass(kind) {
      if (kind === "bucket") return "bucket";
      if (kind === "file") return "file";
      return "folder";
    }
    function browserDisplaySize(item) {
      if (!item || item.kind !== "file") return "";
      return bytes(item.size || 0);
    }
    function eta(seconds) {
      if (seconds === null || seconds === undefined || seconds < 0) return "--:--:--";
      seconds = Math.floor(seconds);
      const h = String(Math.floor(seconds / 3600)).padStart(2, "0");
      const m = String(Math.floor((seconds % 3600) / 60)).padStart(2, "0");
      const s = String(seconds % 60).padStart(2, "0");
      return h + ":" + m + ":" + s;
    }
    async function loadTasks() {
      const data = await api("/api/tasks");
      renderTasks(data.tasks || []);
      if (!selectedTaskId && data.tasks && data.tasks[0]) selectedTaskId = data.tasks[0].task_id;
      if (selectedTaskId) await loadTask(selectedTaskId);
    }
    function renderTasks(tasks) {
      taskList.innerHTML = "";
      tasks.forEach(task => {
        const percent = pct(task.dashboard && task.dashboard.percent);
        const card = document.createElement("article");
        card.className = "task-card" + (task.task_id === selectedTaskId ? " selected" : "");
        card.innerHTML = `<h3>${task.name || task.task_id}</h3><p class="muted">${task.source || "未设置源"} → ${task.target || "未设置目标"}</p><div class="progress-line"><span style="width:${percent}%"></span></div><p>${task.state} · ${percent.toFixed(1)}% · 错误 ${(task.dashboard && task.dashboard.upload_errors) || 0}</p>`;
        card.addEventListener("click", () => { selectedTaskId = task.task_id; loadTask(task.task_id); });
        taskList.appendChild(card);
      });
    }
    async function loadTask(taskId) {
      const data = await api("/api/tasks/" + taskId);
      renderTask(data.task);
    }
    function renderTask(task) {
      const d = task.dashboard || {};
      const metrics = [
        ["总进度", pct(d.percent).toFixed(1) + "%"],
        ["已处理大小", bytes(d.done_bytes) + " / " + bytes(d.total_bytes)],
        ["ETA", eta(d.eta_seconds)],
        ["完成文件", d.files_done || 0],
        ["上传跳过", d.upload_skip || 0],
        ["扫描跳过", d.scan_skip || 0],
        ["索引状态", d.index_status || "unknown"],
        ["扫描状态", d.scan_status || "unknown"],
        ["检查状态", d.check_status || "unknown"],
        ["上传状态", d.upload_status || "unknown"],
        ["缓存命中", (d.cache_hit || 0) + "/" + (d.cache_total || 0)],
        ["命中率", Number(d.hit_rate || 0).toFixed(1) + "%"],
        ["扫描文件", d.scan_files || 0],
        ["扫描速度", Number(d.scan_speed || 0).toFixed(1) + " 文件/s"],
        ["扫描错误", d.scan_errors || 0],
        ["上传错误", d.upload_errors || 0],
        ["累计处理速度", bytes(d.process_speed) + "/s"],
        ["实时上传速度", bytes(d.net_upload_speed) + "/s"],
        ["检查队列", JSON.stringify(d.check_queue || {})],
        ["传输队列", JSON.stringify(d.transfer_queue || {})],
        ["检查线程", JSON.stringify(d.check_workers || {})],
        ["上传线程", JSON.stringify(d.upload_workers || {})],
        ["扫描线程", JSON.stringify(d.scan_workers || {})],
      ];
      dashboardMetrics.innerHTML = metrics.map(([k, v]) => `<div class="metric-card"><span>${k}</span><strong>${v}</strong></div>`).join("");
      workerList.innerHTML = (d.active_workers || []).map(w => `<div class="worker-item">${w.stage || ""} ${w.worker_name || ""} ${w.detail || ""}<br>${w.task_summary || ""}</div>`).join("") || "<p class='muted'>暂无活跃 Worker</p>";
      taskOutput.textContent = JSON.stringify(task, null, 2);
      renderConcurrency(task.concurrency || {});
      setStatus("任务状态已更新");
    }
    function renderConcurrency(concurrency) {
      document.getElementById("concurrency-upload").value = concurrency.upload_workers || 32;
      document.getElementById("concurrency-check").value = concurrency.check_workers || 16;
      document.getElementById("concurrency-scan").value = concurrency.scan_workers || 4;
      document.getElementById("concurrency-multipart").value = concurrency.multipart_concurrency || 4;
    }
    function showNoTaskSelected() {
      const message = "请先点击“新增任务”创建任务，或从任务列表选择一个任务。";
      setStatus(message);
      if (taskOutput) taskOutput.textContent = message;
    }
    async function taskAction(action) {
      if (!selectedTaskId) {
        showNoTaskSelected();
        return;
      }
      const data = await api(`/api/tasks/${selectedTaskId}/${action}`, { method: "POST" });
      renderTask(data.task || data.status);
      await loadTasks();
    }
    async function saveConcurrency() {
      if (!selectedTaskId) return;
      const data = await api(`/api/tasks/${selectedTaskId}/concurrency`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          upload_workers: document.getElementById("concurrency-upload").value,
          check_workers: document.getElementById("concurrency-check").value,
          scan_workers: document.getElementById("concurrency-scan").value,
          multipart_concurrency: document.getElementById("concurrency-multipart").value
        })
      });
      renderTask(data.task);
    }
    async function createTask() {
      const source = document.getElementById("new-task-source").value;
      const target = document.getElementById("new-task-target").value;
      const data = await api("/api/tasks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: document.getElementById("new-task-name").value || "迁移任务",
          config: {
            SOURCE: { path: { value: source }, prefix: { value: source } },
            TARGET: { prefix: { value: target } }
          },
          concurrency: { upload_workers: document.getElementById("new-task-upload-workers").value }
        })
      });
      selectedTaskId = data.task_id;
      document.getElementById("task-editor").classList.add("hidden");
      await loadTasks();
    }
    async function loadConfig() {
      const data = await api("/api/config");
      renderConfigEditor(data.config);
      configOutput.textContent = JSON.stringify(data.config, null, 2);
    }
    function renderConfigEditor(config) {
      configForm.innerHTML = "";
      const sections = Object.entries(config || {});
      const tabList = document.createElement("div");
      tabList.className = "config-tabs";
      tabList.setAttribute("role", "tablist");
      configForm.appendChild(tabList);
      sections.forEach(([section, values], index) => {
        const tab = document.createElement("button");
        tab.type = "button";
        tab.className = "config-tab" + (index === 0 ? " active" : "");
        tab.dataset.section = section;
        tab.setAttribute("role", "tab");
        tab.setAttribute("aria-selected", index === 0 ? "true" : "false");
        tab.setAttribute("aria-controls", "config-panel-" + section);
        tab.textContent = section;
        tab.addEventListener("click", () => selectConfigTab(section));
        tabList.appendChild(tab);

        const fieldset = document.createElement("fieldset");
        fieldset.id = "config-panel-" + section;
        fieldset.className = "config-panel";
        fieldset.dataset.section = section;
        fieldset.setAttribute("role", "tabpanel");
        if (index !== 0) fieldset.hidden = true;
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
          input.value = meta && meta.value !== undefined ? meta.value : "";
          label.appendChild(input);
          fieldset.appendChild(label);
        });
        configForm.appendChild(fieldset);
      });
    }
    function selectConfigTab(section) {
      configForm.querySelectorAll(".config-tab").forEach(tab => {
        const active = tab.dataset.section === section;
        tab.classList.toggle("active", active);
        tab.setAttribute("aria-selected", active ? "true" : "false");
      });
      configForm.querySelectorAll(".config-panel").forEach(panel => {
        panel.hidden = panel.dataset.section !== section;
      });
      setStatus(section + " 配置已打开");
    }
    function collectConfigPayload() {
      const payload = {};
      configForm.querySelectorAll('[name="config-field"]').forEach(input => {
        payload[input.dataset.section] = payload[input.dataset.section] || {};
        payload[input.dataset.section][input.dataset.key] = { value: input.value };
      });
      return payload;
    }
    async function saveConfig() {
      const data = await api("/api/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(collectConfigPayload())
      });
      configOutput.textContent = JSON.stringify(data, null, 2);
      await loadConfig();
    }
    function browserLocation() {
      return {
        scope: document.getElementById("browser-scope").value,
        path: document.getElementById("browser-path").value || ".",
        bucket: document.getElementById("browser-bucket").value,
        filter: document.getElementById("browser-filter").value
      };
    }
    async function browse(pushHistory = true) {
      const loc = browserLocation();
      if (pushHistory) {
        browserHistory.push(Object.assign({}, loc));
        browserForward = [];
      }
      browserStatus.textContent = "正在加载...";
      const params = new URLSearchParams({ page_size: "100", filter: loc.filter || "" });
      let data;
      if (loc.scope === "local") {
        params.set("path", loc.path || ".");
        data = await api("/api/browser/local?" + params.toString());
      } else {
        params.set("section", loc.scope);
        params.set("bucket", loc.bucket || "");
        params.set("prefix", loc.path || "");
        data = await api("/api/browser/remote?" + params.toString());
      }
      renderBrowser(data.page);
    }
    function renderBreadcrumbs(page) {
      const scope = document.getElementById("browser-scope").value;
      const bucket = page.bucket || document.getElementById("browser-bucket").value || "";
      const rawPath = page.prefix !== undefined ? page.prefix : (page.path || document.getElementById("browser-path").value || "");
      const parts = [];
      parts.push(scope === "local" ? "此电脑" : scope);
      if (bucket) parts.push(bucket);
      String(rawPath || "").replace(/\\/g, "/").split("/").filter(Boolean).forEach(part => parts.push(part));
      browserBreadcrumbs.innerHTML = parts.map(part => `<span>${escapeHtml(part)}</span>`).join("<b>›</b>");
    }
    function syncBrowserTree(scope) {
      document.querySelectorAll("[data-browser-scope]").forEach(button => {
        button.classList.toggle("active", button.dataset.browserScope === scope);
      });
    }
    function renderBrowser(page) {
      selectedBrowserItem = null;
      browserSelected.textContent = "未选择项目";
      browserOutput.textContent = JSON.stringify(page, null, 2);
      if (page.path !== undefined) document.getElementById("browser-path").value = page.path || "";
      if (page.bucket !== undefined) document.getElementById("browser-bucket").value = page.bucket || "";
      if (page.prefix !== undefined) document.getElementById("browser-path").value = page.prefix || "";
      renderBreadcrumbs(page);
      syncBrowserTree(document.getElementById("browser-scope").value);
      browserBody.innerHTML = "";
      const items = (page.items || []).slice().sort((left, right) => {
        const leftRank = left.kind === "file" ? 1 : 0;
        const rightRank = right.kind === "file" ? 1 : 0;
        if (leftRank !== rightRank) return leftRank - rightRank;
        return String(left.name || "").localeCompare(String(right.name || ""), "zh-CN");
      });
      browserStatus.textContent = `${items.length} 个项目`;
      items.forEach(item => {
        const tr = document.createElement("tr");
        const iconClass = browserIconClass(item.kind);
        tr.innerHTML = `<td><span class="file-name"><span class="file-icon ${iconClass}"></span>${escapeHtml(item.name || "")}</span></td><td>${escapeHtml(item.mtime || item.etag || "")}</td><td>${browserKindLabel(item.kind)}</td><td>${browserDisplaySize(item)}</td>`;
        tr.addEventListener("click", () => {
          selectedBrowserItem = item;
          browserBody.querySelectorAll("tr").forEach(row => row.classList.remove("selected"));
          tr.classList.add("selected");
          browserSelected.textContent = `已选择：${item.name || ""}`;
        });
        tr.addEventListener("dblclick", () => enterBrowserItem(item));
        browserBody.appendChild(tr);
      });
    }
    function enterBrowserItem(item) {
      if (!item || item.kind === "file") return;
      if (item.kind === "bucket") document.getElementById("browser-bucket").value = item.name;
      document.getElementById("browser-path").value = item.path || item.name || "";
      browserForward = [];
      browse(true).catch(error => browserOutput.textContent = error.message);
    }
    function restoreBrowserLocation(loc) {
      document.getElementById("browser-scope").value = loc.scope;
      document.getElementById("browser-path").value = loc.path;
      document.getElementById("browser-bucket").value = loc.bucket;
      document.getElementById("browser-filter").value = loc.filter;
      browse(false).catch(error => browserOutput.textContent = error.message);
    }
    function browserUp() {
      const pathInput = document.getElementById("browser-path");
      const text = pathInput.value.replace(/\\/g, "/").replace(/\/$/, "");
      pathInput.value = text.includes("/") ? text.split("/").slice(0, -1).join("/") : "";
      browse(true).catch(error => browserOutput.textContent = error.message);
    }
    async function addSelectedToList() {
      if (!selectedBrowserItem) {
        const message = "请先在文件列表中单击选择一个目录或对象。";
        browserSelected.textContent = message;
        browserOutput.textContent = message;
        setStatus(message);
        return;
      }
      const selectedPath = selectedBrowserItem.path || selectedBrowserItem.name || "";
      if (!selectedPath) {
        const message = "选中项目缺少可加入迁移列表的路径。";
        browserSelected.textContent = message;
        browserOutput.textContent = message;
        setStatus(message);
        return;
      }
      const data = await api("/api/source-list", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items: [selectedPath] })
      });
      browserSelected.textContent = "已加入迁移列表：" + selectedPath;
      setStatus("已加入迁移列表");
      browserOutput.textContent = JSON.stringify(data, null, 2);
    }
    function fillSelectedTaskConfig() {
      if (!selectedBrowserItem) return;
      window.location.hash = "#dashboard";
      showPage("dashboard");
      document.getElementById("task-editor").classList.remove("hidden");
      document.getElementById("task-editor").scrollIntoView({ behavior: "smooth", block: "start" });
      document.getElementById("new-task-source").value = selectedBrowserItem.path || selectedBrowserItem.name || "";
    }
    async function bootApp() {
      showApp();
      await Promise.all([loadTasks(), loadConfig()]);
      browse(false).catch(() => {});
      window.setInterval(() => loadTasks().catch(() => {}), 3000);
    }
    document.getElementById("login-button").addEventListener("click", login);
    document.getElementById("logout-button").addEventListener("click", logout);
    document.getElementById("refresh-tasks").addEventListener("click", () => loadTasks().catch(error => setStatus(error.message)));
    document.getElementById("new-task-button").addEventListener("click", () => {
      const editor = document.getElementById("task-editor");
      const hidden = editor.classList.toggle("hidden");
      if (!hidden) {
        setStatus("新增任务配置已打开");
        editor.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
    });
    document.getElementById("create-task").addEventListener("click", () => createTask().catch(error => setStatus(error.message)));
    document.getElementById("start-selected-task").addEventListener("click", () => taskAction("start").catch(error => setStatus(error.message)));
    document.getElementById("pause-selected-task").addEventListener("click", () => taskAction("pause").catch(error => setStatus(error.message)));
    document.getElementById("resume-selected-task").addEventListener("click", () => taskAction("resume").catch(error => setStatus(error.message)));
    document.getElementById("stop-selected-task").addEventListener("click", () => taskAction("stop").catch(error => setStatus(error.message)));
    document.getElementById("save-concurrency").addEventListener("click", () => saveConcurrency().catch(error => setStatus(error.message)));
    document.getElementById("reload-config").addEventListener("click", () => loadConfig().catch(error => configOutput.textContent = error.message));
    document.getElementById("save-config").addEventListener("click", () => saveConfig().catch(error => configOutput.textContent = error.message));
    document.getElementById("browser-refresh").addEventListener("click", () => browse(true).catch(error => browserOutput.textContent = error.message));
    document.getElementById("browser-go").addEventListener("click", () => browse(true).catch(error => browserOutput.textContent = error.message));
    document.getElementById("browser-scope").addEventListener("change", () => browse(true).catch(error => browserOutput.textContent = error.message));
    document.getElementById("browser-filter").addEventListener("keydown", event => {
      if (event.key === "Enter") browse(true).catch(error => browserOutput.textContent = error.message);
    });
    document.getElementById("browser-up").addEventListener("click", browserUp);
    document.getElementById("browser-back").addEventListener("click", () => { if (browserHistory.length > 1) { browserForward.push(browserHistory.pop()); restoreBrowserLocation(browserHistory[browserHistory.length - 1]); } });
    document.getElementById("browser-forward").addEventListener("click", () => { const loc = browserForward.pop(); if (loc) { browserHistory.push(loc); restoreBrowserLocation(loc); } });
    document.getElementById("browser-add-list").addEventListener("click", () => addSelectedToList().catch(error => browserOutput.textContent = error.message));
    document.getElementById("browser-fill-task").addEventListener("click", fillSelectedTaskConfig);
    document.querySelectorAll("[data-browser-scope]").forEach(button => {
      button.addEventListener("click", () => {
        document.getElementById("browser-scope").value = button.dataset.browserScope;
        document.getElementById("browser-path").value = button.dataset.browserPath || "";
        if (button.dataset.browserScope === "local") document.getElementById("browser-bucket").value = "";
        browse(true).catch(error => browserOutput.textContent = error.message);
      });
    });
    window.addEventListener("hashchange", () => showPage());
    if (localStorage.getItem(AUTH_KEY)) {
      bootApp().catch(() => showLogin("登录已过期，请重新登录。"));
    } else {
      showLogin();
    }
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
        stop_all = getattr(self.task_manager, "stop_all", None)
        if callable(stop_all):
            try:
                stop_all()
            except Exception:
                LOGGER.exception("Failed to stop Web console tasks")
            join_all = getattr(self.task_manager, "join_all", None)
            if callable(join_all):
                try:
                    join_all(timeout=2)
                except Exception:
                    LOGGER.exception("Failed to join Web console tasks")
            return
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

            def do_PATCH(self):
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
            if path.startswith("/api/") and request.command in {"POST", "PATCH"} and not self._is_same_origin_request(request):
                self._send_json(request, {"ok": False, "error": "forbidden"}, HTTPStatus.FORBIDDEN)
                return
            if request.command == "POST" and path == "/api/login":
                self._handle_login(request)
                return
            if request.command == "POST" and path == "/api/logout":
                self._handle_logout(request)
                return
            if path.startswith("/api/") and not self._is_authorized(request):
                self._send_json(request, {"ok": False, "error": "unauthorized"}, HTTPStatus.UNAUTHORIZED)
                return

            task_route = self._match_task_route(path)
            if request.command == "GET" and path == "/api/tasks":
                self._handle_list_tasks(request)
            elif request.command == "POST" and path == "/api/tasks":
                self._handle_create_task(request)
            elif task_route and request.command == "GET":
                self._handle_get_task(request, task_route[0])
            elif task_route and request.command == "PATCH" and task_route[1] == "concurrency":
                self._handle_task_concurrency(request, task_route[0])
            elif task_route and request.command == "POST":
                self._handle_task_action(request, task_route[1], task_id=task_route[0])
            elif request.command == "GET" and path == "/api/config":
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
        headers = {"Set-Cookie": f"obs_web_session={token}; Max-Age={self.session_ttl_seconds}; HttpOnly; SameSite=Strict; Path=/"}
        self._send_json(request, {"ok": True, "expires_in": self.session_ttl_seconds}, headers=headers)

    def _handle_logout(self, request):
        cookie = SimpleCookie(request.headers.get("Cookie", ""))
        session = cookie.get("obs_web_session")
        if session is not None:
            with self._sessions_lock:
                self.sessions.pop(session.value, None)
        headers = {"Set-Cookie": "obs_web_session=; Max-Age=0; HttpOnly; SameSite=Strict; Path=/"}
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

    def _handle_list_tasks(self, request):
        if hasattr(self.task_manager, "list_tasks"):
            self._send_json(request, {"ok": True, "tasks": self.task_manager.list_tasks()})
            return
        self._send_json(request, {"ok": True, "tasks": [self.task_manager.snapshot()]})

    def _handle_create_task(self, request):
        if not hasattr(self.task_manager, "create_task"):
            self._send_json(request, {"ok": False, "error": "multi task manager is unavailable"}, HTTPStatus.BAD_REQUEST)
            return
        payload = self._read_json(request)
        cfg = _copy_config(self.config_loader())
        _apply_task_config_overlay(cfg, payload.get("config", {}))
        concurrency = payload.get("concurrency", {})
        _apply_task_concurrency(cfg, concurrency)
        task_id = self.task_manager.create_task(cfg, name=str(payload.get("name") or "迁移任务"))
        if concurrency and hasattr(self.task_manager, "update_concurrency"):
            self.task_manager.update_concurrency(task_id, concurrency)
        self._send_json(request, {"ok": True, "task_id": task_id, "task": self.task_manager.snapshot(task_id)})

    def _handle_get_task(self, request, task_id):
        try:
            self._send_json(request, {"ok": True, "task": self.task_manager.snapshot(task_id)})
        except KeyError:
            self._send_json(request, {"ok": False, "error": "task not found"}, HTTPStatus.NOT_FOUND)

    def _handle_task_concurrency(self, request, task_id):
        if not hasattr(self.task_manager, "update_concurrency"):
            self._send_json(request, {"ok": False, "error": "concurrency update is unavailable"}, HTTPStatus.BAD_REQUEST)
            return
        try:
            task = self.task_manager.update_concurrency(task_id, self._read_json(request))
        except KeyError:
            self._send_json(request, {"ok": False, "error": "task not found"}, HTTPStatus.NOT_FOUND)
            return
        self._send_json(request, {"ok": True, "task": task})

    def _handle_task_action(self, request, action, task_id=None):
        if action == "start":
            result = self.task_manager.start(task_id) if task_id else self.task_manager.start(self.config_loader())
        elif action in {"pause", "resume", "stop"}:
            try:
                result = getattr(self.task_manager, action)(task_id) if task_id else getattr(self.task_manager, action)()
            except TypeError:
                result = getattr(self.task_manager, action)()
        else:
            self._send_json(request, {"ok": False, "error": "not found"}, HTTPStatus.NOT_FOUND)
            return
        try:
            status = self.task_manager.snapshot(task_id) if task_id else self.task_manager.snapshot()
        except TypeError:
            status = self.task_manager.snapshot()
        self._send_json(request, {"ok": True, "result": result, "status": status, "task": status})

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

    def _match_task_route(self, path):
        parts = [part for part in str(path or "").split("/") if part]
        if len(parts) == 3 and parts[0] == "api" and parts[1] == "tasks":
            return parts[2], ""
        if len(parts) == 4 and parts[0] == "api" and parts[1] == "tasks":
            return parts[2], parts[3]
        return None

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


def _apply_task_config_overlay(cfg, payload):
    if not isinstance(payload, dict):
        raise ValueError("config must be an object")
    for section, values in payload.items():
        if not isinstance(values, dict):
            raise ValueError("config section must be an object")
        section = str(section)
        if not cfg.has_section(section):
            cfg.add_section(section)
        for key, meta in values.items():
            value = meta.get("value") if isinstance(meta, dict) else meta
            cfg.set(section, str(key), str(value))


def _apply_task_concurrency(cfg, concurrency):
    if not concurrency:
        return
    if not isinstance(concurrency, dict):
        raise ValueError("concurrency must be an object")
    mapping = {
        "upload_workers": ("UPLOAD", "workers"),
        "check_workers": ("UPLOAD", "checkers"),
        "scan_workers": ("SCAN", "scan_workers"),
        "multipart_concurrency": ("UPLOAD", "multipart_concurrency"),
        "max_connections": ("UPLOAD", "max_connections"),
    }
    for key, value in concurrency.items():
        if key not in mapping:
            continue
        try:
            clean = max(1, int(value))
        except (TypeError, ValueError):
            raise ValueError(f"{key} must be an integer")
        section, option = mapping[key]
        if not cfg.has_section(section):
            cfg.add_section(section)
        cfg.set(section, option, str(clean))

# Web Console Design

## Context

The migration tool currently starts from `obs_migrate.py` and provides an interactive CLI configuration menu, local and OBS/S3 browsing helpers, Rich dashboard rendering, checkpointing, and reports. The new Web console must preserve the existing CLI workflow while adding a browser-based control surface for configuration, browsing, task control, and monitoring.

The project favors offline deployment and a small dependency set, so the first implementation should use Python standard library HTTP primitives instead of adding Flask, FastAPI, or a frontend build system.

## Goals

- Keep the CLI available.
- Add a `[WEB_UI]` configuration section that can enable the Web console.
- Start the Web console when `[WEB_UI] enabled = true` or the `--web` command-line flag requests it.
- Print the Web console URL when it starts.
- Provide online configuration editing, saving, and safe reload behavior.
- Support local and OBS/S3 directory browsing.
- Provide task dashboard and task controls: start, pause, resume, stop.
- Keep a migration task running when the browser tab is closed, as long as the tool process remains alive.
- Support local-only and externally reachable deployments with login protection.

## Non-Goals

- No independent daemon that survives the tool process exiting.
- No hard interruption of in-flight object storage requests.
- No multi-user permission model in the first version.
- No WebSocket requirement in the first version.
- No new web framework dependency in the first version.

## Configuration

Add this default section to `DEFAULT_CONFIG`, `config.example.ini`, and generated configs:

```ini
[WEB_UI]
enabled = false
host = 127.0.0.1
port = 8765
require_login = true
username = admin
password = admin
auto_open = false
```

Rules:

- Existing CLI behavior remains the default.
- When `enabled = true`, the tool starts an embedded Web server and prints a URL such as `http://127.0.0.1:8765`.
- The `--web` command-line option also enables the Web console for one run.
- If `auto_open = true`, the tool attempts to open the browser. Failure to open the browser does not stop the server.
- If `host` is not a loopback host, `require_login` must be true. Startup fails with a clear error if external access is configured without login.
- `username` defaults to `admin`.
- `password` can start as plaintext for initial setup. Saving or changing it through the Web UI writes it back encrypted using the same config encryption mechanism used for sensitive storage fields.

## Web Structure

Use an Operations Shell layout:

- Fixed navigation for Config, Browser, Dashboard, and Logs/Reports.
- A top status bar with task state, source summary, target summary, current user, and Web address.
- Dense operational screens instead of a landing page or wizard.

Sections:

- Config: grouped editor for `SOURCE`, `TARGET`, `UPLOAD`, `SCAN`, `CHECK`, `PATH`, `UI`, and `WEB_UI`.
- Browser: local directory browsing and OBS/S3 bucket, prefix, and object browsing.
- Dashboard: task controls, progress, rates, queues, errors, and worker states.
- Logs/Reports: report files and recent error summaries in the first version.

The frontend should be plain HTML/CSS/JavaScript served by the Python process. No frontend build step is required.

## API Surface

Initial endpoints:

- `GET /api/config`
- `POST /api/config`
- `POST /api/config/reload`
- `GET /api/browser/local`
- `GET /api/browser/remote`
- `POST /api/source-list`
- `GET /api/task/status`
- `POST /api/task/start`
- `POST /api/task/pause`
- `POST /api/task/resume`
- `POST /api/task/stop`

The dashboard can use polling against `GET /api/task/status`. WebSocket support is not required for the first version.

## Task Manager

Add a process-local `TaskManager` that owns the current migration task and exposes state to both Web APIs and CLI reuse.

Task states:

- `idle`
- `starting`
- `running`
- `pausing`
- `paused`
- `stopping`
- `stopped`
- `failed`
- `completed`

Behavior:

- Starting a task launches migration work in a background thread inside the current process.
- Only one migration task may run at a time.
- Closing the browser does not stop the task.
- Exiting the tool process stops the Web server and task.
- Stop is graceful: stop scanning, stop claiming new queued work, let in-flight work reach a safe point, then close checkpoint and reports.

Pause and resume:

- Pause prevents scanner, checker, and uploader workers from claiming new work.
- In-flight work continues until it reaches a safe point.
- Large object transfers are not hard-killed.
- Resume releases the pause gate and lets queues continue.
- If the process exits and starts again, checkpoint behavior handles already completed work and remaining work is revalidated.

## Runtime Refactor

Refactor the current one-shot `main()` flow into a callable migration runner such as:

```python
run_migration(cfg, controls)
```

`controls` should provide:

- pause gate
- stop request flag
- task status storage
- optional hooks for status updates

Existing CLI startup can continue to call the same migration runner after interactive configuration. The Web `TaskManager` calls the runner from a background thread.

## Safe Reload

The Web config editor supports saving and reloading configuration.

When no task is running:

- All supported configuration fields may be edited and reloaded.

When a task is running:

- Locked: `SOURCE`, `TARGET`, `PATH`, and key `CHECK` migration semantics.
- Allowed or staged: safe runtime settings such as rate limit and UI/Web settings.
- Worker count changes are saved during a running task and marked as taking effect on the next task. Runtime-safe values such as rate limit can apply immediately.

The UI must show why a locked field cannot be changed instead of silently ignoring edits.

## Security

- `require_login = true` by default.
- Default credentials are `admin/admin`.
- Login uses server-side session cookies.
- Modification endpoints require an authenticated session.
- APIs should accept same-origin requests only.
- Sensitive fields show as masked values. They are overwritten only when the user enters a replacement value.
- `SOURCE.ak`, `SOURCE.sk`, `TARGET.ak`, `TARGET.sk`, and `WEB_UI.password` are sensitive fields.
- Non-loopback listening requires login.

## Error Handling

- Server startup errors should be printed clearly in the terminal.
- Port conflict should fail with a message that includes the configured host and port.
- Remote browser API errors should return structured JSON errors.
- Config validation errors should identify the section and key.
- Task start should fail if another task is active.
- Stop, pause, and resume should be idempotent where possible.

## Testing

Add tests for:

- `[WEB_UI]` defaults and config migration.
- Loopback versus non-loopback login enforcement.
- Password encryption-on-save behavior.
- Config API masking and sensitive-field preservation.
- Local browser API behavior.
- Remote browser API behavior with fake OBS client.
- TaskManager state transitions for start, pause, resume, stop, complete, and failure.
- Running-task config locking behavior.

Use standard library test clients or direct handler-level tests to avoid adding HTTP test dependencies.

## Implementation Notes

- Keep the Web code in focused modules, for example `core/web_ui.py`, `core/task_manager.py`, and static asset helpers.
- Reuse existing `list_local_path`, `list_remote_buckets`, `list_remote_prefix`, source-list helpers, config read/write helpers, and dashboard snapshot logic.
- Avoid duplicating CLI browser logic where the lower-level `core.object_browser` functions already provide the needed data.
- Keep the initial Web UI static and self-contained.

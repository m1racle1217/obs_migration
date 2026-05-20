import configparser
import ipaddress


MASKED_SECRET = "********"


def _obs_migrate():
    import obs_migrate

    return obs_migrate


def is_sensitive(section, key):
    obs_migrate = _obs_migrate()
    return (section, key) in obs_migrate.SENSITIVE_FIELDS


def is_loopback_host(host):
    text = str(host or "").strip().lower()
    if text in {"localhost", "[::1]"}:
        return True
    if not text:
        return False
    try:
        return ipaddress.ip_address(text).is_loopback
    except ValueError:
        return False


def validate_web_access(cfg):
    if not cfg.has_section("WEB_UI"):
        return

    _validate_web_port(cfg)
    for key in ("enabled", "require_login", "auto_open"):
        _validate_web_bool(cfg, key)

    host = cfg.get("WEB_UI", "host", fallback="127.0.0.1")
    require_login = _get_web_bool(cfg, "require_login", True)
    if not is_loopback_host(host) and not require_login:
        raise ValueError("WEB_UI.require_login must be true when WEB_UI.host is not loopback")


def config_to_payload(cfg, decrypt_secret=None):
    obs_migrate = _obs_migrate()
    payload = {}

    for section in obs_migrate.DEFAULT_CONFIG:
        if not cfg.has_section(section):
            continue
        payload[section] = {}
        for key, value in cfg[section].items():
            sensitive = is_sensitive(section, key)
            shown_value = value
            if sensitive and value:
                shown_value = MASKED_SECRET
            payload[section][key] = {
                "value": shown_value,
                "sensitive": sensitive,
                "description": obs_migrate.CONFIG_DESC.get(f"{section}.{key}", ""),
            }

    for section in cfg.sections():
        if section in payload:
            continue
        payload[section] = {}
        for key, value in cfg[section].items():
            sensitive = is_sensitive(section, key)
            payload[section][key] = {
                "value": MASKED_SECRET if sensitive and value else value,
                "sensitive": sensitive,
                "description": obs_migrate.CONFIG_DESC.get(f"{section}.{key}", ""),
            }

    return payload


def apply_config_payload(cfg, payload, encrypt_secret, task_running=False):
    obs_migrate = _obs_migrate()
    sections = payload.get("sections", payload)
    next_cfg = _copy_config(cfg)
    changed = []

    for section, values in sections.items():
        if section not in obs_migrate.DEFAULT_CONFIG:
            raise ValueError(f"Unknown config section: {section}")
        if not isinstance(values, dict):
            raise ValueError(f"Invalid config section payload: {section}")

        if not next_cfg.has_section(section):
            next_cfg.add_section(section)

        for key, raw_item in values.items():
            if key not in obs_migrate.DEFAULT_CONFIG[section]:
                raise ValueError(f"Unknown config key: {section}.{key}")

            value = raw_item.get("value") if isinstance(raw_item, dict) else raw_item
            value = "" if value is None else str(value)

            if is_sensitive(section, key):
                if value in {"", MASKED_SECRET}:
                    continue
                value = encrypt_secret(value)

            current_value = next_cfg.get(section, key, fallback="")
            if current_value == value:
                continue

            if task_running and _is_running_task_locked(section, key):
                raise ValueError(f"Cannot change {section}.{key} while a task is running")

            next_cfg.set(section, key, value)
            changed.append(f"{section}.{key}")

    validate_web_access(next_cfg)
    _replace_config(cfg, next_cfg)
    return changed


def _is_running_task_locked(section, key):
    if section in {"SOURCE", "TARGET", "PATH"}:
        return True
    if section == "CHECK":
        return True
    return False


def _validate_web_port(cfg):
    raw_value = cfg.get("WEB_UI", "port", fallback="8765")
    try:
        port = int(str(raw_value).strip())
    except (TypeError, ValueError):
        raise ValueError("WEB_UI.port must be an integer from 1 to 65535")
    if port <= 0 or port > 65535:
        raise ValueError("WEB_UI.port must be an integer from 1 to 65535")


def _validate_web_bool(cfg, key):
    _get_web_bool(cfg, key, False)


def _get_web_bool(cfg, key, fallback):
    try:
        return cfg.getboolean("WEB_UI", key, fallback=fallback)
    except ValueError:
        raise ValueError(f"WEB_UI.{key} must be a boolean value")


def _copy_config(cfg):
    copied = configparser.ConfigParser()
    for section in cfg.sections():
        copied.add_section(section)
        for key, value in cfg[section].items():
            copied.set(section, key, value)
    return copied


def _replace_config(cfg, next_cfg):
    for section in cfg.sections():
        cfg.remove_section(section)
    for section in next_cfg.sections():
        cfg.add_section(section)
        for key, value in next_cfg[section].items():
            cfg.set(section, key, value)

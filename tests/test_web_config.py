import configparser
import os
import tempfile
import unittest
from unittest.mock import patch

import obs_migrate
from core.web_config import (
    MASKED_SECRET,
    apply_config_payload,
    config_to_payload,
    is_loopback_host,
    is_sensitive,
    validate_web_access,
)


def make_config():
    cfg = configparser.ConfigParser()
    for section, items in obs_migrate.DEFAULT_CONFIG.items():
        cfg.add_section(section)
        for key, value in items.items():
            cfg.set(section, key, value)
    return cfg


class WebConfigTests(unittest.TestCase):
    def test_web_ui_defaults_are_migrated_into_existing_config(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.ini")
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "\n".join(
                        [
                            "[SOURCE]",
                            "type = local",
                            "selection_mode = directory",
                            "path = .",
                            "",
                            "[TARGET]",
                            "type = s3",
                            "",
                            "[UI]",
                            "prompt_config = false",
                            "",
                        ]
                    )
                )

            with patch.dict(os.environ, {"OBS_MIGRATE_CONFIG": config_path, "CI": "1"}):
                cfg = obs_migrate.load_config()

        self.assertTrue(cfg.has_section("WEB_UI"))
        self.assertEqual(cfg.get("WEB_UI", "enabled"), "false")
        self.assertEqual(cfg.get("WEB_UI", "host"), "127.0.0.1")
        self.assertEqual(cfg.get("WEB_UI", "port"), "8765")
        self.assertEqual(cfg.get("WEB_UI", "require_login"), "true")
        self.assertEqual(cfg.get("WEB_UI", "username"), "admin")
        self.assertEqual(cfg.get("WEB_UI", "password"), "admin")
        self.assertEqual(cfg.get("WEB_UI", "auto_open"), "false")

    def test_payload_masks_sensitive_values_and_marks_sensitive_fields(self):
        cfg = make_config()
        cfg.set("WEB_UI", "password", "stored-password")
        cfg.set("SOURCE", "ak", "source-ak")

        payload = config_to_payload(cfg)

        self.assertEqual(payload["WEB_UI"]["password"]["value"], MASKED_SECRET)
        self.assertTrue(payload["WEB_UI"]["password"]["sensitive"])
        self.assertEqual(payload["SOURCE"]["ak"]["value"], MASKED_SECRET)
        self.assertTrue(payload["SOURCE"]["ak"]["sensitive"])
        self.assertEqual(payload["WEB_UI"]["username"]["value"], "admin")
        self.assertFalse(payload["WEB_UI"]["username"]["sensitive"])
        self.assertTrue(is_sensitive("WEB_UI", "password"))

    def test_apply_payload_preserves_blank_and_masked_sensitive_values(self):
        cfg = make_config()
        cfg.set("WEB_UI", "password", "encrypted-password")

        blank_changed = apply_config_payload(
            cfg,
            {"WEB_UI": {"password": {"value": ""}}},
            encrypt_secret=lambda value: f"enc:{value}",
        )
        masked_changed = apply_config_payload(
            cfg,
            {"WEB_UI": {"password": {"value": MASKED_SECRET}}},
            encrypt_secret=lambda value: f"enc:{value}",
        )

        self.assertEqual(blank_changed, [])
        self.assertEqual(masked_changed, [])
        self.assertEqual(cfg.get("WEB_UI", "password"), "encrypted-password")

    def test_apply_payload_encrypts_changed_web_password(self):
        cfg = make_config()
        cfg.set("WEB_UI", "password", "old-encrypted")

        changed = apply_config_payload(
            cfg,
            {"WEB_UI": {"password": {"value": "new-secret"}}},
            encrypt_secret=lambda value: f"enc:{value}",
        )

        self.assertEqual(changed, ["WEB_UI.password"])
        self.assertEqual(cfg.get("WEB_UI", "password"), "enc:new-secret")

    def test_apply_payload_locks_migration_keys_while_task_running(self):
        cfg = make_config()

        with self.assertRaises(ValueError):
            apply_config_payload(
                cfg,
                {"SOURCE": {"path": {"value": "/new/source"}}},
                encrypt_secret=lambda value: value,
                task_running=True,
            )

        with self.assertRaises(ValueError):
            apply_config_payload(
                cfg,
                {"CHECK": {"target_compare_mode": {"value": "head_only"}}},
                encrypt_secret=lambda value: value,
                task_running=True,
            )

    def test_running_task_allows_unchanged_locked_fields_with_web_ui_change(self):
        cfg = make_config()
        cfg.set("SOURCE", "path", "/existing/source")
        cfg.set("SOURCE", "ak", "encrypted-source-ak")
        cfg.set("WEB_UI", "auto_open", "false")

        payload = config_to_payload(cfg)
        payload["WEB_UI"]["auto_open"]["value"] = "true"

        changed = apply_config_payload(
            cfg,
            payload,
            encrypt_secret=lambda value: f"enc:{value}",
            task_running=True,
        )

        self.assertEqual(changed, ["WEB_UI.auto_open"])
        self.assertEqual(cfg.get("SOURCE", "path"), "/existing/source")
        self.assertEqual(cfg.get("SOURCE", "ak"), "encrypted-source-ak")
        self.assertEqual(cfg.get("WEB_UI", "auto_open"), "true")

    def test_running_task_rejects_changed_locked_non_sensitive_field(self):
        cfg = make_config()
        cfg.set("SOURCE", "path", "/existing/source")

        with self.assertRaises(ValueError):
            apply_config_payload(
                cfg,
                {"SOURCE": {"path": {"value": "/new/source"}}},
                encrypt_secret=lambda value: value,
                task_running=True,
            )

    def test_external_host_requires_login(self):
        cfg = make_config()
        cfg.set("WEB_UI", "host", "0.0.0.0")
        cfg.set("WEB_UI", "require_login", "false")

        self.assertTrue(is_loopback_host("127.0.0.1"))
        self.assertTrue(is_loopback_host("localhost"))
        self.assertFalse(is_loopback_host("0.0.0.0"))
        with self.assertRaises(ValueError):
            validate_web_access(cfg)

    def test_validate_web_access_accepts_default_loopback_config(self):
        cfg = make_config()

        validate_web_access(cfg)

    def test_validate_web_access_rejects_invalid_ports_with_key_name(self):
        for value in ("abc", "0", "-1", "65536"):
            with self.subTest(value=value):
                cfg = make_config()
                cfg.set("WEB_UI", "port", value)

                with self.assertRaisesRegex(ValueError, "WEB_UI\\.port"):
                    validate_web_access(cfg)

    def test_validate_web_access_rejects_invalid_booleans_with_key_name(self):
        for key in ("enabled", "require_login", "auto_open"):
            with self.subTest(key=key):
                cfg = make_config()
                cfg.set("WEB_UI", key, "sometimes")

                with self.assertRaisesRegex(ValueError, f"WEB_UI\\.{key}"):
                    validate_web_access(cfg)

    def test_apply_payload_rejects_invalid_web_values_before_mutating(self):
        cfg = make_config()
        cfg.set("WEB_UI", "auto_open", "false")

        with self.assertRaisesRegex(ValueError, "WEB_UI\\.auto_open"):
            apply_config_payload(
                cfg,
                {"WEB_UI": {"auto_open": {"value": "sometimes"}}},
                encrypt_secret=lambda value: value,
            )

        self.assertEqual(cfg.get("WEB_UI", "auto_open"), "false")


if __name__ == "__main__":
    unittest.main()

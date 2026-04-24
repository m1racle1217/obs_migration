# -*- coding: utf-8 -*-
"""提供源端与目标端能力探测。"""


# ================================
# 探测后端能力
# ================================
def detect_backend_capabilities(
    source_type,
    target_type,
    source_scheme="s3",
    target_scheme="s3",
    source_endpoint_host="",
    target_endpoint_host="",
):
    source_type = (source_type or "").strip().lower()
    target_type = (target_type or "").strip().lower()
    source_scheme = (source_scheme or "s3").strip().lower()
    target_scheme = (target_scheme or "s3").strip().lower()
    source_endpoint_host = (source_endpoint_host or "").strip().lower()
    target_endpoint_host = (target_endpoint_host or "").strip().lower()

    same_endpoint = bool(source_endpoint_host and source_endpoint_host == target_endpoint_host)
    remote_to_remote = source_type == "s3" and target_type == "s3"

    etag_comparable_backends = {"s3", "obs", "oss"}
    etag_hint = source_scheme in etag_comparable_backends and target_scheme in etag_comparable_backends

    return {
        "source_scheme": source_scheme,
        "target_scheme": target_scheme,
        "same_endpoint": same_endpoint,
        "supports_server_side_copy": remote_to_remote and same_endpoint,
        "supports_multipart_copy": remote_to_remote,
        "etag_hint": etag_hint,
    }

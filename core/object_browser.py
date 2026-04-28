# -*- coding: utf-8 -*-
"""浏览本地文件系统与 OBS / S3 兼容对象存储中的桶、目录和文件。"""

import os
from dataclasses import dataclass
from typing import Optional

try:
    from obs import ObsClient
except ImportError:
    class ObsClient:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise RuntimeError("obs sdk is required for remote storage operations")

from .retry import call_with_retries
from .utils import (
    normalize_obs_key,
    sanitize_key,
    to_unix_timestamp,
)



# ================================
# 浏览器列表项
# ================================
@dataclass
class BrowserItem:
    name: str
    kind: str
    path: str
    size: Optional[int] = None
    mtime: float = 0.0
    etag: str = ""
    storage_class: str = ""
    owner: str = ""
    raw: Optional[object] = None


# ================================
# 浏览器分页结果
# ================================
@dataclass
class BrowserPage:
    scope: str
    bucket: str = ""
    prefix: str = ""
    path: str = ""
    items: Optional[list[BrowserItem]] = None
    page: int = 1
    page_size: int = 50
    total_known: Optional[int] = None
    next_marker: Optional[str] = None
    has_next: bool = False

    def __post_init__(self):
        if self.items is None:
            self.items = []


# ================================
# 归一化远端前缀
# ================================
def normalize_prefix(prefix):
    return sanitize_key(normalize_obs_key(prefix or "")).strip("/")


# ================================
# 获取远端前缀的上一层
# ================================
def parent_prefix(prefix):
    normalized = normalize_prefix(prefix)
    if not normalized:
        return ""
    parts = normalized.split("/")
    return "/".join(parts[:-1])


# ================================
# 获取远端前缀末级名称
# ================================
def basename_from_prefix(prefix):
    normalized = normalize_prefix(prefix)
    if not normalized:
        return ""
    return normalized.rstrip("/").rsplit("/", 1)[-1]


# ================================
# 创建 OBS / S3 兼容客户端
# ================================
def create_obs_client(ak, sk, endpoint, request_timeout=60):
    return ObsClient(
        access_key_id=ak,
        secret_access_key=sk,
        server=endpoint,
        timeout=max(int(request_timeout or 60), 1),
    )


# ================================
# 分页列举远端桶
# ================================
def list_remote_buckets(client, page=1, page_size=50, low_level_retries=3, low_level_retry_sleep=0.5):
    def do_list():
        return client.listBuckets()

    response = call_with_retries(
        do_list,
        retries=low_level_retries,
        base_sleep=low_level_retry_sleep,
        operation="browseBuckets",
    )
    if response.status >= 300:
        raise RuntimeError(f"list buckets error {response.status}")

    body = getattr(response, "body", None)
    raw_buckets = getattr(body, "buckets", None) or getattr(body, "bucketList", None) or []
    items = []
    for bucket in raw_buckets:
        name = getattr(bucket, "name", None)
        if name is None and isinstance(bucket, str):
            name = bucket
        if not name:
            continue
        items.append(
            BrowserItem(
                name=str(name),
                kind="bucket",
                path=str(name),
                mtime=to_unix_timestamp(
                    getattr(bucket, "create_date", None)
                    or getattr(bucket, "creationDate", None)
                    or getattr(bucket, "creation_date", None)
                ),
                owner=getattr(bucket, "owner", "") or "",
                raw=bucket,
            )
        )

    items.sort(key=lambda item: item.name.lower())
    page = max(1, int(page or 1))
    page_size = max(1, int(page_size or 50))
    start = (page - 1) * page_size
    end = start + page_size
    return BrowserPage(
        scope="buckets",
        items=items[start:end],
        page=page,
        page_size=page_size,
        total_known=len(items),
        has_next=end < len(items),
    )


# ================================
# 提取对象存储返回中的公共前缀
# ================================
def _extract_common_prefixes(body):
    for attr in ("commonPrefixs", "commonPrefixes", "commonPrefixList"):
        values = getattr(body, attr, None)
        if not values:
            continue
        for item in values:
            prefix = getattr(item, "prefix", None)
            if prefix is None and isinstance(item, str):
                prefix = item
            if prefix:
                yield sanitize_key(normalize_obs_key(prefix or ""))


# ================================
# 提取远端对象修改时间
# ================================
def _remote_object_mtime(obj):
    return to_unix_timestamp(
        getattr(obj, "lastModified", None)
        or getattr(obj, "last_modified", None)
        or getattr(obj, "lastmodified", None)
    )


# ================================
# 分页列举远端目录与文件
# ================================
def list_remote_prefix(
    client,
    bucket,
    prefix="",
    marker=None,
    page=1,
    page_size=50,
    low_level_retries=3,
    low_level_retry_sleep=0.5,
):
    current_prefix = normalize_prefix(prefix)
    request_prefix = f"{current_prefix}/" if current_prefix else ""
    page_size = max(1, min(int(page_size or 50), 1000))
    kwargs = {
        "prefix": request_prefix,
        "marker": marker,
        "max_keys": page_size,
    }

    def do_list():
        try:
            return client.listObjects(bucket, delimiter="/", **kwargs)
        except TypeError:
            return client.listObjects(bucket, **kwargs)

    response = call_with_retries(
        do_list,
        retries=low_level_retries,
        base_sleep=low_level_retry_sleep,
        operation=f"browseList:{bucket}/{request_prefix or ''}",
    )
    if response.status >= 300:
        raise RuntimeError(f"list objects error {response.status}")

    body = getattr(response, "body", None)
    if body is None:
        return BrowserPage(scope="objects", bucket=bucket, prefix=current_prefix, page=page, page_size=page_size)

    items = []
    for child_prefix in _extract_common_prefixes(body):
        normalized = normalize_prefix(child_prefix)
        if normalized == current_prefix:
            continue
        items.append(
            BrowserItem(
                name=basename_from_prefix(normalized),
                kind="dir",
                path=normalized,
            )
        )

    for obj in getattr(body, "contents", []) or []:
        key = sanitize_key(normalize_obs_key(getattr(obj, "key", "") or "")).strip("/")
        if not key:
            continue
        size = int(getattr(obj, "size", 0) or 0)
        if key == current_prefix or (key.endswith("/") and size == 0):
            continue
        if current_prefix and not key.startswith(f"{current_prefix}/"):
            continue
        name = key.rsplit("/", 1)[-1]
        if not name:
            continue
        items.append(
            BrowserItem(
                name=name,
                kind="file",
                path=key,
                size=size,
                mtime=_remote_object_mtime(obj),
                etag=getattr(obj, "etag", None) or "",
                storage_class=getattr(obj, "storageClass", None) or getattr(obj, "storage_class", "") or "",
                raw=obj,
            )
        )

    items.sort(key=lambda item: (item.kind != "dir", item.name.lower()))
    next_marker = getattr(body, "next_marker", None) or getattr(body, "nextMarker", None)
    has_next = bool(getattr(body, "is_truncated", False) or next_marker)
    return BrowserPage(
        scope="objects",
        bucket=bucket,
        prefix=current_prefix,
        items=items,
        page=page,
        page_size=page_size,
        next_marker=next_marker,
        has_next=has_next,
    )


# ================================
# 统计远端目录的直接子项数量
# ================================
def count_remote_prefix_items(
    client,
    bucket,
    prefix="",
    low_level_retries=3,
    low_level_retry_sleep=0.5,
):
    marker = None
    total = 0

    while True:
        page = list_remote_prefix(
            client,
            bucket,
            prefix,
            marker=marker,
            page_size=1000,
            low_level_retries=low_level_retries,
            low_level_retry_sleep=low_level_retry_sleep,
        )
        total += len(page.items or [])

        if not page.has_next or not page.next_marker:
            break

        marker = page.next_marker

    return total


# ================================
# 分页列举本地目录与文件
# ================================
def list_local_path(path, page=1, page_size=50):
    current_path = os.path.abspath(path or os.getcwd())
    if not os.path.exists(current_path):
        current_path = os.path.abspath(os.path.dirname(current_path) or os.getcwd())
    if os.path.isfile(current_path):
        current_path = os.path.dirname(current_path)

    entries = []
    with os.scandir(current_path) as iterator:
        for entry in iterator:
            try:
                stat = entry.stat()
                is_dir = entry.is_dir()
            except OSError:
                continue
            entries.append(
                BrowserItem(
                    name=entry.name,
                    kind="dir" if is_dir else "file",
                    path=entry.path,
                    size=None if is_dir else int(stat.st_size),
                    mtime=float(stat.st_mtime),
                )
            )

    entries.sort(key=lambda item: (item.kind != "dir", item.name.lower()))
    page = max(1, int(page or 1))
    page_size = max(1, int(page_size or 50))
    start = (page - 1) * page_size
    end = start + page_size
    return BrowserPage(
        scope="local",
        path=current_path,
        items=entries[start:end],
        page=page,
        page_size=page_size,
        total_known=len(entries),
        has_next=end < len(entries),
    )

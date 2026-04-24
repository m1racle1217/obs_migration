# core/checkpoint.py
# -*- coding: utf-8 -*-
"""管理迁移断点状态与目标端对象索引缓存。"""

import os
import sqlite3
import threading


# ================================
# 管理断点与目标索引
# ================================
class Checkpoint:
    """使用 SQLite 保存已完成任务与目标端对象元数据。"""

    # ================================
    # 初始化断点管理器
    # ================================
    def __init__(self, db_path, batch_size=500, obs_index_batch_size=20000):
        self.obs_index_ready = False

        dir_path = os.path.dirname(db_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        self.conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
        )

        self.lock = threading.RLock()
        self.cache = {}
        self.batch = []
        self.batch_size = batch_size
        self.obs_batch = []
        self.obs_index_batch_size = max(int(obs_index_batch_size or 20000), 1000)

        self._init_db()
        self.load_index_flag()

    # ================================
    # 初始化数据库结构
    # ================================
    def _init_db(self):

        with self.lock:
            cursor = self.conn.cursor()

            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            try:
                cursor.execute("PRAGMA temp_store=MEMORY")
                cursor.execute("PRAGMA cache_size=-65536")
                cursor.execute("PRAGMA mmap_size=268435456")
            except Exception:
                pass

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS completed(
                    path TEXT PRIMARY KEY,
                    size INTEGER,
                    mtime REAL
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_path ON completed(path,size,mtime)"
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS obs_objects (
                    key TEXT PRIMARY KEY,
                    size INTEGER,
                    etag TEXT
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_obs_key ON obs_objects(key)"
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            self.conn.commit()

    # ================================
    # 标记索引已就绪
    # ================================
    def set_index_ready(self):

        with self.lock:
            self._flush_obs_index_locked()
            self.conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('obs_index_ready','1')"
            )
            self.conn.commit()
            self.obs_index_ready = True

    # ================================
    # 加载索引状态
    # ================================
    def load_index_flag(self):

        with self.lock:
            cursor = self.conn.execute(
                "SELECT value FROM meta WHERE key='obs_index_ready'"
            )
            row = cursor.fetchone()
            self.obs_index_ready = bool(row and row[0] == "1")

    # ================================
    # 重置目标端索引
    # ================================
    def reset_obs_index(self):

        with self.lock:
            self.obs_batch.clear()
            self.conn.execute("DROP TABLE IF EXISTS obs_objects")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS obs_objects (
                    key TEXT PRIMARY KEY,
                    size INTEGER,
                    etag TEXT
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_obs_key ON obs_objects(key)"
            )
            self.conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('obs_index_ready','0')"
            )
            self.conn.commit()
            self.obs_index_ready = False

    # ================================
    # 写入单条对象索引
    # ================================
    def upsert_obs(self, key, size, etag=None):

        self.upsert_obs_many([(key, size, etag)])

    # ================================
    # 批量写入对象索引
    # ================================
    def upsert_obs_many(self, rows):

        if not rows:
            return

        with self.lock:
            self.obs_batch.extend(rows)
            if len(self.obs_batch) >= self.obs_index_batch_size:
                self._flush_obs_index_locked()

    # ================================
    # 查询对象索引
    # ================================
    def get_obs(self, key):

        with self.lock:
            cursor = self.conn.execute(
                "SELECT size, etag FROM obs_objects WHERE key=?",
                (key,),
            )
            return cursor.fetchone()

    # ================================
    # 刷新对象索引批次
    # ================================
    def flush_obs_index(self):

        with self.lock:
            self._flush_obs_index_locked()

    # ================================
    # 预加载完成缓存
    # ================================
    def is_done(self, path, size, mtime):
        safe = self._normalize_path(path)
        rec = self.cache.get(safe)
        if not rec:
            with self.lock:
                row = self.conn.execute(
                    "SELECT size,mtime FROM completed WHERE path=?",
                    (safe,),
                ).fetchone()
            if not row:
                return False
            rec = (row[0], row[1])
            self.cache[safe] = rec

        old_size, old_mtime = rec
        return old_size == size and old_mtime == mtime

    # ================================
    # 标记任务完成
    # ================================
    def mark_done(self, path, size, mtime):

        safe = self._normalize_path(path)

        with self.lock:
            old = self.cache.get(safe)
            if old == (size, mtime):
                return

            self.cache[safe] = (size, mtime)
            self.batch.append((safe, size, mtime))

            if len(self.batch) >= self.batch_size:
                self._flush_completed_locked()

    # ================================
    # 归一化路径
    # ================================
    def _normalize_path(self, path):

        if isinstance(path, bytes):
            return path.decode("utf-8", "surrogateescape")

        return path.encode("utf-8", "surrogatepass").decode(
            "utf-8",
            "surrogatepass",
        )

    # ================================
    # 刷新完成批次
    # ================================
    def _flush_completed_locked(self):

        if not self.batch:
            return

        self.conn.executemany(
            "INSERT OR REPLACE INTO completed(path,size,mtime) VALUES (?,?,?)",
            self.batch,
        )
        self.conn.commit()
        self.batch.clear()

    # ================================
    # 刷新对象索引批次
    # ================================
    def _flush_obs_index_locked(self):

        if not self.obs_batch:
            return

        self.conn.executemany(
            "INSERT OR REPLACE INTO obs_objects(key,size,etag) VALUES(?,?,?)",
            self.obs_batch,
        )
        self.conn.commit()
        self.obs_batch.clear()

    # ================================
    # 关闭数据库连接
    # ================================
    def close(self):

        with self.lock:
            self._flush_obs_index_locked()
            self._flush_completed_locked()
            self.conn.close()

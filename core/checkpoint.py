# core/checkpoint.py
# -*- coding: utf-8 -*-

import sqlite3
import threading
import os


class Checkpoint:

    def __init__(self, db_path, batch_size=500):
        self.obs_index_ready = False

        dir_path = os.path.dirname(db_path)

        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        self.conn = sqlite3.connect(
            db_path,
            check_same_thread=False
        )

        self.lock = threading.Lock()

        self.cache = {}

        self.batch = []
        self.batch_size = batch_size

        self._init_db()

        self.load_index_flag()

        self._load_cache()

    # ===============================
    # 初始化数据库
    # ===============================

    def _init_db(self):

        c = self.conn.cursor()

        # WAL 提高并发性能
        c.execute("PRAGMA journal_mode=WAL")

        # 减少磁盘同步
        c.execute("PRAGMA synchronous=NORMAL")

        c.execute(
            """
            CREATE TABLE  IF NOT EXISTS completed(
                path TEXT PRIMARY KEY,
                size INTEGER,
                mtime REAL
            )
            """
        )

        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_path ON completed(path,size,mtime)"
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS obs_objects (
            key TEXT PRIMARY KEY,
            size INTEGER,
            etag TEXT
            )
            """
        )

        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_obs_key ON obs_objects(key);"
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
            );
            """
        )
        self.conn.commit()

    # ===============================
    # index ready 标记（持久化）
    # ===============================

    def set_index_ready(self):
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('obs_index_ready','1')"
            )
        self.obs_index_ready = True  # ✅ 内存也同步

    def load_index_flag(self):
        cur = self.conn.execute(
            "SELECT value FROM meta WHERE key='obs_index_ready'"
        )
        row = cur.fetchone()

        self.obs_index_ready = (row and row[0] == '1')
    # ===============================
    # 插入obs_list作为缓存index
    # ===============================
    def upsert_obs(self, key, size, etag=None):
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO obs_objects(key,size,etag) VALUES(?,?,?)",
                (key, size, etag)
            )

    def get_obs(self, key):
        cur = self.conn.execute(
            "SELECT size, etag FROM obs_objects WHERE key=?",
            (key,)
        )
        return cur.fetchone()


    # ===============================
    # 载入缓存
    # ===============================

    def _load_cache(self):

        c = self.conn.cursor()

        rows = c.execute(
            "SELECT path,size,mtime FROM completed"
        ).fetchall()

        for path, size, mtime in rows:
            self.cache[path] = (size, mtime)

    # ===============================
    # 是否完成
    # ===============================

    def is_done(self, path, size, mtime):

        rec = self.cache.get(path)

        if not rec:
            return False

        old_size, old_mtime = rec

        return old_size == size and old_mtime == mtime

    # ===============================
    # 标记完成（批量写）
    # ===============================

    def mark_done(self, path, size, mtime):
        safe = path.encode(
           "utf-8",
           "surrogatepass"
        ).decode(
           "utf-8",
            "surrogatepass"
        )

        with self.lock:

            old = self.cache.get(safe)

            if old == (size, mtime):
                return

            self.cache[safe] = (size, mtime)

            self.batch.append((safe, size, mtime))

    # ===============================
    # 批量写入
    # ===============================

    def _flush(self):

        if not self.batch:
            return

        c = self.conn.cursor()

        c.executemany(
            "INSERT OR REPLACE INTO completed(path,size,mtime) VALUES (?,?,?)",
            self.batch
        )

        self.conn.commit()

        self.batch.clear()

    # ===============================
    # 关闭
    # ===============================

    def close(self):

        with self.lock:

            self._flush()

            self.conn.close()

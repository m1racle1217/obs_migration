# core/checkpoint.py
# -*- coding: utf-8 -*-

import sqlite3
import threading
import os


class Checkpoint:

    def __init__(self, db_path, batch_size=500):

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

        self.conn.commit()

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
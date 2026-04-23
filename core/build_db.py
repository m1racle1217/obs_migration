"""初始化历史版本使用的上传状态数据库。"""

import os
import sqlite3


APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ================================
# 初始化数据库
# ================================
def init_db(db_dir=None):
    if db_dir is None:
        db_dir = os.path.join(APP_DIR, "state")

    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    db_path = os.path.join(db_dir, "tasks.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 创建上传记录表
    # path: 本地文件绝对路径（主键）
    # info: 保存 size_mtime 字符串，用于判断文件是否变化
    # update_time: 记录迁移成功的时间
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS uploaded (
            path TEXT PRIMARY KEY,
            info TEXT,
            update_time DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # 创建索引，提升海量文件下的查询速度
    cur.execute("CREATE INDEX IF NOT EXISTS idx_path ON uploaded(path)")

    conn.commit()
    conn.close()
    print(f"数据库初始化完成: {db_path}")


if __name__ == "__main__":
    init_db()

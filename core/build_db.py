import sqlite3
import os


def init_db(db_dir="state"):
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    db_path = os.path.join(db_dir, "tasks.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 创建上传记录表
    # path: 本地文件绝对路径 (主键)
    # info: 存储 "size_mtime" 字符串，用于校验文件是否被修改过
    # update_time: 记录迁移成功的时间点
    cur.execute("""
    CREATE TABLE IF NOT EXISTS uploaded (
        path TEXT PRIMARY KEY,
        info TEXT,
        update_time DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 建立索引优化百万文件的查询速度
    cur.execute("CREATE INDEX IF NOT EXISTS idx_path ON uploaded(path)")

    conn.commit()
    conn.close()
    print(f"数据库已初始化成功: {db_path}")


if __name__ == "__main__":
    init_db()
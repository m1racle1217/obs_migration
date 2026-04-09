from obs import ObsClient
import logging
import threading

def build_obs_index(ak, sk, endpoint, bucket, prefix, checkpoint):
    client = ObsClient(
        access_key_id=ak,
        secret_access_key=sk,
        server=endpoint
    )

    marker = None
    total = 0

    logging.info(f"[OBS_INDEX] start build index prefix={prefix}")

    while True:
        resp = client.listObjects(
            bucket,
            prefix=prefix,
            marker=marker,
            max_keys=1000
        )

        if resp.status >= 300:
            raise Exception(f"OBS list error {resp.status}")

        for obj in resp.body.contents:
            logging.warning(f"[INDEX_KEY] {obj.key}")
            checkpoint.upsert_obs(obj.key, obj.size, obj.etag)
            total += 1

        if not resp.body.is_truncated:
            break

        marker = resp.body.next_marker

    # ✅ 标记 index ready
    checkpoint.obs_index_ready = True

    logging.info(f"[OBS_INDEX] done total={total}")


# ===============================
# 新增文件 core/reporter.py
# ===============================

class CSVReporter:
    def __init__(self, filepath):
        self.lock = threading.Lock()
        self.fp = open(filepath, "w", encoding="utf-8")
        self.fp.write("local_path,obs_key\n")

    def write(self, local, obs):
        with self.lock:
            self.fp.write(f"{local},{obs}\n")

    def close(self):
        self.fp.close()


# ===============================
# 修改 scanner.py（关键改造）
# ===============================


def scan_directory(
        root_dir,
        obs_prefix,
        task_queue,
        progress,
        checkpoint,
        reporter=None
):

    import os, logging, threading, queue
    from .utils import normalize_obs_key, sanitize_key, clean_path_to_utf8

    root_dir_bytes = os.fsencode(root_dir)

    dir_queue = queue.Queue()
    dir_queue.put(root_dir_bytes)

    def worker():
        while True:
            try:
                current_dir_bytes = dir_queue.get()
            except queue.Empty:
                return

            try:
                with os.scandir(current_dir_bytes) as it:
                    for entry in it:

                        if entry.is_dir(follow_symlinks=False):
                            dir_queue.put(entry.path)
                            continue

                        if not entry.is_file():
                            continue

                        local_bytes = entry.path
                        local_str = clean_path_to_utf8(local_bytes)

                        relative = local_bytes[len(root_dir_bytes):].lstrip(b"/")
                        rel_str = clean_path_to_utf8(relative)

                        obs_key = "/".join(filter(None, [obs_prefix.strip("/"), rel_str]))
                        obs_key = sanitize_key(normalize_obs_key(obs_key))

                        st = entry.stat()
                        size = st.st_size

                        # 写报告
                        if reporter:
                            reporter.write(local_str, obs_key)


                        task_queue.put({
                            "local": local_bytes,
                            "obs": obs_key
                        })

            finally:
                dir_queue.task_done()

    threads = []
    for _ in range(4):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    dir_queue.join()
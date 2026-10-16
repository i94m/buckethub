"""BucketHub「数据处理」模块：B 类流水线（转换→质检→选最长→脱敏→打包）。

由 app.py 在路由定义后调用 register(app, socketio) 激活：
- 懒创建 process_jobs 表
- 单 worker 线程以子进程执行 pipeline/pipeline_runner.py，轮询 status.json 推送进度
- REST 路由由 app.py 侧注册（本模块仅提供数据/执行函数）
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from pathlib import Path

from flask import jsonify, request

PROCESS_DIR = Path(os.environ.get("PROCESS_DIR", "/opt/b2-file-manager/process_jobs"))
RUNNER = Path(os.environ.get("PROCESS_RUNNER",
                             "/opt/b2-file-manager/pipeline/pipeline_runner.py"))
STAGES = ["转换", "质检", "选最长", "脱敏", "打包"]

PROCESS_QUEUE: list[int] = []
PROCESS_COND = threading.Condition()
_worker_started = False
_socketio_ref = None

DDL = """
CREATE TABLE IF NOT EXISTS process_jobs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    label VARCHAR(255) NOT NULL DEFAULT '',
    source_path VARCHAR(1024) NOT NULL,
    workdir VARCHAR(1024) NOT NULL,
    file_id INT,
    status VARCHAR(20) NOT NULL DEFAULT 'queued',
    stage VARCHAR(64) NOT NULL DEFAULT '',
    progress DOUBLE NOT NULL DEFAULT 0,
    stats_json LONGTEXT,
    error TEXT,
    created_at DOUBLE NOT NULL,
    started_at DOUBLE,
    finished_at DOUBLE,
    cancelled TINYINT NOT NULL DEFAULT 0
)
"""


def register(app, socketio):
    global _socketio_ref
    _socketio_ref = socketio

    @socketio.on("connect")
    def _process_snapshot():
        socketio.emit("process_snapshot",
                      [process_payload(r) for r in recent_process_jobs(50)])

    start_worker_once()


def get_job_row(job_id: int):
    with get_db_conn() as conn:
        return conn.execute(
            "SELECT * FROM process_jobs WHERE id = %s", (job_id,)).fetchone()


def cancel_job(job_id: int):
    with get_db_conn() as conn:
        row = conn.execute(
            "SELECT workdir FROM process_jobs WHERE id = %s", (job_id,)).fetchone()
        if row:
            conn.execute(
                "UPDATE process_jobs SET cancelled = 1 WHERE id = %s", (job_id,))
    if row:
        try:
            (Path(row["workdir"]) / "cancel.flag").write_text("cancel", encoding="utf-8")
        except Exception:
            pass


def create_job(source: str, label: str) -> int:
    p = Path(source)
    if not p.exists():
        raise ValueError(f"路径不存在: {source}")
    return create_process_job(source, label)


def apikey_ok_local():
    expected = os.environ.get("APP_API_KEY", "")
    if not expected:
        return False
    provided = request.args.get("apikey", "")
    if not provided:
        provided = request.headers.get("X-API-Key", "")
    if not provided and request.headers.get("Authorization", "").startswith("Bearer "):
        provided = request.headers["Authorization"][7:]
    import secrets
    return bool(provided) and secrets.compare_digest(provided, expected)


def get_db_conn():
    from db import get_db
    return get_db()


def ensure_tables():
    with get_db_conn() as conn:
        conn.execute(DDL)
        cols = {row["Field"] for row in conn.execute("SHOW COLUMNS FROM process_jobs")}
        if "file_id" not in cols:
            conn.execute("ALTER TABLE process_jobs ADD COLUMN file_id INT")


def _resolve_file_id(conn, source: str):
    """按服务器路径/来源匹配文件记录（local_path 最长命中优先）。"""
    if not source:
        return None
    row = conn.execute(
        """SELECT id FROM files
           WHERE (local_path IS NOT NULL AND local_path <> ''
                  AND (local_path = ? OR ? LIKE CONCAT('%/', local_path)))
              OR source_url = ? OR object_key = ?
           ORDER BY LENGTH(local_path) DESC LIMIT 1""",
        (source, source, source, source)).fetchone()
    return row["id"] if row else None


def create_process_job(source: str, label: str) -> int:
    ensure_tables()
    workdir = str(PROCESS_DIR / f"job-{int(time.time())}")
    now = time.time()
    with get_db_conn() as conn:
        file_id = _resolve_file_id(conn, source)
        cur = conn.execute(
            """INSERT INTO process_jobs
               (label, source_path, workdir, status, stage, progress, created_at, file_id)
               VALUES (%s, %s, %s, 'queued', '排队中', 0, %s, %s)""",
            (label, source, workdir, now, file_id))
        job_id = cur.lastrowid
        PROCESS_QUEUE.append(job_id)
        with PROCESS_COND:
            PROCESS_COND.notify_all()
        return job_id


def recent_process_jobs(limit: int = 50):
    ensure_tables()
    with get_db_conn() as conn:
        return conn.execute(
            "SELECT * FROM process_jobs ORDER BY id DESC LIMIT %s", (limit,)).fetchall()


def process_payload(row: dict) -> dict:
    stats = None
    if row.get("stats_json"):
        try:
            stats = json.loads(row["stats_json"])
        except Exception:
            stats = None
    return {
        "id": row["id"], "label": row.get("label"), "source_path": row.get("source_path"),
        "status": row.get("status"), "stage": row.get("stage"),
        "progress": row.get("progress"), "stats": stats, "error": row.get("error"),
        "created_at": row.get("created_at"), "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "stages": [s.get("status") if isinstance(s, dict) else s
                   for s in (stats or {}).get("stages", [])] if stats else [],
    }


def update_job(job_id: int, **fields):
    sets = ", ".join(f"{k} = %s" for k in fields)
    with get_db_conn() as conn:
        conn.execute(
            f"UPDATE process_jobs SET {sets} WHERE id = %s",
            tuple(fields.values()) + (job_id,))


def kick_worker():
    global _worker_started
    with PROCESS_COND:
        if not _worker_started:
            _worker_started = True
            threading.Thread(target=worker_loop, name="process-worker",
                             daemon=True).start()


def start_worker_once():
    pass

def start_worker():
    start_worker_once()
    kick_worker()


def worker_loop():
    ensure_tables()
    while True:
        with PROCESS_COND:
            if not PROCESS_QUEUE:
                PROCESS_COND.wait(timeout=5)
                continue
            job_id = PROCESS_QUEUE.pop(0)
        try:
            run_job(job_id)
        except Exception as exc:  # noqa: BLE001
            try:
                update_job(job_id, status="failed", error=str(exc)[:500],
                           finished_at=time.time())
                emit_update(job_id)
            except Exception:
                pass


def run_job(job_id: int):
    with get_db_conn() as conn:
        row = conn.execute(
            "SELECT * FROM process_jobs WHERE id = %s", (job_id,)).fetchone()
    if not row or row.get("cancelled"):
        update_job(job_id, status="cancelled", finished_at=time.time())
        emit_update(job_id)
        return
    source, workdir = row["source_path"], row["workdir"]
    Path(workdir).mkdir(parents=True, exist_ok=True)
    update_job(job_id, status="running", stage="转换", progress=0,
               started_at=time.time())
    emit_update(job_id)

    cmd = [sys_executable(), str(RUNNER), "--job-dir", workdir, "--source", source]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    status_path = Path(workdir) / "status.json"
    last_key = None
    while proc.poll() is None:
        time.sleep(2)
        if row.get("cancelled") or (Path(workdir) / "cancel.flag").exists():
            proc.terminate()
            update_job(job_id, status="cancelled", finished_at=time.time())
            emit_update(job_id)
            return
        try:
            st = json.loads(status_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        stage_idx = next((i for i, s in enumerate(st.get("stages", []))
                          if s.get("status") == "running"), None)
        key = (st.get("progress"), stage_idx)
        if key != last_key:
            last_key = key
            stage_label = STAGES[stage_idx] if stage_idx is not None else ""
            update_job(job_id, status="running", stage=stage_label,
                       progress=st.get("progress", 0),
                       stats_json=json.dumps({"stages": st.get("stages", [])},
                                             ensure_ascii=False))
            emit_update(job_id)
    rc = proc.returncode
    try:
        st = json.loads(status_path.read_text(encoding="utf-8"))
        extra_stats = json.dumps({"stages": st.get("stages", []),
                                  "records": st.get("records"),
                                  "kept_sessions": st.get("kept_sessions")},
                                 ensure_ascii=False)
    except Exception:
        extra_stats = None
    if rc == 0:
        update_job(job_id, status="completed", stage="完成", progress=100,
                   stats_json=extra_stats, finished_at=time.time())
        # 处理完成：回写交付条数 delivered_records，并把状态从
        # pending/processing 推进到 processed（已交付 synced 的保持不动）
        records = None
        try:
            records = json.loads(status_path.read_text(encoding="utf-8")).get("records")
        except Exception:
            records = None
        file_id = row.get("file_id")
        if file_id:
            with get_db_conn() as conn:
                conn.execute(
                    "UPDATE files SET "
                    "delivered_records = COALESCE(?, delivered_records), "
                    "status = CASE WHEN status IN ('pending', 'processing') "
                    "THEN 'processed' ELSE status END, "
                    "updated_at = %s "
                    "WHERE id = %s",
                    (records if isinstance(records, int) else None, time.time(), file_id))
    elif rc == 3:
        update_job(job_id, status="cancelled", finished_at=time.time())
    else:
        update_job(job_id, status="failed", error="流水线执行失败，详见 job 目录 runner.log",
                   stats_json=extra_stats, finished_at=time.time())
    emit_update(job_id)


def emit_update(job_id: int):
    try:
        socketio = _socketio_ref
        if socketio is None:
            return
        with get_db_conn() as conn:
            row = conn.execute(
                "SELECT * FROM process_jobs WHERE id = %s", (job_id,)).fetchone()
        if row:
            socketio.emit("process_update", process_payload(row))
    except Exception:
        pass


def sys_executable() -> str:
    return "/usr/bin/python3"

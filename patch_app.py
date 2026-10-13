#!/usr/bin/env python3
"""Apply the process-module patch to /opt/b2-file-manager/app.py (anchor-verified)."""
from pathlib import Path

APP = Path("/opt/b2-file-manager/app.py")
src = APP.read_text(encoding="utf-8")

ANCHOR_ROUTE = '@app.get("/api/buckets")'
BLOCK_ROUTE = '''# ---- 数据处理（B 类流水线：转换→质检→选最长→脱敏→打包）----
import process_module as _process_module

_process_module.register(app, socketio)


@app.get("/api/process/jobs")
def api_process_jobs_list():
    """处理任务列表。"""
    blocked = require_auth()
    if blocked:
        return blocked
    from flask import jsonify as _jf
    return _jf(_process_module.recent_process_jobs(50))


@app.get("/api/process/jobs/<int:job_id>")
def api_process_job_detail(job_id: int):
    blocked = require_auth()
    if blocked:
        return blocked
    row = _process_module.get_job_row(job_id)
    if row is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(_process_module.process_payload(row))


@app.post("/api/process/jobs")
def api_process_job_create():
    """新建处理任务（source_path 为服务器上的原始 jsonl/目录）。"""
    blocked = require_auth()
    if blocked:
        return blocked
    data = request.get_json(silent=True) or {}
    try:
        job_id = _process_module.create_job(
            str(data.get("source_path") or "").strip(),
            str(data.get("label") or "").strip())
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    socketio.emit("process_update", {"id": job_id, "status": "queued"})
    return jsonify({"id": job_id, "status": "queued"}), 201


@app.post("/api/process/jobs/<int:job_id>/cancel")
def api_process_job_cancel(job_id: int):
    blocked = require_auth()
    if blocked:
        return blocked
    _process_module.cancel_job(job_id)
    socketio.emit("process_update", {"id": job_id, "status": "cancelling"})
    return jsonify({"id": job_id, "status": "cancelling"})


@app.get("/api/buckets")'''

ANCHOR_THREAD = '''    threading.Thread(
        target=worker_loop, args=(DOWNLOAD_QUEUE, None),
        name="download-worker", daemon=True,
    ).start()'''
BLOCK_THREAD = ANCHOR_THREAD + '''
    # 数据处理单并发道：一次跑一个流水线任务
    _process_module.start_worker()'''

assert ANCHOR_ROUTE in src, "route anchor missing"
assert ANCHOR_THREAD in src, "thread anchor missing"
assert "process_module" not in src, "already patched"

src = src.replace(ANCHOR_ROUTE, BLOCK_ROUTE, 1)
src = src.replace(ANCHOR_THREAD, BLOCK_THREAD, 1)
APP.write_text(src, encoding="utf-8")
print("app.py patched OK")

#!/usr/bin/env python3
"""BucketHub - 多桶文件传输调度台。

- 配置：.env（参考 .env.example），Python 依赖由 uv 管理
- 鉴权：所有请求需带 ?apikey=<APP_API_KEY>
- 上传：文件先落盘到 tmp_uploads/，后台单线程队列上传，任务记录在 jobs.db
- 下载：从 B2 流式返回给浏览器
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import queue
import secrets
import shutil
from db import get_db, Error as DBError
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from flask import (
    Flask,
    Response,
    flash,
    jsonify,
    redirect,
    request,
    send_file,
    url_for,
)
from flask_socketio import SocketIO, emit

from backblaze_upload import clean_prefix, normalize_endpoint

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "tmp_uploads"
MAX_JOBS_SHOWN = 50
DEFAULT_REGION = "us-east-1"
# 上传/下载两条单并发 FIFO 队列：互不冲突（可同时各跑一个），
# 但同一队列内严格按提交顺序逐个执行（同一时间最多一个上传 / 一个下载）。
# serial 参数保留兼容（并入对应道，行为一致），不再单独成道。
UPLOAD_QUEUE: queue.Queue[int] = queue.Queue()
DOWNLOAD_QUEUE: queue.Queue[int] = queue.Queue()
SERIAL_QUEUE: queue.Queue[int] = queue.Queue()  # 已废弃，仅占位防外部引用报错
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "3"))
# 并行道并发上限的可调最大值（顶部下拉框范围；每条道固定起这么多个 worker 线程）
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "8"))


class DynamicGate:
    """可动态调整上限的并发闸门：worker 取到任务后先领名额，超过上限则等待。

    调小上限不影响已在传输的任务（新任务等待）；调大后等待中的 worker 立即被唤醒。
    """

    def __init__(self, limit: int) -> None:
        self._cond = threading.Condition()
        self._limit = max(1, int(limit))
        self._active = 0

    def acquire(self) -> None:
        with self._cond:
            while self._active >= self._limit:
                self._cond.wait()
            self._active += 1

    def release(self) -> None:
        with self._cond:
            self._active -= 1
            self._cond.notify()

    def set_limit(self, limit: int) -> int:
        with self._cond:
            self._limit = max(1, int(limit))
            self._cond.notify_all()
            return self._limit

    @property
    def limit(self) -> int:
        with self._cond:
            return self._limit


# 两条道的并发闸门已退役：上传/下载各固定单并发（FIFO 排队）。
# DynamicGate 类保留（无引用，将来若恢复可调并发可复用）。

load_dotenv(BASE_DIR / ".env")

TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=100 * 1024 * 1024,
    multipart_chunksize=100 * 1024 * 1024,
    max_concurrency=4,
    use_threads=True,
)

# 直传任务的分片大小（源流边读边传，不落盘）
TRANSFER_PART_SIZE = 16 * 1024 * 1024

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

BUCKET_PRIVATE: bool | None = None
BUCKET_PRIVATE_NOTE = ""

# 桶客户端注册表：bucket_id → {"client": boto3 client, "name": 显示名, "bucket_name": S3 桶名}
# 由 get_bucket_entry() 惰性构建；凭证/endpoint 变更时 invalidate_bucket_client() 失效。
_BUCKET_REGISTRY: dict[int, dict] = {}
_REGISTRY_LOCK = threading.Lock()

# 旧标识符 → files 表旧 flag 列（仅带 legacy_key 的行镜像写入用，勿在业务代码直接 UPDATE）
LEGACY_FLAG_COLUMNS = {
    "self": "uploaded",
    "beijing": "uploaded_beijing",
    "bucket2": "uploaded_bucket2",
}


def format_bytes(value: float) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} TiB"


def format_time(epoch: float) -> str:
    return "--" if not epoch else time.strftime("%Y-%m-%d %H:%M", time.localtime(epoch))


# --------------------------------------------------------------------------
# 配置与 boto3 client
# --------------------------------------------------------------------------

def validate_config() -> str | None:
    """桶凭证存于 buckets 表（通过 /api/buckets 维护），这里只校验必填的基础项。

    注意：validate_config 在 init_db 之前运行，不能查表。
    """
    if not os.environ.get("APP_API_KEY"):
        return "缺少配置项: APP_API_KEY（请复制 .env.example 为 .env 并填写）"
    required_db = ("DB_HOST", "DB_USERNAME", "DB_DATABASE")
    missing_db = [name for name in required_db if not os.environ.get(name)]
    if missing_db:
        return "缺少数据库配置: " + ", ".join(missing_db)
    return None


def _resolve_bucket_endpoint(endpoint: str | None, region: str | None) -> tuple[str, str]:
    """归一化 endpoint / region（逻辑同旧 resolve_endpoint，来源改为 buckets 行）。

    优先使用 endpoint，能推断 region 就用推断值；自定义 endpoint 无法推断时
    回退 DEFAULT_REGION，也可用 region 显式指定。
    """
    try:
        return normalize_endpoint(endpoint, region)
    except ValueError:
        if not endpoint:
            raise
        endpoint = endpoint.strip()
        if not endpoint.startswith(("https://", "http://")):
            endpoint = "https://" + endpoint
        return endpoint.rstrip("/"), region or DEFAULT_REGION


def _build_client(row: dict):
    """按 buckets 表行构建一个 S3 客户端（不缓存，test 端点也用它）。"""
    endpoint, region = _resolve_bucket_endpoint(row.get("endpoint"), row.get("region"))
    addressing = (row.get("addressing_style") or "auto").strip()
    # 签名版本按桶可配：AWS / B2 / TOS 走 s3v4；华为云 OBS 的 S3 兼容层
    # 对 SigV4 的 PUT 报 400（payload SHA256 校验不兼容），需用 s3（SigV2）。
    signature = (row.get("signature_version") or "s3v4").strip()
    if signature not in ("s3", "s3v4"):
        signature = "s3v4"
    config_kwargs: dict = {
        "signature_version": signature,
        "retries": {"max_attempts": 8, "mode": "standard"},
    }
    if addressing in ("virtual", "path"):
        config_kwargs["s3"] = {"addressing_style": addressing}
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=row["application_key_id"],
        aws_secret_access_key=row["application_key"],
        config=Config(**config_kwargs),
    )


def get_bucket_entry(row: dict) -> dict:
    """惰性构建并缓存桶客户端；row 为 buckets 表行（dict）。"""
    bucket_id = int(row["id"])
    with _REGISTRY_LOCK:
        entry = _BUCKET_REGISTRY.get(bucket_id)
        if entry is None:
            entry = {
                "client": _build_client(row),
                "name": row["name"],
                "bucket_name": row["bucket_name"],
            }
            _BUCKET_REGISTRY[bucket_id] = entry
    return entry


def invalidate_bucket_client(bucket_id: int) -> None:
    with _REGISTRY_LOCK:
        _BUCKET_REGISTRY.pop(bucket_id, None)


def get_buckets(enabled_only: bool = True) -> list[dict]:
    """桶列表（按 sort_order、id；顺序由桶管理拖动排序维护）。"""
    where = "WHERE enabled=1" if enabled_only else ""
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM buckets {where} ORDER BY sort_order, id"
        ).fetchall()
    return [dict(r) for r in rows]


def default_bucket() -> dict | None:
    buckets = get_buckets(enabled_only=True)
    return buckets[0] if buckets else None


def _default_bucket_name() -> str:
    b = default_bucket()
    return b["bucket_name"] if b else ""


def resolve_bucket_ref(param) -> dict:
    """把桶引用解析为 buckets 表行。

    ''/None/'self'/'cloud' → 默认桶；'12' → 桶 id；
    'bucket2'/'beijing' 等 → legacy_key 别名（机器人兼容）。
    找不到时 raise ValueError。
    """
    raw = (str(param) if param is not None else "").strip()
    if raw in ("", "self", "cloud"):
        b = default_bucket()
        if b is None:
            raise ValueError("未配置任何桶，请先在「桶管理」中添加")
        return b
    if raw.isdigit():
        with get_db() as conn:
            row = conn.execute("SELECT * FROM buckets WHERE id=?", (int(raw),)).fetchone()
        if row is None:
            raise ValueError(f"桶 {raw} 不存在")
        return dict(row)
    with get_db() as conn:
        row = conn.execute("SELECT * FROM buckets WHERE legacy_key=?", (raw,)).fetchone()
        if row is not None:
            return dict(row)
    raise ValueError(f"无效的桶标识: {raw}")


def resolve_bucket(target=None) -> tuple:
    """统一获取 (client, bucket_name)（保持旧签名，内部走 resolve_bucket_ref）。"""
    row = resolve_bucket_ref(target)
    entry = get_bucket_entry(row)
    return entry["client"], entry["bucket_name"]


def beijing_enabled() -> bool:
    """兼容 shim：legacy_key='beijing' 的桶存在且启用。"""
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM buckets WHERE legacy_key='beijing' AND enabled=1"
        ).fetchone()
    return row is not None


def apply_upload_flag(conn, file_id: int, bucket_row: dict, value: bool) -> None:
    """统一维护 file_uploads 关联 + 旧 flag 列镜像（须在 conn 事务内调用）。

    所有「某文件已上传到某桶」的状态变更都走这里，勿散落直接 UPDATE。
    """
    if value:
        conn.execute(
            "INSERT IGNORE INTO file_uploads (file_id, bucket_id, uploaded_at) VALUES (?, ?, ?)",
            (file_id, bucket_row["id"], time.time()),
        )
    else:
        conn.execute(
            "DELETE FROM file_uploads WHERE file_id=? AND bucket_id=?",
            (file_id, bucket_row["id"]),
        )
    legacy_key = bucket_row.get("legacy_key")
    if legacy_key and legacy_key in LEGACY_FLAG_COLUMNS:
        conn.execute(
            f"UPDATE files SET {LEGACY_FLAG_COLUMNS[legacy_key]}=? WHERE id=?",
            (1 if value else 0, file_id),
        )


def server_root() -> Path | None:
    """SERVER_FILE_ROOT 配置的服务器文件根目录；未配置返回 None。"""
    raw = os.environ.get("SERVER_FILE_ROOT", "").strip()
    if not raw:
        return None
    root = Path(raw).expanduser()
    if not root.is_absolute():
        root = BASE_DIR / root
    return root.resolve()


def resolve_server_path(raw: str) -> Path:
    """解析服务器路径并校验在 SERVER_FILE_ROOT 内（未配置时不限制）。"""
    if not raw or not raw.strip():
        raise ValueError("请输入绝对路径")
    root = server_root()
    path = Path(raw).expanduser().resolve()
    if not path.is_absolute():
        raise ValueError("请输入绝对路径")
    if root is not None:
        try:
            path.relative_to(root)
        except ValueError:
            raise ValueError(f"路径必须在 SERVER_FILE_ROOT 目录内: {root}") from None
    return path


def resolve_local_path(raw: str) -> Path:
    """解析本地文件列表里的路径（相对 SERVER_FILE_ROOT 或绝对路径）并校验在目录内。

    列表接口返回的是相对路径（如 a.txt、sub/c.txt），下载/删除时直接用它。
    """
    if not raw or not raw.strip():
        raise ValueError("请输入路径")
    root = server_root()
    if root is None:
        raise ValueError("未配置 SERVER_FILE_ROOT")
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate
    path = candidate.resolve()
    try:
        path.relative_to(root)
    except ValueError:
        raise ValueError(f"路径必须在 SERVER_FILE_ROOT 目录内: {root}") from None
    return path


def list_server_files() -> tuple[str | None, list[dict]]:
    root = server_root()
    if root is None:
        return None, []
    root.mkdir(parents=True, exist_ok=True)
    files = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and not path.is_symlink():
            try:
                relative = path.relative_to(root).as_posix()
            except ValueError:
                continue
            files.append({"path": relative, "size": path.stat().st_size, "absolute": str(path)})
    return str(root), files


# --------------------------------------------------------------------------
# 数据库任务记录（MySQL via PyMySQL）
# --------------------------------------------------------------------------


def _drop_index_if_exists(conn, index_name, table_name):
    exists = conn.execute(
        "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
        (table_name, index_name),
    ).fetchone()
    if exists:
        conn.execute(f"DROP INDEX {index_name} ON {table_name}")


def _create_unique_index_if_not_exists(conn, index_name, table_name, columns):
    exists = conn.execute(
        "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
        (table_name, index_name),
    ).fetchone()
    if not exists:
        conn.execute(f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({columns})")


def _create_index_if_not_exists(conn, index_name, table_name, columns):
    exists = conn.execute(
        "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
        (table_name, index_name),
    ).fetchone()
    if not exists:
        conn.execute(f"CREATE INDEX {index_name} ON {table_name} ({columns})")


def init_db() -> None:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INT PRIMARY KEY AUTO_INCREMENT,
                kind VARCHAR(20) NOT NULL DEFAULT 'upload',
                filename VARCHAR(512) NOT NULL,
                object_key VARCHAR(1024) NOT NULL,
                source VARCHAR(1024),
                destination VARCHAR(1024),
                size BIGINT NOT NULL DEFAULT 0,
                status VARCHAR(20) NOT NULL DEFAULT 'queued',
                progress BIGINT NOT NULL DEFAULT 0,
                error TEXT,
                created_at DOUBLE NOT NULL,
                started_at DOUBLE,
                finished_at DOUBLE,
                note TEXT,
                cancelled TINYINT NOT NULL DEFAULT 0,
                paused TINYINT NOT NULL DEFAULT 0,
                serial TINYINT NOT NULL DEFAULT 0
            )
            """
        )
        # 兼容旧库：补齐新增列
        columns = {row["Field"] for row in conn.execute("SHOW COLUMNS FROM jobs")}
        for name, definition in (
            ("kind", "VARCHAR(20) NOT NULL DEFAULT 'upload'"),
            ("source", "VARCHAR(1024)"),
            ("destination", "VARCHAR(1024)"),
            ("note", "TEXT"),
            ("cancelled", "TINYINT NOT NULL DEFAULT 0"),
            ("paused", "TINYINT NOT NULL DEFAULT 0"),
            ("serial", "TINYINT NOT NULL DEFAULT 0"),
            ("bucket_id", "INT"),
        ):
            if name not in columns:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {definition}")
        # 数据源表（原 scripts，已改名/改字段）：
        #   name        数据源名称
        #   script_path 脚本路径（允许空，仅记录备注，系统不执行）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS datasources (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(255) NOT NULL,
                script_path VARCHAR(1024),
                description TEXT,
                created_at DOUBLE NOT NULL,
                updated_at DOUBLE NOT NULL
            )
            """
        )
        # 通用 KV 设置表（如并发上限等运行时可调配置）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                k VARCHAR(64) PRIMARY KEY,
                v VARCHAR(255) NOT NULL
            )
            """
        )
        # 桶配置表：桶从此变成数据，加桶只需插入一行（通过 /api/buckets 或页面「桶管理」维护）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS buckets (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(64) NOT NULL,
                bucket_name VARCHAR(255) NOT NULL,
                application_key_id VARCHAR(255) NOT NULL,
                application_key VARCHAR(255) NOT NULL,
                endpoint VARCHAR(512),
                region VARCHAR(64),
                addressing_style VARCHAR(20) NOT NULL DEFAULT 'auto',
                signature_version VARCHAR(10) NOT NULL DEFAULT 's3v4',
                legacy_key VARCHAR(20) NULL UNIQUE,
                is_default TINYINT NOT NULL DEFAULT 0,
                enabled TINYINT NOT NULL DEFAULT 1,
                sort_order INT NOT NULL DEFAULT 0,
                created_at DOUBLE NOT NULL,
                updated_at DOUBLE NOT NULL
            )
            """
        )
        # 兼容旧库：buckets 表补齐新增列（signature_version：S3 签名版本，见 _build_client）
        bucket_columns = {row["Field"] for row in conn.execute("SHOW COLUMNS FROM buckets")}
        if "signature_version" not in bucket_columns:
            conn.execute(
                "ALTER TABLE buckets ADD COLUMN signature_version VARCHAR(10) NOT NULL DEFAULT 's3v4'"
            )
        # 文件 ↔ 桶 上传关联表（替代 files.uploaded / uploaded_beijing / uploaded_bucket2）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_uploads (
                file_id INT NOT NULL,
                bucket_id INT NOT NULL,
                uploaded_at DOUBLE NOT NULL,
                PRIMARY KEY (file_id, bucket_id)
            )
            """
        )
        _create_index_if_not_exists(conn, "idx_file_uploads_bucket", "file_uploads", "bucket_id")
        # 旧库迁移：把旧 scripts 表的数据搬到 datasources（command → script_path）
        has_old_scripts = conn.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'scripts'"
        ).fetchone()
        if has_old_scripts:
            old_count = conn.execute("SELECT COUNT(*) AS c FROM scripts").fetchone()["c"]
            if old_count > 0:
                conn.execute(
                    "INSERT IGNORE INTO datasources (id, name, script_path, description, created_at, updated_at) "
                    "SELECT id, name, command, description, created_at, updated_at FROM scripts"
                )
            conn.execute("DROP TABLE scripts")
        # 检查 files 表是否存在（SHOW COLUMNS 在表不存在时会报错）
        files_exists = conn.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'files'"
        ).fetchone()
        if files_exists:
            file_info = conn.execute("SHOW COLUMNS FROM files").fetchall()
            file_columns = {row["Field"] for row in file_info}
            md5_col = next((row for row in file_info if row["Field"] == "md5"), None)
        else:
            file_info = []
            file_columns = set()
            md5_col = None
        files_ddl = """
            CREATE TABLE files (
                id INT PRIMARY KEY AUTO_INCREMENT,
                job_id INT,
                object_key VARCHAR(1024) NOT NULL,
                filename VARCHAR(512),
                md5 VARCHAR(32),
                size BIGINT NOT NULL DEFAULT 0,
                bucket VARCHAR(255) NOT NULL DEFAULT '',
                source_url VARCHAR(2048),
                uploaded TINYINT NOT NULL DEFAULT 0,
                uploaded_beijing TINYINT NOT NULL DEFAULT 0,
                uploaded_bucket2 TINYINT NOT NULL DEFAULT 0,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                datasource_id INT,
                download_kind VARCHAR(20),
                download_bucket_id INT,
                local_path VARCHAR(1024),
                auto_upload_buckets TEXT,
                created_at DOUBLE NOT NULL,
                updated_at DOUBLE NOT NULL,
                synced_at DOUBLE,
                error TEXT
            )
        """
        if md5_col is not None and md5_col["Null"] == "NO":
            # 旧版 files 表：md5 NOT NULL。重建为可空（pending 记录先无 md5），并保留已有数据。
            _drop_index_if_exists(conn, "idx_files_md5", "files")
            _drop_index_if_exists(conn, "idx_files_job", "files")
            conn.execute("ALTER TABLE files RENAME TO files_old")
            old_columns = {row["Field"] for row in conn.execute("SHOW COLUMNS FROM files_old")}

            def src(name: str, fallback: str) -> str:
                return name if name in old_columns else fallback

            conn.execute(files_ddl)
            empty_bucket = "''"
            conn.execute(
                "INSERT INTO files "
                "(id, job_id, object_key, filename, md5, size, bucket, source_url, "
                " uploaded, status, datasource_id, created_at, updated_at, synced_at, error) "
                f"SELECT id, {src('job_id', 'NULL')}, object_key, filename, md5, size, "
                f"{src('bucket', empty_bucket)}, source_url, {src('uploaded', '0')}, status, "
                f"{src('datasource_id', src('script_id', 'NULL'))}, created_at, "
                f"{src('updated_at', 'created_at')}, synced_at, error FROM files_old"
            )
            conn.execute("DROP TABLE files_old")
        else:
            conn.execute(files_ddl.replace("CREATE TABLE files", "CREATE TABLE IF NOT EXISTS files", 1))
            # 建表后重新读取列（file_columns 是建表前的快照，全新库时为空集，
            # 不重读会导致对已存在列重复 ADD COLUMN 而报 duplicate column name）
            file_columns = {row["Field"] for row in conn.execute("SHOW COLUMNS FROM files")}
            for name, definition in (
                ("job_id", "INT"),
                ("md5", "VARCHAR(32)"),
                ("bucket", "VARCHAR(255) NOT NULL DEFAULT ''"),
                ("uploaded", "TINYINT NOT NULL DEFAULT 0"),
                ("uploaded_beijing", "TINYINT NOT NULL DEFAULT 0"),
                ("uploaded_bucket2", "TINYINT NOT NULL DEFAULT 0"),
                ("datasource_id", "INT"),
                ("download_kind", "VARCHAR(20)"),
                ("download_bucket_id", "INT"),
                ("local_path", "VARCHAR(1024)"),
                ("auto_upload_buckets", "TEXT"),
                ("updated_at", "DOUBLE"),
            ):
                if name not in file_columns:
                    conn.execute(f"ALTER TABLE files ADD COLUMN {name} {definition}")
            # 旧库迁移：把 script_id 的数据迁到 datasource_id
            if "datasource_id" not in file_columns and "script_id" in file_columns:
                conn.execute("UPDATE files SET datasource_id = script_id WHERE script_id IS NOT NULL")
        _create_unique_index_if_not_exists(conn, "idx_files_md5", "files", "md5")
        _create_unique_index_if_not_exists(conn, "idx_files_job", "files", "job_id")

        # ── 桶配置完全来自数据库（buckets 表），通过 /api/buckets 或页面「桶管理」维护 ──

        # 确保恰好有一个默认桶（无默认桶时把排序最前的桶提为默认）
        has_default = conn.execute(
            "SELECT id FROM buckets WHERE is_default=1 LIMIT 1"
        ).fetchone()
        if has_default is None:
            first = conn.execute(
                "SELECT id FROM buckets ORDER BY sort_order, id LIMIT 1"
            ).fetchone()
            if first is not None:
                conn.execute("UPDATE buckets SET is_default=1 WHERE id=?", (first["id"],))

        # ── 一次性迁移①：files 旧 flag 列 → file_uploads（guard：关联表为空） ──
        uploads_count = conn.execute("SELECT COUNT(*) AS c FROM file_uploads").fetchone()["c"]
        if uploads_count == 0:
            for legacy_key, column in LEGACY_FLAG_COLUMNS.items():
                conn.execute(
                    f"INSERT IGNORE INTO file_uploads (file_id, bucket_id, uploaded_at) "
                    f"SELECT f.id, b.id, COALESCE(f.synced_at, f.updated_at, f.created_at) "
                    f"FROM files f JOIN buckets b ON b.legacy_key=? WHERE f.{column}=1",
                    (legacy_key,),
                )

        # ── 一次性迁移②：jobs 旧 kind（upload_beijing 等 6 组）→ kind + bucket_id ──
        for old_kind, new_kind, legacy_key in (
            ("upload_beijing", "upload", "beijing"),
            ("upload_bucket2", "upload", "bucket2"),
            ("download_beijing", "download", "beijing"),
            ("download_bucket2", "download", "bucket2"),
        ):
            conn.execute(
                "UPDATE jobs j JOIN buckets b ON b.legacy_key=? "
                "SET j.kind=?, j.bucket_id=b.id "
                "WHERE j.kind=? AND j.bucket_id IS NULL",
                (legacy_key, new_kind, old_kind),
            )

        # 旧库回填：uploaded 的记录补 bucket 名（改为默认桶行的桶名）
        default_row = conn.execute(
            "SELECT bucket_name FROM buckets WHERE is_default=1 LIMIT 1"
        ).fetchone()
        default_bucket_name = default_row["bucket_name"] if default_row else ""
        conn.execute(
            "UPDATE files SET uploaded=1, status='synced', "
            "updated_at=COALESCE(updated_at, created_at), "
            "bucket=COALESCE(NULLIF(bucket, ''), ?) WHERE uploaded=1 OR status='synced'",
            (default_bucket_name,),
        )


def get_setting(key: str, default: str | None = None) -> str | None:
    """读 KV 设置（settings 表），不存在返回 default。"""
    with get_db() as conn:
        row = conn.execute("SELECT v FROM settings WHERE k=?", (key,)).fetchone()
    return row["v"] if row is not None else default


def set_setting(key: str, value: str) -> None:
    with get_db() as conn:
        conn.execute(
            "INSERT INTO settings (k, v) VALUES (?, ?) "
            "ON DUPLICATE KEY UPDATE v=VALUES(v)",
            (key, value),
        )


def recent_jobs(limit: int = MAX_JOBS_SHOWN) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(row) for row in rows]


def insert_job(
    *,
    kind: str,
    filename: str,
    object_key: str,
    size: int,
    source: str | None = None,
    destination: str | None = None,
    bucket_id: int | None = None,
    serial: bool = False,
) -> int:
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO jobs (kind, filename, object_key, source, destination, size, bucket_id, serial, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)",
            (kind, filename, object_key, source, destination, size, bucket_id, int(serial), time.time()),
        )
        return cursor.lastrowid


def bucket_prefix() -> str:
    """读取并清洗 BUCKET_PREFIX（可选的应用级隔离前缀）。

    设置后所有上传对象 key 都会前置该前缀，便于在同一个 bucket 中隔离本应用的数据；
    缺省或设为空字符串则不添加前缀，对象 key 直接为 <prefix>/<文件名>。
    """
    return clean_prefix(os.environ.get("BUCKET_PREFIX", ""))


def default_prefix() -> str:
    """表单「前缀」输入框的默认值。

    优先读 DEFAULT_PREFIX；旧配置名 B2_PREFIX 仍向后兼容。
    与 bucket_prefix() 的区别：
      - bucket_prefix()：全局固定的应用隔离前缀，不暴露给用户、不可在表单覆盖；
      - default_prefix()：仅作为表单默认值，用户上传/录入时可随意修改。
    两者最终在 build_object_key() 拼接为：BUCKET_PREFIX/<default_prefix>/<原始文件名>
    """
    return os.environ.get("DEFAULT_PREFIX") or os.environ.get("B2_PREFIX", "")


def build_object_key(prefix: str, filename: str) -> str:
    """生成 BUCKET_PREFIX/<prefix>/<原始文件名> 的对象 key，保留原始文件名。"""
    parts = [p for p in (bucket_prefix(), prefix, filename) if p]
    return "/".join(parts)


def filename_from_url(url: str) -> str:
    """从 URL 提取文件名（path 末段），无则 download。"""
    return PurePosixPath(urlparse(url).path).name or "download"


def object_exists(client, bucket_name: str, object_key: str) -> bool:
    """head_object 成功即存在；404/NotFound 视为不存在；其它异常抛出。

    403/AccessDenied 视为不存在：只写权限的桶（如华为云 OBS 交付桶）无法读取，
    存在性无从判断，按"不存在"放行，由后续实际操作自行成败。
    """
    try:
        client.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        if code in ("403", "AccessDenied", "Forbidden"):
            return False
        raise


def _bucket_display_name(bucket_id) -> str | None:
    """桶显示名：优先注册表缓存，DB 兜底（给 job_payload 用）。"""
    if bucket_id is None:
        return None
    with _REGISTRY_LOCK:
        entry = _BUCKET_REGISTRY.get(int(bucket_id))
        if entry is not None:
            return entry["name"]
    with get_db() as conn:
        row = conn.execute("SELECT name FROM buckets WHERE id=?", (int(bucket_id),)).fetchone()
    return row["name"] if row else None


def job_payload(job: dict) -> dict:
    return {
        "id": job["id"],
        "kind": job["kind"],
        "filename": job["filename"],
        "object_key": job["object_key"],
        "source": job.get("source"),
        "destination": job.get("destination"),
        "note": job.get("note"),
        "size": job["size"],
        "status": job["status"],
        "progress": job["progress"],
        "error": job["error"],
        "created_at": job["created_at"],
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "cancelled": bool(job.get("cancelled", 0)),
        "paused": bool(job.get("paused", 0)),
        "serial": bool(job.get("serial", 0)),
        "bucket_id": job.get("bucket_id"),
        "bucket_name": _bucket_display_name(job.get("bucket_id")),
    }


def emit_job_update(job_id: int) -> None:
    """向所有已连接的页面广播单个任务的最新状态。"""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if row is not None:
        socketio.emit("job_update", job_payload(dict(row)))


# --------------------------------------------------------------------------
# 后台上传队列
# --------------------------------------------------------------------------

class JobCancelled(Exception):
    """任务被用户取消时抛出，用于中止 boto3 传输。"""


# 进程内已请求取消的 job_id 集合。worker 传输回调每块检查它（比查 DB 快），
# 命中则抛 JobCancelled 中止 boto3 传输。cancel 接口写入，worker 收尾时移除。
_CANCELLED: set[int] = set()
_CANCELLED_LOCK = threading.Lock()


def request_cancel(job_id: int) -> None:
    """请求取消一个 job：标记内存集合 + 更新 DB。"""
    with _CANCELLED_LOCK:
        _CANCELLED.add(job_id)
    with get_db() as conn:
        conn.execute("UPDATE jobs SET cancelled=1 WHERE id=?", (job_id,))


def is_cancelled(job_id: int) -> bool:
    return job_id in _CANCELLED


def clear_cancel(job_id: int) -> None:
    with _CANCELLED_LOCK:
        _CANCELLED.discard(job_id)


# 暂停基础设施：用 threading.Event 实现阻塞/唤醒。
# set() = 放行（未暂停），clear() = 阻塞（已暂停）。
_PAUSED: set[int] = set()
_PAUSED_LOCK = threading.Lock()
_PAUSE_EVENTS: dict[int, threading.Event] = {}


def _get_pause_event(job_id: int) -> threading.Event:
    with _PAUSED_LOCK:
        ev = _PAUSE_EVENTS.get(job_id)
        if ev is None:
            ev = threading.Event()
            ev.set()  # 初始：未暂停（set = 放行）
            _PAUSE_EVENTS[job_id] = ev
        return ev


def request_pause(job_id: int) -> None:
    with _PAUSED_LOCK:
        _PAUSED.add(job_id)
    _get_pause_event(job_id).clear()  # clear = 阻塞


def request_resume(job_id: int) -> None:
    with _PAUSED_LOCK:
        _PAUSED.discard(job_id)
    _get_pause_event(job_id).set()  # set = 放行


def is_paused(job_id: int) -> bool:
    return job_id in _PAUSED


def wait_if_paused(job_id: int) -> None:
    """暂停时阻塞，每 0.5s 醒来检查取消标志。"""
    while is_paused(job_id):
        _get_pause_event(job_id).wait(timeout=0.5)
        if is_cancelled(job_id):
            raise JobCancelled(f"任务 {job_id} 暂停期间被取消")


def clear_pause(job_id: int) -> None:
    with _PAUSED_LOCK:
        _PAUSED.discard(job_id)
        _PAUSE_EVENTS.pop(job_id, None)


def make_progress(job_id: int, size: int):
    lock = threading.Lock()
    state = {"progress": 0, "last": 0.0}

    def callback(amount: int) -> None:
        # 每块传输前检查取消标志，命中即中止传输
        if is_cancelled(job_id):
            raise JobCancelled(f"任务 {job_id} 已取消")
        wait_if_paused(job_id)  # 暂停时阻塞，恢复后继续
        with lock:
            if size > 0:
                state["progress"] = min(size, state["progress"] + amount)
            else:
                state["progress"] += amount
            now = time.monotonic()
            if now - state["last"] >= 0.5 or (size > 0 and state["progress"] >= size):
                state["last"] = now
                with get_db() as conn:
                    conn.execute(
                        "UPDATE jobs SET progress=? WHERE id=?",
                        (state["progress"], job_id),
                    )
                emit_job_update(job_id)

    return callback


def _default_client_and_name() -> tuple:
    """默认桶的 (client, bucket_name)；未配置桶时 raise ValueError。"""
    b = default_bucket()
    if b is None:
        raise ValueError("未配置任何桶，请先在「桶管理」中添加")
    entry = get_bucket_entry(b)
    return entry["client"], entry["bucket_name"]


def upload_to_bucket(source: Path, object_key: str, job_id: int, size: int,
                     client=None, bucket_name: str | None = None) -> None:
    if client is None or bucket_name is None:
        default_client, default_name = _default_client_and_name()
        client = client or default_client
        bucket_name = bucket_name or default_name
    client.upload_file(
        str(source),
        bucket_name,
        object_key,
        Callback=make_progress(job_id, size),
        Config=TRANSFER_CONFIG,
    )


def download_to_path(object_key: str, size: int, destination: Path, job_id: int,
                     client=None, bucket_name: str | None = None) -> None:
    if client is None or bucket_name is None:
        default_client, default_name = _default_client_and_name()
        client = client or default_client
        bucket_name = bucket_name or default_name
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".part", dir=str(destination.parent)
    )
    os.close(fd)
    completed = False
    try:
        client.download_file(
            bucket_name,
            object_key,
            temporary_name,
            Callback=make_progress(job_id, size),
            Config=TRANSFER_CONFIG,
        )
        actual_size = os.path.getsize(temporary_name)
        if actual_size != size:
            raise OSError(f"文件大小校验失败：预期 {size}，实际 {actual_size}")
        os.replace(temporary_name, destination)
        completed = True
    finally:
        if not completed:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass


def fetch_url_to_temp(url: str, job_id: int) -> tuple[Path, int, str]:
    """从 URL 流式下载到系统临时目录（/tmp），返回（临时路径, 文件大小, md5）。

    上传到 bucket 后由 cleanup_job_temp 清理该临时文件。
    Content-Length 已知时做大小校验：暂停导致连接中断、读到半截 EOF 时报错，
    而不是把不完整的文件当作下载完成。
    """
    request = Request(url, headers={"User-Agent": "BucketHub/0.2"})
    temp_path = Path(tempfile.gettempdir()) / f"b2-fetch-{job_id}.part"
    digest = hashlib.md5()
    total = 0
    with urlopen(request, timeout=60) as response:
        total = int(response.headers.get("Content-Length") or 0)
        if total > 0:
            with get_db() as conn:
                conn.execute("UPDATE jobs SET size=? WHERE id=?", (total, job_id))
            emit_job_update(job_id)
        callback = make_progress(job_id, total)
        with open(temp_path, "wb") as out:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                digest.update(chunk)
                callback(len(chunk))
    size = temp_path.stat().st_size
    if total > 0 and size != total:
        # 暂停/网络中断导致提前 EOF：删除半截文件并报错，防止被当作下载完成
        temp_path.unlink(missing_ok=True)
        raise OSError(f"下载不完整：预期 {total} 字节，实际 {size}（连接可能被中断）")
    with get_db() as conn:
        conn.execute("UPDATE jobs SET size=? WHERE id=?", (size, job_id))
    emit_job_update(job_id)
    return temp_path, size, digest.hexdigest()


def hash_file(path: Path) -> str:
    """计算本地文件 md5。"""
    digest = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def ensure_unique_md5(md5: str, job_id: int, bucket_id: int | None = None) -> bool:
    """md5 已在该桶存在时跳过上传并给任务加备注，返回是否为新文件。

    只有 file_uploads 里已关联该桶（实际上传成功）的记录才算数，避免中断/失败的
    残留记录导致后续相同文件被误判为"已存在"而跳过上传。
    """
    with get_db() as conn:
        if bucket_id is not None:
            row = conn.execute(
                "SELECT f.object_key FROM files f "
                "JOIN file_uploads fu ON fu.file_id=f.id AND fu.bucket_id=? "
                "WHERE f.md5=? AND f.job_id <> ?",
                (bucket_id, md5, job_id),
            ).fetchone()
        else:
            # 兜底：未指定桶时按任意已上传记录查重（近似旧行为）
            row = conn.execute(
                "SELECT f.object_key FROM files f "
                "JOIN file_uploads fu ON fu.file_id=f.id "
                "WHERE f.md5=? AND f.job_id <> ?",
                (md5, job_id),
            ).fetchone()
    if row is not None:
        with get_db() as conn:
            conn.execute(
                "UPDATE jobs SET note=? WHERE id=?",
                (f"md5 已存在，跳过上传（已存于 {row['object_key']}）", job_id),
            )
        emit_job_update(job_id)
        return False
    return True


def verify_object(object_key: str, size: int, client=None, bucket_name: str | None = None) -> None:
    if client is None or bucket_name is None:
        default_client, default_name = _default_client_and_name()
        client = client or default_client
        bucket_name = bucket_name or default_name
    try:
        meta = client.head_object(Bucket=bucket_name, Key=object_key)
    except ClientError as exc:
        # 只写权限的桶无法读取做核对（上传本身已成功），跳过校验不算失败
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in ("403", "AccessDenied", "Forbidden"):
            return
        raise
    actual = int(meta["ContentLength"])
    if actual != size:
        raise OSError(f"上传校验失败：bucket 中 {actual} 字节，本地 {size} 字节")


class _HashingReader:
    """包装源流：read 时同步累计 md5 与总字节数（直传任务边传边算）。"""

    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.md5()
        self.total = 0

    def read(self, n=-1):
        chunk = self.stream.read(n)
        if chunk:
            self.digest.update(chunk)
            self.total += len(chunk)
        return chunk


def transfer_to_bucket(stream, object_key: str, job_id: int, size: int,
                       client, bucket_name: str) -> tuple[int, str]:
    """源数据流式直传到目标桶（手动分片 multipart，边读边传不落盘）。

    返回（实际传输字节数, md5）。源流只需支持 read()，由调用方负责关闭。
    零字节文件退化为 put_object；任何异常（含取消）都会 abort 分片上传。
    """
    reader = _HashingReader(stream)
    callback = make_progress(job_id, size)
    mpu = client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
    upload_id = mpu["UploadId"]
    parts = []
    part_number = 1
    try:
        while True:
            chunk = reader.read(TRANSFER_PART_SIZE)
            if not chunk:
                break
            resp = client.upload_part(
                Bucket=bucket_name,
                Key=object_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=chunk,
            )
            parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
            part_number += 1
            callback(len(chunk))
        if parts:
            client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        else:
            # 空对象：multipart 不允许空分片列表，改为 put_object
            client.abort_multipart_upload(Bucket=bucket_name, Key=object_key, UploadId=upload_id)
            client.put_object(Bucket=bucket_name, Key=object_key, Body=b"")
    except Exception:
        try:
            client.abort_multipart_upload(Bucket=bucket_name, Key=object_key, UploadId=upload_id)
        except Exception:
            pass
        raise
    return reader.total, reader.digest.hexdigest()


def insert_file_pending(
    *,
    job_id: int | None = None,
    object_key: str,
    filename: str,
    size: int,
    source_url: str | None = None,
    datasource_id: int | None = None,
    download_kind: str | None = None,
    download_bucket_id: int | None = None,
    auto_upload_buckets: list[int] | None = None,
) -> int:
    """登记一条 pending 记录，后续由 worker 逐步更新。

    job_id 为空时表示「只登记未上传」（用户稍后手动触发上传），
    返回新插入的 file id。
    download_kind：文件级下载源 'url'（来源链接）/ 'local'（服务器本地路径）/
    'bucket'（指定桶，配 download_bucket_id）/ None 未配置。
    auto_upload_buckets：勾选的自动上传桶 id 列表（JSON 存储）；
    下载到服务器完成后由 worker 自动逐桶创建上传任务。
    """
    now = time.time()
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO files (job_id, object_key, filename, size, bucket, source_url, datasource_id, "
            "download_kind, download_bucket_id, auto_upload_buckets, status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
            (
                job_id,
                object_key,
                filename,
                size,
                _default_bucket_name(),
                source_url,
                datasource_id,
                download_kind,
                download_bucket_id,
                json.dumps(auto_upload_buckets) if auto_upload_buckets else None,
                now,
                now,
            ),
        )
        return int(cursor.lastrowid)


def update_file_by_job(job_id: int, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = time.time()
    sets = ", ".join(f"{name}=?" for name in fields)
    values = list(fields.values()) + [job_id]
    with get_db() as conn:
        conn.execute(f"UPDATE files SET {sets} WHERE job_id=?", values)


def sync_to_bucket(
    source_path: Path,
    object_key: str,
    job_id: int,
    size: int,
    md5: str,
    filename: str | None = None,
    source_url: str | None = None,
    client=None,
    bucket_name: str | None = None,
    bucket_row: dict | None = None,
) -> bool:
    """上传到 bucket 并更新 files 记录；md5 重复时删除 pending 记录并跳过。

    bucket_row 为 buckets 表行；None 时兜底用默认桶。
    成功后对每个关联 file 走 apply_upload_flag(True)（含旧列镜像），
    默认桶额外回填 files.bucket。
    """
    if bucket_row is None:
        bucket_row = default_bucket()
        if bucket_row is None:
            raise ValueError("未配置任何桶，请先在「桶管理」中添加")
    if client is None or bucket_name is None:
        entry = get_bucket_entry(bucket_row)
        client = client or entry["client"]
        bucket_name = bucket_name or entry["bucket_name"]
    if not ensure_unique_md5(md5, job_id, bucket_row["id"]):
        with get_db() as conn:
            conn.execute("DELETE FROM files WHERE job_id=?", (job_id,))
        return False
    update_file_by_job(job_id, size=size, md5=md5)
    upload_to_bucket(source_path, object_key, job_id, size, client=client, bucket_name=bucket_name)
    verify_object(object_key, size, client=client, bucket_name=bucket_name)
    now = time.time()
    is_default = bool(bucket_row.get("is_default"))
    with get_db() as conn:
        conn.execute(
            "UPDATE files SET md5=?, size=?, status='synced', synced_at=?, updated_at=? WHERE job_id=?",
            (md5, size, now, now, job_id),
        )
        for r in conn.execute("SELECT id FROM files WHERE job_id=?", (job_id,)).fetchall():
            apply_upload_flag(conn, r["id"], bucket_row, True)
            if is_default:
                conn.execute(
                    "UPDATE files SET bucket=? WHERE id=?",
                    (bucket_row["bucket_name"], r["id"]),
                )
    return True


def _copy_local_to_dest(src: Path, dest: Path, job_id: int) -> None:
    """本地来源 → 服务器目录：优先硬链接（同盘零拷贝），跨盘回退流式复制（带进度/取消）。

    复制走隐藏 .part 文件，完成后原子替换，中断不留半截目标文件。
    """
    part = dest.with_name(f".{dest.name}.{job_id}.part")
    try:
        if part.exists() or part.is_symlink():
            part.unlink()
        os.link(src, part)
    except OSError:
        callback = make_progress(job_id, src.stat().st_size)
        with open(src, "rb") as fsrc, open(part, "wb") as fdst:
            while True:
                chunk = fsrc.read(1024 * 1024)
                if not chunk:
                    break
                fdst.write(chunk)
                callback(len(chunk))
    if dest.exists() or dest.is_symlink():
        dest.unlink()
    part.replace(dest)


def cleanup_job_temp(job_id: int, kind: str, destination: str | None = None) -> None:
    """清理任务产生的临时文件（服务器路径上传任务的源文件不属于临时文件）。"""
    if kind == "download" and destination:
        try:
            dest = Path(destination)
            for leftover in dest.parent.glob(f".{dest.name}.*.part"):
                leftover.unlink(missing_ok=True)
        except OSError:
            pass
    else:
        # 同时清理新路径（/tmp/b2-fetch-*）与旧路径（tmp_uploads/*.part），兼容两种来源；
        # 本地来源复制任务的 .part 残留（取消/失败中断）也一并清理
        try:
            (Path(tempfile.gettempdir()) / f"b2-fetch-{job_id}.part").unlink(missing_ok=True)
        except OSError:
            pass
        try:
            (UPLOAD_DIR / f"{job_id}.part").unlink(missing_ok=True)
        except OSError:
            pass
        if destination:
            try:
                dest = Path(destination)
                for leftover in dest.parent.glob(f".{dest.name}.*.part"):
                    leftover.unlink(missing_ok=True)
            except OSError:
                pass


def schedule_auto_uploads(file_row: dict, local_path: str, size: int, serial: bool) -> None:
    """下载完成后按 auto_upload_buckets 自动创建上传任务（文件级自动化链）。

    local_path 为相对 SERVER_FILE_ROOT 的路径；对每个勾选桶建 kind=upload job 入队。
    已上传过的桶跳过；某个桶已被删除时静默跳过。
    """
    raw = file_row.get("auto_upload_buckets")
    if not raw:
        return
    try:
        bucket_ids = [int(x) for x in json.loads(raw)]
    except (TypeError, ValueError):
        return
    if not bucket_ids:
        return
    with get_db() as conn:
        uploaded_ids = {
            u["bucket_id"] for u in conn.execute(
                "SELECT bucket_id FROM file_uploads WHERE file_id=?", (file_row["id"],)
            ).fetchall()
        }
        for bid in bucket_ids:
            if bid in uploaded_ids:
                continue
            b = conn.execute("SELECT * FROM buckets WHERE id=?", (bid,)).fetchone()
            if b is None:
                continue
            job_id = insert_job(
                kind="upload",
                filename=file_row["filename"] or "download",
                object_key=file_row["object_key"],
                size=size,
                source=str(server_root() / local_path),
                bucket_id=bid,
                serial=serial,
            )
            conn.execute("UPDATE files SET job_id=? WHERE id=?", (job_id, file_row["id"]))
            enqueue_job(job_id, "upload", serial)
            emit_job_update(job_id)


def process_job(job_id: int) -> None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None or row["status"] != "queued":
            return
        if is_paused(job_id):
            # 暂停的排队任务跳过（恢复时由 resume 端点重新入队）
            return
        conn.execute(
            "UPDATE jobs SET status='uploading', started_at=? WHERE id=?",
            (time.time(), job_id),
        )
        kind = row["kind"]
        object_key = row["object_key"]
        size = row["size"]
        source = row["source"]
        destination = row["destination"]
        filename = row["filename"]
        bucket_id = row.get("bucket_id")
    emit_job_update(job_id)

    try:
        if kind == "download":
            bucket_row = resolve_bucket_ref(bucket_id)
            entry = get_bucket_entry(bucket_row)
            download_to_path(object_key, size, Path(destination), job_id,
                             client=entry["client"], bucket_name=entry["bucket_name"])
            # 若该 job 关联了 file 记录（从文件列表「下载到服务器」发起），
            # 成功后回写 local_path（相对 SERVER_FILE_ROOT 的路径）作为已下载标识
            with get_db() as conn:
                linked = conn.execute(
                    "SELECT id FROM files WHERE job_id=? LIMIT 1", (job_id,)
                ).fetchone()
            if linked is not None:
                root = server_root()
                if root is not None:
                    try:
                        rel = Path(destination).resolve().relative_to(root).as_posix()
                    except ValueError:
                        rel = Path(destination).name
                else:
                    rel = Path(destination).name
                update_file_by_job(job_id, local_path=rel)
            final_size = size
        else:
            source_path = (
                Path(source)
                if source and not source.startswith(("http://", "https://"))
                else None
            )
            if kind == "fetch":
                # 本地文件来源（source 非 http(s)）：直接取源文件，复制/硬链接不搬移原件
                local_source = source_path
                if local_source is not None:
                    if not local_source.is_file():
                        raise FileNotFoundError(f"本地文件不存在: {source}")
                    size = local_source.stat().st_size
                    md5 = hash_file(local_source)
                    with get_db() as conn:
                        conn.execute("UPDATE jobs SET size=? WHERE id=?", (size, job_id))
                    emit_job_update(job_id)
                else:
                    source_path, size, md5 = fetch_url_to_temp(source, job_id)
                if destination:
                    update_file_by_job(job_id, size=size, md5=md5)
                    dest = Path(destination)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if local_source is not None:
                        _copy_local_to_dest(local_source, dest, job_id)
                    else:
                        shutil.move(str(source_path), dest)
                    # 回写 local_path（相对 SERVER_FILE_ROOT）作为已下载标识
                    root = server_root()
                    if root is not None:
                        try:
                            rel = dest.resolve().relative_to(root).as_posix()
                        except ValueError:
                            rel = dest.name
                    else:
                        rel = dest.name
                    update_file_by_job(job_id, local_path=rel)
                    # 录入时勾选了自动上传桶 → 下载落地后自动链上传任务
                    with get_db() as conn:
                        frow = conn.execute(
                            "SELECT * FROM files WHERE job_id=? LIMIT 1", (job_id,)
                        ).fetchone()
                    if frow is not None:
                        schedule_auto_uploads(dict(frow), rel, size, bool(row.get("serial")))
                else:
                    bucket_row = resolve_bucket_ref(bucket_id)  # 无 bucket_id 时取默认桶
                    sync_to_bucket(source_path, object_key, job_id, size, md5, filename, source,
                                   bucket_row=bucket_row)
            elif kind == "upload":
                bucket_row = resolve_bucket_ref(bucket_id)
                entry = get_bucket_entry(bucket_row)
                if source_path is None:
                    source_path = UPLOAD_DIR / f"{job_id}.part"
                md5 = hash_file(source_path)
                sync_to_bucket(source_path, object_key, job_id, size, md5, filename,
                               client=entry["client"], bucket_name=entry["bucket_name"],
                               bucket_row=bucket_row)
            elif kind == "transfer":
                # 从文件的下载源（桶/链接/本地路径）流式直传到目标桶，无需本地副本
                with get_db() as conn:
                    frow = conn.execute(
                        "SELECT * FROM files WHERE job_id=? LIMIT 1", (job_id,)
                    ).fetchone()
                if frow is None:
                    raise ValueError("未找到关联的文件记录")
                f = dict(frow)
                target_row = resolve_bucket_ref(bucket_id)
                target_entry = get_bucket_entry(target_row)
                with get_db() as conn:
                    uploaded_ids = {
                        u["bucket_id"] for u in conn.execute(
                            "SELECT bucket_id FROM file_uploads WHERE file_id=?", (f["id"],)
                        ).fetchall()
                    }
                fk = f.get("download_kind")
                src_bucket_row = None
                stream = None
                try:
                    if fk == "bucket":
                        if not f.get("download_bucket_id"):
                            raise ValueError("下载源指定的桶不存在或已被删除")
                        src_bucket_row = resolve_bucket_ref(f["download_bucket_id"])
                    elif fk == "url":
                        if not (f.get("source_url") or "").strip():
                            raise ValueError("该文件下载源为下载链接，但没有来源链接")
                    elif fk != "local":
                        d = default_bucket()
                        if d is not None and d["id"] in uploaded_ids:
                            src_bucket_row = d

                    if src_bucket_row is not None:
                        if src_bucket_row["id"] == target_row["id"]:
                            raise ValueError("下载源与目标为同一桶，无需直传")
                        src_entry = get_bucket_entry(src_bucket_row)
                        try:
                            metadata = src_entry["client"].head_object(
                                Bucket=src_entry["bucket_name"], Key=object_key
                            )
                            size = int(metadata["ContentLength"])
                        except ClientError as exc:
                            code = str(exc.response.get("Error", {}).get("Code", ""))
                            if code in ("404", "NoSuchKey", "NotFound"):
                                raise ValueError(
                                    f"下载源桶{src_bucket_row['name']}中不存在对象: {object_key}"
                                ) from exc
                            raise
                        with get_db() as conn:
                            conn.execute("UPDATE jobs SET size=? WHERE id=?", (size, job_id))
                        emit_job_update(job_id)
                        stream = src_entry["client"].get_object(
                            Bucket=src_entry["bucket_name"], Key=object_key
                        )["Body"]
                    elif fk == "local":
                        raw_src = (f.get("source_url") or "").strip()
                        if not raw_src:
                            raise ValueError("该文件下载源为本地，但未填写服务器文件路径")
                        src = Path(raw_src).expanduser()
                        if not src.is_file():
                            raise FileNotFoundError(f"本地文件不存在: {raw_src}")
                        size = src.stat().st_size
                        with get_db() as conn:
                            conn.execute("UPDATE jobs SET size=? WHERE id=?", (size, job_id))
                        emit_job_update(job_id)
                        stream = open(src, "rb")
                    else:
                        url = (f.get("source_url") or "").strip()
                        if not url:
                            raise ValueError("无可直传来源（下载源未配置且没有来源链接）")
                        response = urlopen(
                            Request(url, headers={"User-Agent": "BucketHub/0.2"}), timeout=60
                        )
                        stream = response
                        total = int(response.headers.get("Content-Length") or 0)
                        if total > 0:
                            size = total
                            with get_db() as conn:
                                conn.execute("UPDATE jobs SET size=? WHERE id=?", (size, job_id))
                            emit_job_update(job_id)

                    size, md5 = transfer_to_bucket(
                        stream, object_key, job_id, size,
                        client=target_entry["client"],
                        bucket_name=target_entry["bucket_name"],
                    )
                finally:
                    if stream is not None and hasattr(stream, "close"):
                        try:
                            stream.close()
                        except OSError:
                            pass

                verify_object(object_key, size,
                              client=target_entry["client"],
                              bucket_name=target_entry["bucket_name"])
                now = time.time()
                is_default = bool(target_row.get("is_default"))
                with get_db() as conn:
                    conn.execute(
                        "UPDATE files SET md5=?, size=?, status='synced', synced_at=?, updated_at=? "
                        "WHERE job_id=?",
                        (md5, size, now, now, job_id),
                    )
                    for r in conn.execute(
                        "SELECT id FROM files WHERE job_id=?", (job_id,)
                    ).fetchall():
                        apply_upload_flag(conn, r["id"], target_row, True)
                        if is_default:
                            conn.execute(
                                "UPDATE files SET bucket=? WHERE id=?",
                                (target_row["bucket_name"], r["id"]),
                            )
            else:
                raise ValueError(f"未知任务类型: {kind}")
            final_size = size
        with get_db() as conn:
            conn.execute(
                "UPDATE jobs SET status='done', progress=?, finished_at=? WHERE id=?",
                (final_size, time.time(), job_id),
            )
        emit_job_update(job_id)
    except JobCancelled:
        # 用户取消：标记 cancelled，清理临时文件与 B2 分片
        update_file_by_job(job_id, status="cancelled", error="任务已取消")
        with get_db() as conn:
            conn.execute(
                "UPDATE jobs SET status='cancelled', error='任务已取消', finished_at=? WHERE id=?",
                (time.time(), job_id),
            )
        emit_job_update(job_id)
    except (BotoCoreError, ClientError, OSError, ValueError, TimeoutError, TypeError) as exc:
        update_file_by_job(job_id, status="failed", error=str(exc))
        with get_db() as conn:
            conn.execute(
                "UPDATE jobs SET status='failed', error=?, finished_at=? WHERE id=?",
                (str(exc), time.time(), job_id),
            )
        emit_job_update(job_id)
    finally:
        cleanup_job_temp(job_id, kind, destination)
        clear_cancel(job_id)
        clear_pause(job_id)


def enqueue_job(job_id: int, kind: str, serial: bool = False) -> None:
    """按任务类型分流入队：upload/transfer → 上传道，download/fetch → 下载道。

    两条道各自单并发 FIFO：同一时间最多一个上传 + 一个下载。
    serial 参数仅为兼容旧调用保留（并入对应道，行为一致）。
    """
    if kind in ("upload", "transfer"):
        UPLOAD_QUEUE.put(job_id)
    else:
        DOWNLOAD_QUEUE.put(job_id)


def worker_loop(q: queue.Queue[int], gate: DynamicGate | None = None) -> None:
    while True:
        job_id = q.get()
        if gate is not None:
            gate.acquire()
        try:
            process_job(job_id)
        except Exception as exc:  # 兜底：未预期异常也标记失败，避免任务卡死
            with get_db() as conn:
                conn.execute(
                    "UPDATE jobs SET status='failed', error=?, finished_at=? WHERE id=?",
                    (f"内部错误（{exc}），请重试", time.time(), job_id),
                )
            update_file_by_job(job_id, status="failed", error=f"内部错误（{exc}），请重试")
            emit_job_update(job_id)
        finally:
            if gate is not None:
                gate.release()


def recover_jobs() -> None:
    """重启恢复：上传/下载中的任务标记失败并清理临时文件；排队中的任务继续处理。"""
    # 重启后内存 _PAUSED 清空，DB 中残留的 paused=1 无意义，重置为 0
    with get_db() as conn:
        conn.execute("UPDATE jobs SET paused=0 WHERE paused=1")
        interrupted = conn.execute("SELECT * FROM jobs WHERE status='uploading'").fetchall()
        interrupted = conn.execute("SELECT * FROM jobs WHERE status='uploading'").fetchall()
        for row in interrupted:
            cleanup_job_temp(row["id"], row["kind"], row["destination"])
            update_file_by_job(row["id"], status="failed", error="服务重启，任务中断")
            conn.execute(
                "UPDATE jobs SET status='failed', error='服务重启，任务中断，请重新提交', "
                "finished_at=? WHERE id=?",
                (time.time(), row["id"]),
            )
        pending = conn.execute(
            "SELECT id, kind, serial FROM jobs WHERE status='queued' ORDER BY id"
        ).fetchall()
    for row in pending:
        enqueue_job(row["id"], row["kind"], bool(row["serial"]))


def cleanup_stale_multipart(max_age_hours: float = 24.0, client=None, bucket_name: str | None = None) -> None:
    """清理 B2 中超过阈值仍未完成的分片上传（进程崩溃时的残留兜底）。"""
    if client is None or bucket_name is None:
        default_client, default_name = _default_client_and_name()
        client = client or default_client
        bucket_name = bucket_name or default_name
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        count = 0
        paginator = client.get_paginator("list_multipart_uploads")
        for page in paginator.paginate(Bucket=bucket_name):
            for upload in page.get("Uploads", []):
                initiated = upload.get("Initiated")
                if initiated is None:
                    continue
                if initiated.tzinfo is None:
                    initiated = initiated.replace(tzinfo=timezone.utc)
                if initiated < cutoff:
                    client.abort_multipart_upload(
                        Bucket=bucket_name,
                        Key=upload["Key"],
                        UploadId=upload["UploadId"],
                    )
                    count += 1
        if count:
            print(f"已清理 {count} 个超过 {max_age_hours:.0f} 小时未完成的分片上传。")
    except (BotoCoreError, ClientError) as exc:
        print(f"清理未完成分片上传失败（可忽略）: {exc}")


def recent_files(limit: int = 500) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM files ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(row) for row in rows]


def recent_scripts() -> list[dict]:
    """数据源列表（原 scripts 表）。"""
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM datasources ORDER BY id DESC").fetchall()
    return [dict(row) for row in rows]


def check_bucket_private() -> tuple[bool | None, str]:
    """检测默认桶是否私有：S3 ACL 中出现 AllUsers 公开读即视为公开。"""
    d = default_bucket()
    if d is None:
        return None, "未配置默认桶，跳过公开状态检测"
    try:
        entry = get_bucket_entry(d)
        acl = entry["client"].get_bucket_acl(Bucket=entry["bucket_name"])
        for grant in acl.get("Grants", []):
            grantee = grant.get("Grantee", {})
            uri = grantee.get("URI", "") or ""
            if "AllUsers" in uri:
                return False, "bucket 检测为公开读（ACL 含 AllUsers），请关闭公开访问！"
        return True, "bucket 为私有，禁止外部链接访问 ✓"
    except (BotoCoreError, ClientError) as exc:
        return None, f"无法检测 bucket 公开状态（{exc}）"


def check_bucket_health(client, bucket_name: str, endpoint: str, addressing: str) -> dict:
    """检测桶连通性 + 元数据 / 诊断信息。

    每个字段独立容错：单个 API 失败不影响其它字段。
    - head_bucket：可达性 + 延迟 + 响应头里的 region / 冗余 / 存储类
    - get_bucket_location：region（比 head_bucket 头更权威）
    - get_bucket_versioning：版本控制状态
    - get_bucket_acl：是否公开读（含 AllUsers）
    """
    result: dict = {
        "ok": None,
        "error": None,
        "latency_ms": None,
        "status_code": None,
        "endpoint": endpoint,
        "addressing_style": addressing,
        "region": None,
        "versioning": None,
        "public": None,
        "redundancy": None,
        "storage_class": None,
    }

    # head_bucket：可达性 + 延迟 + 响应头
    try:
        t0 = time.time()
        resp = client.head_bucket(Bucket=bucket_name)
        result["latency_ms"] = int((time.time() - t0) * 1000)
        meta = resp.get("ResponseMetadata", {})
        result["status_code"] = meta.get("HTTPStatusCode")
        headers = meta.get("HTTPHeaders", {}) or {}
        result["redundancy"] = headers.get("x-amz-az-redundancy")
        result["storage_class"] = headers.get("x-amz-storage-class")
        result["region"] = headers.get("x-amz-bucket-region")
        result["ok"] = True
    except (BotoCoreError, ClientError) as exc:
        result["ok"] = False
        code = str(exc.response.get("Error", {}).get("Code", "")) if isinstance(exc, ClientError) else ""
        if code in ("403", "AccessDenied", "Forbidden"):
            # 只写权限的桶（如华为云 OBS 交付桶）：可达但无读权限，不是连通性问题
            result["error"] = "桶可达，但该 AK 无读权限（403，只写桶属正常，上传不受影响）"
            result["status_code"] = 403
        else:
            result["error"] = str(exc)
        return result  # 连不通则不再查询其它字段

    # get_bucket_location
    try:
        loc = client.get_bucket_location(Bucket=bucket_name)
        constraint = loc.get("LocationConstraint")
        if constraint:
            result["region"] = constraint
    except (BotoCoreError, ClientError):
        pass

    # get_bucket_versioning
    try:
        ver = client.get_bucket_versioning(Bucket=bucket_name)
        result["versioning"] = ver.get("Status") or "Disabled"
    except (BotoCoreError, ClientError):
        pass

    # get_bucket_acl：是否公开读
    try:
        acl = client.get_bucket_acl(Bucket=bucket_name)
        is_public = any(
            "AllUsers" in (g.get("Grantee", {}).get("URI", "") or "")
            for g in acl.get("Grants", [])
        )
        result["public"] = is_public
    except (BotoCoreError, ClientError):
        pass

    return result


# --------------------------------------------------------------------------
# Flask 路由
# --------------------------------------------------------------------------

def apikey_ok() -> bool:
    """检查 apikey：支持 query param、X-API-Key header、Authorization Bearer。"""
    expected = os.environ.get("APP_API_KEY", "")
    if not expected:
        return False
    provided = request.args.get("apikey", "")
    if not provided:
        provided = request.headers.get("X-API-Key", "")
    if not provided:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            provided = auth[7:]
    return bool(provided) and secrets.compare_digest(provided, expected)


def require_auth():
    if not apikey_ok():
        if wants_json():
            return jsonify({"error": "未授权"}), 401
        return Response("401 未授权", status=401)
    return None


def wants_json() -> bool:
    """判断是否返回 JSON 响应。

    - Accept: application/json → JSON（React fetch、机器人）
    - 无 Accept 或 Accept: */* → JSON（机器人/curl 默认）
    - Accept: text/html... → redirect（旧浏览器表单）
    """
    accept = request.headers.get("Accept", "")
    if not accept or "application/json" in accept or accept.strip() == "*/*":
        return True
    return False


def respond(status: str, message: str, apikey: str, category: str = "ok",
            code: int = 200, **extra):
    """统一封装「表单提交类」路由的成功/失败响应。

    - JSON 请求（React）：返回 JSON，不跳转。
    - 表单请求（旧）：flash 消息并 redirect 回首页。
    """
    if wants_json():
        payload = {"status": status, "message": message}
        payload.update(extra)
        return jsonify(payload), code
    flash(message, category)
    return redirect(url_for("index", apikey=apikey))


@socketio.on("connect")
def socket_connect():
    if not apikey_ok():
        return False
    emit("jobs_snapshot", [job_payload(job) for job in recent_jobs()])


def list_objects(prefix: str, limit: int | None = 50, client=None, bucket_name: str | None = None) -> list[dict]:
    prefix = prefix.strip("/")
    if prefix:
        prefix += "/"
    objects: list[dict] = []
    if client is None or bucket_name is None:
        default_client, default_name = _default_client_and_name()
        client = client or default_client
        bucket_name = bucket_name or default_name
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.endswith("/") and int(item.get("Size", 0)) == 0:
                continue
            last_modified = item.get("LastModified")
            objects.append({
                "key": key,
                "size": int(item.get("Size", 0)),
                "last_modified": int(last_modified.timestamp()) if last_modified else 0,
            })
    objects.sort(key=lambda obj: obj["last_modified"], reverse=True)
    if limit is not None:
        objects = objects[:limit]
    return objects


def stream_body(body, chunk_size: int = 1024 * 1024):
    try:
        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        body.close()


_downloads_lock = threading.Lock()
_active_downloads: dict[str, dict] = {}


def download_payload(
    dl_id: str, key: str, size: int, transferred: int, status: str, error: str | None = None
) -> dict:
    return {
        "id": dl_id,
        "key": key,
        "size": size,
        "transferred": transferred,
        "status": status,
        "error": error,
    }


def update_download(
    dl_id: str,
    *,
    transferred: int | None = None,
    status: str | None = None,
    error: str | None = None,
) -> None:
    with _downloads_lock:
        record = _active_downloads.get(dl_id)
        if record is None:
            return
        if transferred is not None:
            record["transferred"] = transferred
        if status is not None:
            record["status"] = status
        if error is not None:
            record["error"] = error
        payload = download_payload(
            dl_id,
            record["key"],
            record["size"],
            record["transferred"],
            record["status"],
            record.get("error"),
        )
    socketio.emit("download_update", payload)


@app.get("/")
def index():
    """根路由：前端已由 React (Vite) 接管，Flask 仅提供 /api/* 等接口。

    这里返回最小引导信息（鉴权后），便于直接 curl 探活；
    页面本身由 Vite dev server（开发）或 nginx 托管的 dist/（生产）提供。
    """
    blocked = require_auth()
    if blocked:
        return blocked
    buckets = get_buckets()
    default = next((b for b in buckets if b["is_default"]), None)
    return jsonify({
        "app": "buckethub",
        "bucket": default["bucket_name"] if default else "",
        "default_prefix": default_prefix(),
        "bucket_private": BUCKET_PRIVATE,
        "bucket_private_note": BUCKET_PRIVATE_NOTE,
        "buckets": [
            {
                "id": b["id"],
                "name": b["name"],
                "bucket_name": b["bucket_name"],
                "legacy_key": b.get("legacy_key"),
                "is_default": bool(b["is_default"]),
            }
            for b in buckets
        ],
        "endpoints": [
            "GET /api/files",
            "GET /api/scripts",
            "GET /api/jobs",
            "GET /api/objects",
            "GET /api/buckets",
            "POST /api/submit",
            "POST /upload",
            "GET /download",
        ],
    })


@app.post("/upload")
def upload():
    """上传本地文件到 bucket。

    表单字段：file（必填）、prefix（可选）、key（可选，自定义完整 object key）、
    bucket（可选，桶 id 或 legacy 别名 self/bucket2/beijing）、datasource_id（可选）。
    React (Accept: application/json) 调用时返回 JSON，旧表单则 flash + redirect。
    """
    blocked = require_auth()
    if blocked:
        return blocked
    apikey = request.args.get("apikey", "")

    file = request.files.get("file")
    if file is None or not file.filename:
        return respond("error", "请选择要上传的文件。", apikey, "error", 400)

    filename = Path(file.filename.replace("\\", "/")).name

    # 目标桶：bucket 字段接受桶 id / legacy 别名（self=默认桶）
    bucket_ref = (request.form.get("bucket") or request.form.get("bucket_id") or "self").strip()
    try:
        bucket_row = resolve_bucket_ref(bucket_ref)
    except ValueError as exc:
        return respond("error", str(exc), apikey, "error", 400)

    # 自定义完整 key 优先；否则按 prefix 生成 key（保留原始文件名）
    custom_key = (request.form.get("key") or "").strip().lstrip("/")
    if custom_key:
        object_key = custom_key
    else:
        try:
            prefix = clean_prefix(request.form.get("prefix") or default_prefix())
        except argparse.ArgumentTypeError as exc:
            return respond("error", str(exc), apikey, "error", 400)
        object_key = build_object_key(prefix, filename)

    # 同名拒传：目标 key 已存在则报错，不创建 job、不入队
    entry = get_bucket_entry(bucket_row)
    if object_exists(entry["client"], entry["bucket_name"], object_key):
        return respond("error", f"同名文件已存在：{object_key}", apikey, "error", 400)

    datasource_id = request.form.get("datasource_id", type=int)
    incoming = UPLOAD_DIR / f"incoming-{secrets.token_hex(8)}.part"

    try:
        file.save(str(incoming))
        size = incoming.stat().st_size
        if size == 0:
            raise ValueError("文件内容为空。")
        job_id = insert_job(
            kind="upload",
            filename=filename,
            object_key=object_key,
            size=size,
            bucket_id=bucket_row["id"],
        )
        os.replace(incoming, UPLOAD_DIR / f"{job_id}.part")
        insert_file_pending(
            job_id=job_id,
            object_key=object_key,
            filename=filename,
            size=size,
            datasource_id=datasource_id,
        )
    except (OSError, ValueError, DBError) as exc:
        incoming.unlink(missing_ok=True)
        return respond("error", f"接收文件失败: {exc}", apikey, "error", 500)

    enqueue_job(job_id, "upload")
    emit_job_update(job_id)
    return respond(
        "ok", f"「{filename}」已加入上传队列（{format_bytes(size)}）。", apikey,
        job_id=job_id, object_key=object_key, filename=filename, size=size,
    )


@app.post("/server-upload")
def server_upload():
    blocked = require_auth()
    if blocked:
        return blocked
    apikey = request.args.get("apikey", "")
    raw_path = (request.form.get("path") or "").strip()
    try:
        prefix = clean_prefix(request.form.get("prefix") or default_prefix())
    except argparse.ArgumentTypeError as exc:
        return respond("error", str(exc), apikey, "error", 400)

    try:
        source = resolve_server_path(raw_path)
    except ValueError as exc:
        return respond("error", str(exc), apikey, "error", 400)
    if not source.is_file():
        return respond("error", f"路径不存在或不是普通文件: {source}", apikey, "error", 400)

    object_key = build_object_key(prefix, source.name)
    # 目标桶：bucket 字段接受桶 id / legacy 别名（self=默认桶）
    bucket_ref = (request.form.get("bucket") or request.form.get("bucket_id") or "self").strip()
    try:
        bucket_row = resolve_bucket_ref(bucket_ref)
    except ValueError as exc:
        return respond("error", str(exc), apikey, "error", 400)
    entry = get_bucket_entry(bucket_row)
    # 同名拒传
    if object_exists(entry["client"], entry["bucket_name"], object_key):
        return respond("error", f"同名文件已存在：{object_key}", apikey, "error", 400)
    size = source.stat().st_size
    datasource_id = request.form.get("datasource_id", type=int)
    job_id = insert_job(
        kind="upload",
        filename=source.name,
        object_key=object_key,
        size=size,
        source=str(source),
        bucket_id=bucket_row["id"],
    )
    insert_file_pending(
        job_id=job_id,
        object_key=object_key,
        filename=source.name,
        size=size,
        datasource_id=datasource_id,
    )
    enqueue_job(job_id, "upload")
    emit_job_update(job_id)
    return respond(
        "ok", f"「{source}」已加入上传队列（{format_bytes(size)}）。", apikey,
        job_id=job_id, object_key=object_key, filename=source.name, size=size,
    )


@app.post("/server-download")
def server_download():
    blocked = require_auth()
    if blocked:
        return blocked
    apikey = request.args.get("apikey", "")
    key = (request.form.get("key") or "").strip().lstrip("/")
    raw_destination = (request.form.get("destination") or "").strip()

    if not key:
        return respond("error", "请填写要下载的 bucket 对象名（key）。", apikey, "error", 400)
    if raw_destination:
        try:
            destination = resolve_server_path(raw_destination)
        except ValueError as exc:
            return respond("error", str(exc), apikey, "error", 400)
    else:
        # 缺省目标路径：SERVER_FILE_ROOT/<对象文件名>（与文件记录下载行为一致）
        root = server_root()
        if root is None:
            return respond(
                "error", "未配置 SERVER_FILE_ROOT，请填写完整目标路径。", apikey, "error", 400
            )
        destination = root / (PurePosixPath(key).name or "download")
    if destination.is_dir():
        return respond("error", "目标路径是已存在的目录，请填写完整的文件路径。", apikey, "error", 400)

    # 目标桶：bucket 字段接受桶 id / legacy 别名（self=默认桶）
    bucket_ref = (request.form.get("bucket") or request.form.get("bucket_id") or "self").strip()
    try:
        bucket_row = resolve_bucket_ref(bucket_ref)
    except ValueError as exc:
        return respond("error", str(exc), apikey, "error", 400)
    entry = get_bucket_entry(bucket_row)

    try:
        metadata = entry["client"].head_object(Bucket=entry["bucket_name"], Key=key)
        size = int(metadata["ContentLength"])
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("404", "NoSuchKey", "NotFound") or status == 404:
            msg = f"对象不存在: {key}"
        else:
            msg = f"查询对象失败: {exc}"
        return respond("error", msg, apikey, "error", 400)
    except (BotoCoreError, OSError) as exc:
        return respond("error", f"查询对象失败: {exc}", apikey, "error", 400)

    job_id = insert_job(
        kind="download",
        filename=PurePosixPath(key).name or "download",
        object_key=key,
        size=size,
        destination=str(destination),
        bucket_id=bucket_row["id"],
    )
    enqueue_job(job_id, "download")
    emit_job_update(job_id)
    return respond(
        "ok", f"「{key}」下载到 {destination} 的任务已加入队列。", apikey,
        job_id=job_id, object_key=key, size=size,
    )


@app.get("/api/server-files")
def api_server_files():
    """本地文件列表（SERVER_FILE_ROOT 目录），含总大小。

    返回：
        {
          "root": "/abs/path",        // 目录路径；未配置 SERVER_FILE_ROOT 时为 null
          "total_size": 123456,       // 所有文件字节数总和
          "files": [ {"path": "...", "size": 123, "absolute": "/abs"} ]
        }
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    root, files = list_server_files()
    if root is None:
        return jsonify({"root": None, "total_size": 0, "files": []})
    return jsonify({
        "root": root,
        "total_size": sum(f["size"] for f in files),
        "files": files,
    })


@app.get("/server-file/download")
def server_file_download():
    """把 SERVER_FILE_ROOT 里的文件通过浏览器下载到用户电脑。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    raw = (request.args.get("path") or "").strip()
    try:
        path = resolve_local_path(raw)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not path.is_file():
        return jsonify({"error": f"文件不存在: {raw}"}), 404
    return send_file(
        path,
        as_attachment=True,
        download_name=path.name,
    )


@app.delete("/api/server-files")
def api_server_file_delete():
    """删除 SERVER_FILE_ROOT 里的一个本地文件。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    raw = (request.args.get("path") or "").strip()
    try:
        path = resolve_local_path(raw)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not path.is_file():
        return jsonify({"error": f"文件不存在: {path}"}), 404
    try:
        path.unlink()
    except OSError as exc:
        return jsonify({"error": f"删除失败: {exc}"}), 500
    return jsonify({"deleted": True, "path": raw})


@app.post("/url-upload")
def url_upload():
    blocked = require_auth()
    if blocked:
        return blocked
    apikey = request.args.get("apikey", "")
    url = (request.form.get("url") or "").strip()
    if not url:
        return respond("error", "请填写链接或文件标识。", apikey, "error", 400)

    # 文件级下载源（可选）：url=下载链接 / local=服务器本地路径 / bucket=指定桶
    download_kind = (request.form.get("download_kind") or "").strip()
    download_bucket_id = None
    if download_kind in ("", "none"):
        download_kind = None
    elif download_kind in ("url", "local", "bucket"):
        if download_kind == "bucket":
            with get_db() as conn:
                try:
                    download_bucket_id = _check_download_bucket(
                        conn, request.form.get("download_bucket_id")
                    )
                except ValueError as exc:
                    return respond("error", str(exc), apikey, "error", 400)
        elif download_kind == "url" and not url.lower().startswith(("http://", "https://")):
            return respond(
                "error", "文件来源为「网络链接」时，请填写以 http:// 或 https:// 开头的链接。",
                apikey, "error", 400,
            )
        elif download_kind == "local" and not url.startswith("/"):
            return respond(
                "error", "文件来源为「服务器路径」时，请填写服务器上的文件绝对路径（以 / 开头）。",
                apikey, "error", 400,
            )
    else:
        return respond(
            "error", "download_kind 仅支持 url / local / bucket / none", apikey, "error", 400,
        )

    target = (request.form.get("target") or "bucket").strip()
    destination = None
    object_key = ""
    datasource_id = request.form.get("datasource_id", type=int)

    # 自动上传桶（可选）：勾选的桶 id 列表，逗号分隔。
    # 勾选后录入即自动入队下载（先落地服务器再自动逐桶上传，全自动链）。
    auto_upload_buckets: list[int] = []
    raw_auto = (request.form.get("auto_upload_buckets") or "").strip()
    if raw_auto:
        with get_db() as conn:
            for part in raw_auto.split(","):
                part = part.strip()
                if not part.isdigit():
                    return respond("error", "auto_upload_buckets 必须是桶 id 列表", apikey, "error", 400)
                bid = int(part)
                if conn.execute("SELECT 1 FROM buckets WHERE id=?", (bid,)).fetchone() is None:
                    return respond("error", f"桶 {bid} 不存在", apikey, "error", 400)
                if bid not in auto_upload_buckets:
                    auto_upload_buckets.append(bid)

    serial = request.form.get("serial") in ("1", "true", "on")
    already_in_default = False
    if target == "server":
        raw_destination = (request.form.get("destination") or "").strip()
        try:
            destination = resolve_server_path(raw_destination)
        except ValueError as exc:
            return respond("error", str(exc), apikey, "error", 400)
        if destination.is_dir():
            return respond("error", "目标路径是已存在的目录，请填写完整的文件路径。", apikey, "error", 400)
    else:
        # key = 完整 object key（严格模式，优先）；否则回退 prefix + URL 文件名拼接
        raw_key = (request.form.get("key") or "").strip().strip("/")
        if raw_key:
            try:
                object_key = clean_prefix(raw_key)
            except argparse.ArgumentTypeError as exc:
                return respond("error", str(exc), apikey, "error", 400)
        else:
            try:
                prefix = clean_prefix(request.form.get("prefix") or default_prefix())
            except argparse.ArgumentTypeError as exc:
                return respond("error", str(exc), apikey, "error", 400)
            object_key = build_object_key(prefix, filename_from_url(url))
        # 同名对象已在默认桶 → 照常登记，但直接标记该桶已上传（避免后续重复上传冲突）
        try:
            _b = resolve_bucket_ref(None)
        except ValueError as exc:
            return respond("error", str(exc), apikey, "error", 400)
        _entry = get_bucket_entry(_b)
        already_in_default = object_exists(_entry["client"], _entry["bucket_name"], object_key)

    filename = filename_from_url(url)

    if target == "server":
        # 下载到服务器路径：自动入队处理（保持原逻辑）
        job_id = insert_job(
            kind="fetch",
            filename=filename,
            object_key=object_key,
            size=0,
            source=url,
            destination=str(destination),
        )
        enqueue_job(job_id, "fetch")
        emit_job_update(job_id)
        return respond(
            "ok", f"「{url}」下载到 {destination} 的任务已加入队列。", apikey,
            job_id=job_id, object_key=object_key, filename=filename,
        )

    # 录入到 bucket：登记 pending 记录。
    # - 未勾选自动上传桶：只登记，用户稍后在列表手动触发。
    # - 勾选了自动上传桶：立即入队下载（fetch 到 SERVER_FILE_ROOT），
    #   下载落地后由 schedule_auto_uploads 自动逐桶创建上传任务（全自动链）。
    file_id = insert_file_pending(
        job_id=None,
        object_key=object_key,
        filename=filename,
        size=0,
        source_url=url,
        datasource_id=datasource_id,
        download_kind=download_kind,
        download_bucket_id=download_bucket_id,
        auto_upload_buckets=auto_upload_buckets or None,
    )
    if already_in_default:
        # 默认桶已有同名对象：标记已上传、补 size，不再创建任何下载/上传任务
        with get_db() as conn:
            try:
                head = _entry["client"].head_object(
                    Bucket=_entry["bucket_name"], Key=object_key
                )
                fsize = int(head.get("ContentLength") or 0)
            except ClientError:
                fsize = 0
            conn.execute(
                "UPDATE files SET status='synced', size=?, synced_at=?, updated_at=? WHERE id=?",
                (fsize, time.time(), time.time(), file_id),
            )
            apply_upload_flag(conn, file_id, _b, True)
            conn.execute(
                "UPDATE files SET bucket=? WHERE id=?",
                (_b["bucket_name"], file_id),
            )
        return respond(
            "ok", f"「{filename}」已登记（默认桶已有同名对象，标记为已同步）。", apikey,
            file_id=file_id, object_key=object_key, filename=filename,
        )
    if auto_upload_buckets:
        root = server_root()
        if root is None:
            return respond("error", "未配置 SERVER_FILE_ROOT，无法自动下载", apikey, "error", 400)
        root.mkdir(parents=True, exist_ok=True)
        destination_path = root / filename
        job_id = insert_job(
            kind="fetch",
            filename=filename,
            object_key=object_key,
            size=0,
            source=url,
            destination=str(destination_path),
            serial=serial,
        )
        with get_db() as conn:
            conn.execute("UPDATE files SET job_id=? WHERE id=?", (job_id, file_id))
        enqueue_job(job_id, "fetch", serial)
        emit_job_update(job_id)
        names = []
        with get_db() as conn:
            for bid in auto_upload_buckets:
                r = conn.execute("SELECT name FROM buckets WHERE id=?", (bid,)).fetchone()
                names.append(r["name"] if r else str(bid))
        return respond(
            "ok",
            f"「{filename}」已登记并开始下载，完成后自动上传到：{'、'.join(names)}。",
            apikey,
            file_id=file_id, job_id=job_id, object_key=object_key, filename=filename,
        )
    return respond(
        "ok", f"「{filename}」已登记。", apikey,
        file_id=file_id, object_key=object_key, filename=filename,
    )


def _do_file_upload(file_id: int, bucket_param) -> tuple:
    """统一实现：把服务器本地文件上传到指定桶（建 kind=upload job 入队）。

    bucket_param 支持桶 id / legacy 别名 / None(默认桶)。
    要求 files.local_path 存在（先下载到服务器）且尚未上传到该桶。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    try:
        bucket_row = resolve_bucket_ref(bucket_param)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    with get_db() as conn:
        row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
        already = None
        if row is not None:
            already = conn.execute(
                "SELECT 1 FROM file_uploads WHERE file_id=? AND bucket_id=?",
                (file_id, bucket_row["id"]),
            ).fetchone()
    if row is None:
        return jsonify({"error": f"文件记录 {file_id} 不存在"}), 404
    f = dict(row)

    if already:
        return jsonify({"error": f"该文件已上传到{bucket_row['name']}"}), 400
    local_path = f.get("local_path")
    if not local_path:
        return jsonify({"error": "本地文件不存在，请先下载到服务器"}), 400

    try:
        source = resolve_local_path(local_path)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not source.exists():
        return jsonify({"error": f"本地文件不存在: {local_path}"}), 400

    body = request.get_json(silent=True) or {}
    custom_key = (body.get("key") or "").strip().lstrip("/")
    object_key = custom_key or f["object_key"]
    serial = bool(body.get("serial"))

    filename = f["filename"] or "download"
    job_id = insert_job(
        kind="upload",
        filename=filename,
        object_key=object_key,
        size=f.get("size") or 0,
        source=str(source),
        bucket_id=bucket_row["id"],
        serial=serial,
    )
    with get_db() as conn:
        conn.execute(
            "UPDATE files SET job_id=?, object_key=?, updated_at=? WHERE id=?",
            (job_id, object_key, time.time(), file_id),
        )
    enqueue_job(job_id, "upload", serial)
    emit_job_update(job_id)
    return jsonify({
        "status": "ok",
        "message": f"「{filename}」上传到{bucket_row['name']}的任务已加入队列（逐个执行）。",
        "job_id": job_id,
        "file_id": file_id,
    })


@app.post("/api/files/<int:file_id>/upload")
def api_file_upload(file_id: int):
    """把服务器本地文件上传到指定桶。

    请求体 JSON：{"bucket_id": <桶 id 或 legacy 别名，可选，默认桶>, "key": "..."}。
    """
    body = request.get_json(silent=True) or {}
    return _do_file_upload(file_id, body.get("bucket_id"))


@app.post("/api/files/<int:file_id>/upload-cloud")
def api_file_upload_cloud(file_id: int):
    """旧路由包装：上传到默认桶（机器人兼容）。"""
    return _do_file_upload(file_id, "self")


def _do_file_transfer(file_id: int, bucket_param) -> tuple:
    """统一实现：从文件的下载源流式直传到目标桶，无需本地副本（kind=transfer）。

    下载源按文件级配置解析（与 download-server 缺省逻辑一致）：
      download_kind=bucket → 从该桶（可为其他平台）get_object 拉流；
      =url → 从 source_url 抓取；=local → 读 source_url 指定的服务器本地路径；
      未配置 → 默认桶已上传则从默认桶拉流，否则回退 source_url。
    目标桶 bucket_param 支持桶 id / legacy 别名 / None(默认桶)。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    try:
        bucket_row = resolve_bucket_ref(bucket_param)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    with get_db() as conn:
        row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
        already = None
        if row is not None:
            already = conn.execute(
                "SELECT 1 FROM file_uploads WHERE file_id=? AND bucket_id=?",
                (file_id, bucket_row["id"]),
            ).fetchone()
    if row is None:
        return jsonify({"error": f"文件记录 {file_id} 不存在"}), 404
    f = dict(row)
    if already:
        return jsonify({"error": f"该文件已上传到{bucket_row['name']}"}), 400

    # 快速校验下载源可解析（worker 执行时按真实可读性再校验一次）
    fk = f.get("download_kind")
    source_desc = ""
    if fk == "bucket":
        if not f.get("download_bucket_id"):
            return jsonify({"error": "下载源指定的桶不存在或已被删除"}), 400
        if f["download_bucket_id"] == bucket_row["id"]:
            return jsonify({"error": "下载源与目标为同一桶，无需直传"}), 400
        source_desc = f"bucket:{f['download_bucket_id']}"
    elif fk == "url":
        if not (f.get("source_url") or "").strip():
            return jsonify({"error": "该文件下载源为下载链接，但没有来源链接"}), 400
        source_desc = f["source_url"]
    elif fk == "local":
        if not (f.get("source_url") or "").strip():
            return jsonify({"error": "该文件下载源为本地，但未填写服务器文件路径"}), 400
        source_desc = f["source_url"]
    else:
        with get_db() as conn:
            uploaded_ids = {
                u["bucket_id"] for u in conn.execute(
                    "SELECT bucket_id FROM file_uploads WHERE file_id=?", (file_id,)
                ).fetchall()
            }
        d = default_bucket()
        if d is None or d["id"] not in uploaded_ids:
            if not (f.get("source_url") or "").strip():
                return jsonify({"error": "无可直传来源（下载源未配置且没有来源链接）"}), 400
            source_desc = f["source_url"]
        else:
            if d["id"] == bucket_row["id"]:
                return jsonify({"error": "下载源与目标为同一桶，无需直传"}), 400
            source_desc = f"bucket:{d['id']}"

    body = request.get_json(silent=True) or {}
    serial = bool(body.get("serial"))
    filename = f["filename"] or "download"
    job_id = insert_job(
        kind="transfer",
        filename=filename,
        object_key=f["object_key"],
        size=f.get("size") or 0,
        source=source_desc,
        bucket_id=bucket_row["id"],
        serial=serial,
    )
    with get_db() as conn:
        conn.execute(
            "UPDATE files SET job_id=?, updated_at=? WHERE id=?",
            (job_id, time.time(), file_id),
        )
    enqueue_job(job_id, "transfer", serial)
    emit_job_update(job_id)
    return jsonify({
        "status": "ok",
        "message": f"「{filename}」从下载源直传到{bucket_row['name']}的任务已加入队列。",
        "job_id": job_id,
        "file_id": file_id,
    })


@app.post("/api/files/<int:file_id>/transfer")
def api_file_transfer(file_id: int):
    """从文件的下载源（桶/链接/本地路径）流式直传到目标桶，无需本地副本。

    请求体 JSON：{"bucket_id": <桶 id 或 legacy 别名，可选，默认桶>, "serial": <bool，可选>}。
    """
    body = request.get_json(silent=True) or {}
    return _do_file_transfer(file_id, body.get("bucket_id"))


@app.post("/api/files/<int:file_id>/upload-beijing")
def api_file_upload_beijing(file_id: int):
    """旧路由包装：上传到北京桶（legacy_key 别名）。"""
    return _do_file_upload(file_id, "beijing")


@app.post("/api/files/<int:file_id>/upload-bucket2")
def api_file_upload_bucket2(file_id: int):
    """旧路由包装：上传到自己桶2（legacy_key 别名）。"""
    return _do_file_upload(file_id, "bucket2")


def _do_file_download_server(file_id: int, bucket_param, serial: bool = False) -> tuple:
    """统一实现：下载文件到服务器 SERVER_FILE_ROOT（保留原始文件名）。

    - 显式指定桶（bucket_param 为 id/legacy）：必须已上传到该桶 → 从该桶下载。
    - 缺省（''/None/'self'）按优先级取下载源：
      1. 文件级 download_kind：bucket → 直接从该桶下载（不要求上传记录，
         head_object 校验对象存在）；url → 一律走 source_url 抓取；
         local → source_url 视为服务器本地路径，fetch 任务复制/链接过来。
      2. 未配置：默认桶已上传 → 从默认桶下载；否则回退 source_url 抓取
         （kind=fetch + destination）。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    with get_db() as conn:
        row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
        uploaded_ids = set()
        file_bucket = None
        if row is not None:
            uploaded_ids = {
                u["bucket_id"] for u in conn.execute(
                    "SELECT bucket_id FROM file_uploads WHERE file_id=?", (file_id,)
                ).fetchall()
            }
            if row["download_kind"] == "bucket" and row["download_bucket_id"]:
                file_bucket = conn.execute(
                    "SELECT * FROM buckets WHERE id=?", (row["download_bucket_id"],)
                ).fetchone()
    if row is None:
        return jsonify({"error": f"文件记录 {file_id} 不存在"}), 404
    f = dict(row)

    if f.get("local_path"):
        return jsonify({"error": "该文件已在服务器上"}), 400

    object_key = f["object_key"]
    filename = f["filename"] or "download"

    raw = (str(bucket_param) if bucket_param is not None else "").strip()
    bucket_row = None
    # 下载源指定的桶在 404 消息里的前缀（区分 文件级 / 数据源级 / 显式）
    bucket_error_prefix = ""
    force_fetch = False
    force_local = False
    fetch_error = ""
    if raw and raw not in ("self", "cloud"):
        try:
            bucket_row = resolve_bucket_ref(raw)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        if bucket_row["id"] not in uploaded_ids:
            return jsonify({"error": f"该文件未上传到{bucket_row['name']}"}), 400
    else:
        fk = f.get("download_kind")
        if fk == "bucket":
            if file_bucket is None:
                return jsonify({"error": "该文件下载源指定的桶不存在或已被删除"}), 400
            bucket_row = file_bucket
            bucket_error_prefix = "下载源指定的"
        elif fk == "url":
            force_fetch = True
            fetch_error = "该文件下载源为下载链接，但该文件没有来源链接"
        elif fk == "local":
            force_local = True
        else:
            d = default_bucket()
            if d is not None and d["id"] in uploaded_ids:
                bucket_row = d

    root = server_root()
    if root is None:
        return jsonify({"error": "未配置 SERVER_FILE_ROOT"}), 400
    root.mkdir(parents=True, exist_ok=True)
    destination = root / filename

    if bucket_row is not None:
        entry = get_bucket_entry(bucket_row)
        # bucket → server：校验对象存在并取真实大小
        try:
            metadata = entry["client"].head_object(
                Bucket=entry["bucket_name"], Key=object_key
            )
            size = int(metadata["ContentLength"])
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound"):
                if bucket_error_prefix:
                    return jsonify(
                        {
                            "error": (
                                f"{bucket_error_prefix}桶"
                                f"{bucket_row['name']}中不存在对象: {object_key}"
                            )
                        }
                    ), 404
                return jsonify({"error": f"{bucket_row['name']}中不存在对象: {object_key}"}), 404
            return jsonify({"error": str(exc)}), 502
        except (BotoCoreError, OSError) as exc:
            return jsonify({"error": str(exc)}), 502

        job_id = insert_job(
            kind="download",
            filename=filename,
            object_key=object_key,
            size=size,
            destination=str(destination),
            bucket_id=bucket_row["id"],
            serial=serial,
        )
    elif force_local:
        # 服务器本地路径 → server（source_url 存路径，fetch 任务复制/硬链接过来）
        raw_src = (f.get("source_url") or "").strip()
        if not raw_src:
            return jsonify({"error": "该文件下载源为本地，但未填写服务器文件路径"}), 400
        src = Path(raw_src).expanduser()
        if not src.is_file():
            return jsonify({"error": f"本地文件不存在: {raw_src}"}), 400
        job_id = insert_job(
            kind="fetch",
            filename=filename,
            object_key=object_key,
            size=src.stat().st_size,
            source=str(src),
            destination=str(destination),
            serial=serial,
        )
    elif f.get("source_url"):
        # URL → server
        job_id = insert_job(
            kind="fetch",
            filename=filename,
            object_key=object_key,
            size=0,
            source=f["source_url"],
            destination=str(destination),
            serial=serial,
        )
    elif force_fetch:
        return jsonify({"error": fetch_error}), 400
    else:
        return jsonify({"error": "无可下载来源（既不在桶也无来源链接）"}), 400

    with get_db() as conn:
        conn.execute("UPDATE files SET job_id=? WHERE id=?", (job_id, file_id))
    enqueue_job(job_id, "download" if bucket_row is not None else "fetch", serial)
    emit_job_update(job_id)
    return jsonify({
        "status": "ok",
        "message": f"「{filename}」下载到服务器 {destination} 的任务已加入队列（逐个执行）。",
        "job_id": job_id,
        "file_id": file_id,
    })


@app.post("/api/files/<int:file_id>/download-server")
def api_file_download_server(file_id: int):
    """下载文件到服务器。

    请求体 JSON：{"bucket_id": <桶 id 或 legacy 别名，可选>, "serial": <bool，可选>}。
    缺省时默认桶已上传则从默认桶下载，否则回退 source_url。
    serial=true 时进串行队列（一次只跑一个，按提交顺序接续）。
    """
    body = request.get_json(silent=True) or {}
    return _do_file_download_server(file_id, body.get("bucket_id"), bool(body.get("serial")))


@app.post("/api/files/<int:file_id>/download-server-beijing")
def api_file_download_server_beijing(file_id: int):
    """旧路由包装：从北京桶下载到服务器（legacy_key 别名）。"""
    return _do_file_download_server(file_id, "beijing")


@app.post("/api/files/<int:file_id>/download-server-bucket2")
def api_file_download_server_bucket2(file_id: int):
    """旧路由包装：从自己桶2下载到服务器（legacy_key 别名）。"""
    return _do_file_download_server(file_id, "bucket2")


@app.post("/api/files/<int:file_id>/check")
def api_file_check(file_id: int):
    """重新检测文件在指定位置是否存在，按结果更新记录。

    请求体 JSON：
        {"target": "local" | 桶 id | "cloud"(=默认桶) | "beijing" | "bucket2"}

    始终做真实检测（不因 DB 标记为空就跳过）：
    - local：local_path 为空时回退到 SERVER_FILE_ROOT/filename
    - 桶：直接查桶，不因已标记未上传就返回 False

    返回：
        {"target": "...", "exists": true/false, "file": {更新后的记录}}
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    body = request.get_json(silent=True) or {}
    target = str(body.get("target") or "local").strip()

    with get_db() as conn:
        row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
    if row is None:
        return jsonify({"error": f"文件记录 {file_id} 不存在"}), 404
    f = dict(row)
    now = time.time()

    if target == "local":
        root = server_root()
        if root is None:
            return jsonify({"error": "未配置 SERVER_FILE_ROOT"}), 400

        local_path = f.get("local_path")
        # local_path 有值 → 检它；无值 → 回退到 root/filename
        if local_path:
            try:
                path = resolve_local_path(local_path)
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400
        else:
            filename = f.get("filename") or ""
            if not filename:
                with get_db() as conn:
                    return jsonify({"target": "local", "exists": False, "file": _file_item(conn, row)})
            path = (root / filename).resolve()

        exists = path.is_file()
        file_size = path.stat().st_size if exists else 0
        with get_db() as conn:
            if exists:
                # 文件在磁盘上 → 补记 local_path（若空）+ 更新 size
                sets = ["size=?", "updated_at=?"]
                params = [file_size, now]
                if not local_path:
                    try:
                        rel = path.relative_to(root.resolve()).as_posix()
                    except ValueError:
                        rel = path.name
                    sets.append("local_path=?")
                    params.append(rel)
                params.append(file_id)
                conn.execute(
                    f"UPDATE files SET {', '.join(sets)} WHERE id=?", params,
                )
            elif local_path:
                # DB 有记录但文件已不在 → 清空 local_path
                conn.execute(
                    "UPDATE files SET local_path=NULL, updated_at=? WHERE id=?",
                    (now, file_id),
                )
            row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
            f = _file_item(conn, row)
        return jsonify({"target": "local", "exists": exists, "file": f})

    # 桶分支统一：target 为桶 id 或 legacy 别名（'cloud'/'self'→默认桶）
    if target in ("cloud", "self", "beijing", "bucket2") or target.isdigit():
        try:
            bucket_row = resolve_bucket_ref(target)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        entry = get_bucket_entry(bucket_row)
        with get_db() as conn:
            uploaded_ids = {
                u["bucket_id"] for u in conn.execute(
                    "SELECT bucket_id FROM file_uploads WHERE file_id=?", (file_id,)
                ).fetchall()
            }
        was_uploaded = bucket_row["id"] in uploaded_ids
        is_default = bool(bucket_row.get("is_default"))
        try:
            resp = entry["client"].head_object(
                Bucket=entry["bucket_name"], Key=f["object_key"]
            )
            exists = True
            file_size = resp.get("ContentLength") or 0
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound"):
                exists = False
                file_size = 0
            else:
                return jsonify({"error": str(exc)}), 502
        except (BotoCoreError, OSError) as exc:
            return jsonify({"error": str(exc)}), 502
        with get_db() as conn:
            if exists:
                # 存在 → 补记上传标记 + 更新 size；默认桶新发现时置 synced
                sets = ["size=?", "updated_at=?"]
                params = [file_size, now]
                if is_default and not was_uploaded:
                    sets.extend(["status='synced'", "synced_at=?"])
                    params.append(now)
                params.append(file_id)
                conn.execute(
                    f"UPDATE files SET {', '.join(sets)} WHERE id=?", params,
                )
                apply_upload_flag(conn, file_id, bucket_row, True)
            else:
                # 不存在 → 清除该桶标记；默认桶从 synced 回 pending
                apply_upload_flag(conn, file_id, bucket_row, False)
                if is_default and was_uploaded:
                    conn.execute(
                        "UPDATE files SET status='pending', updated_at=? WHERE id=?",
                        (now, file_id),
                    )
            row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
            f = _file_item(conn, row)
        return jsonify({"target": target, "exists": exists, "file": f})

    return jsonify({"error": f"无效的 target: {target}（可选 local / 桶 id / cloud / beijing / bucket2）"}), 400


@app.get("/download")
def download():
    blocked = require_auth()
    if blocked:
        return blocked
    key = (request.args.get("key") or "").lstrip("/")
    if not key:
        return jsonify({"error": "缺少 key 参数"}), 400

    bucket_target = (request.args.get("bucket") or "self").strip()
    try:
        client, bucket = resolve_bucket(bucket_target)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("404", "NoSuchKey", "NotFound") or status == 404:
            return jsonify({"error": f"对象不存在: {key}"}), 404
        return jsonify({"error": f"下载失败: {exc}"}), 502
    except (BotoCoreError, OSError) as exc:
        return jsonify({"error": f"下载失败: {exc}"}), 502

    body = obj["Body"]
    filename = PurePosixPath(key).name or "download"
    size = obj.get("ContentLength") or 0
    dl_id = uuid.uuid4().hex
    with _downloads_lock:
        _active_downloads[dl_id] = {
            "key": key,
            "size": size,
            "transferred": 0,
            "status": "downloading",
            "error": None,
        }
    socketio.emit("download_update", download_payload(dl_id, key, size, 0, "downloading"))

    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}",
    }
    if obj.get("ContentLength") is not None:
        headers["Content-Length"] = str(obj["ContentLength"])
    if obj.get("ContentType"):
        headers["Content-Type"] = obj["ContentType"]

    def generate():
        transferred = 0
        last = 0.0
        try:
            for chunk in stream_body(body):
                transferred += len(chunk)
                now = time.monotonic()
                if now - last >= 0.5 or transferred >= size:
                    last = now
                    update_download(dl_id, transferred=transferred)
                yield chunk
            update_download(dl_id, transferred=size, status="done")
        except GeneratorExit:
            update_download(dl_id, status="failed", error="下载已取消")
            raise
        except Exception as exc:
            update_download(dl_id, status="failed", error=str(exc))
            raise
        finally:
            with _downloads_lock:
                _active_downloads.pop(dl_id, None)

    return Response(generate(), headers=headers)


@app.post("/api/submit")
def api_submit():
    """登记 URL 文件记录（支持单个/批量），可选是否自动下载到服务器本地。

    请求头认证：
        X-API-Key: <key>          或
        Authorization: Bearer <key>

    请求体 JSON：
    {
        "urls": ["https://...", "https://..."],   // 必填，1~50 个链接
        "prefix": "backups/2026",                 // 可选，对象 key 前缀
        "bucket": <桶 id 或 legacy 别名>,          // 可选，目标桶（默认桶）
        "download": "none" | "now" | "serial"     // 可选，登记后的动作（默认 none）
    }

    download 取值：
        "none"   只登记记录，不触发下载（默认；上传到桶由文件列表页手动触发）
        "now"    立即下载到服务器本地（进入下载队列，按提交顺序逐个执行）
        "serial" 与 "now" 行为一致（下载队列固定单并发，逐个执行）

    返回（200 或 400）：
    {
        "submitted": 2,
        "jobs": [
            {"url": "https://...", "file_id": 3, "job_id": 5, "object_key": "abc123.zip", "status": "queued"},
            {"url": "https://...", "file_id": 4, "job_id": null, "object_key": "x.zip", "status": "registered"},
            ...
        ],
        "errors": [
            {"url": "https://bad", "error": "无效的 URL"}
        ]
    }
    """
    if not apikey_ok():
        return jsonify({"error": "未授权：缺少或无效的 apikey"}), 401

    body = request.get_json(silent=True) or {}
    raw_urls = body.get("urls")

    # 也兼容单条 url 字段
    if raw_urls is None:
        single = body.get("url")
        raw_urls = [single] if single else []

    if not raw_urls:
        return jsonify({"error": "请提供 urls 字段（至少一个链接）"}), 400

    if not isinstance(raw_urls, list):
        return jsonify({"error": "urls 必须是数组"}), 400

    if len(raw_urls) > 50:
        return jsonify({"error": "单次最多提交 50 个链接"}), 400

    prefix_raw = body.get("prefix") or default_prefix()
    try:
        prefix = clean_prefix(prefix_raw)
    except argparse.ArgumentTypeError as exc:
        return jsonify({"error": f"prefix 无效: {exc}"}), 400

    # 目标桶：bucket / bucket_id 字段接受桶 id / legacy 别名（缺省=默认桶）
    try:
        bucket_row = resolve_bucket_ref(body.get("bucket") or body.get("bucket_id"))
        entry = get_bucket_entry(bucket_row)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    # 登记后的动作：none=只登记（默认）/ now=立即下载到本地（下载并行道）/ serial=排队串行执行
    download_mode = str(body.get("download") or "none").strip().lower()
    if download_mode not in ("none", "now", "serial", "queue"):
        return jsonify({"error": "download 取值：none（默认，不下载）/ now（立即下载到本地）/ serial（放入排队）"}), 400
    serial = download_mode in ("serial", "queue")
    root = None
    if download_mode != "none":
        root = server_root()
        if root is None:
            return jsonify({"error": "未配置 SERVER_FILE_ROOT，无法下载到服务器本地"}), 400
        root.mkdir(parents=True, exist_ok=True)

    submitted: list[dict] = []
    errors: list[dict] = []

    for url in raw_urls:
        url = (url or "").strip()
        if not url.startswith(("http://", "https://")):
            errors.append({"url": url, "error": "链接必须以 http:// 或 https:// 开头"})
            continue
        filename = PurePosixPath(urlparse(url).path).name or "download"
        object_key = build_object_key(prefix, filename)
        # 同名拒传：冲突放进 errors，不中断其它链接
        if object_exists(entry["client"], entry["bucket_name"], object_key):
            errors.append({"url": url, "error": f"同名文件已存在：{object_key}"})
            continue
        job_id = None
        if root is not None:
            # URL → 服务器本地（fetch）：now 进下载并行道、serial 进串行道
            job_id = insert_job(
                kind="fetch",
                filename=filename,
                object_key=object_key,
                size=0,
                source=url,
                destination=str(root / filename),
                serial=serial,
            )
        file_id = insert_file_pending(
            job_id=job_id,
            object_key=object_key,
            filename=filename,
            size=0,
            source_url=url,
        )
        if job_id is not None:
            enqueue_job(job_id, "fetch", serial)
            emit_job_update(job_id)
        submitted.append({
            "url": url,
            "file_id": file_id,
            "job_id": job_id,
            "object_key": object_key,
            "filename": filename,
            "status": "queued" if job_id is not None else "registered",
        })

    return jsonify({
        "submitted": len(submitted),
        "jobs": submitted,
        "errors": errors,
    }), (200 if submitted else 400)


@app.get("/api/status/<int:job_id>")
def api_status(job_id: int):
    """查询单个任务及其对应文件的状态。

    返回：
    {
        "job": {
            "id": 5, "status": "done", "kind": "fetch",
            "filename": "photo.jpg", "object_key": "abc123.jpg",
            "source": "https://...", "size": 10240, "progress": 10240,
            "error": null, "created_at": 1234567890
        },
        "file": {
            "id": 3, "status": "synced", "md5": "abc...", "uploaded": 1,
            "bucket": "mybucket", "object_key": "abc123.jpg",
            "synced_at": 1234567895
        } | null
    }
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    with get_db() as conn:
        job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if job is None:
            return jsonify({"error": f"任务 {job_id} 不存在"}), 404
        file_row = conn.execute("SELECT * FROM files WHERE job_id=?", (job_id,)).fetchone()
        uploaded_ids: set[int] = set()
        if file_row is not None:
            uploaded_ids = {
                u["bucket_id"] for u in conn.execute(
                    "SELECT bucket_id FROM file_uploads WHERE file_id=?", (file_row["id"],)
                ).fetchall()
            }
        legacy_rows = conn.execute(
            "SELECT id, legacy_key FROM buckets WHERE legacy_key IS NOT NULL"
        ).fetchall()
    legacy_map = {r["id"]: r["legacy_key"] for r in legacy_rows}
    legacy_flags = {key: 0 for key in LEGACY_FLAG_COLUMNS}
    for bucket_id in uploaded_ids:
        legacy_key = legacy_map.get(bucket_id)
        if legacy_key in legacy_flags:
            legacy_flags[legacy_key] = 1

    result = {"job": job_payload(dict(job))}
    if file_row is not None:
        f = dict(file_row)
        result["file"] = {
            "id": f["id"],
            "status": f["status"],
            "md5": f["md5"],
            "size": f["size"],
            # 旧 3 布尔从 file_uploads 派生（机器人兼容）
            "uploaded": legacy_flags["self"],
            "uploaded_beijing": legacy_flags["beijing"],
            "uploaded_bucket2": legacy_flags["bucket2"],
            "uploaded_bucket_ids": sorted(uploaded_ids),
            "bucket": f["bucket"],
            "object_key": f["object_key"],
            "synced_at": f["synced_at"],
            "error": f.get("error"),
        }
    else:
        result["file"] = None

    return jsonify(result)


@app.get("/api/jobs")
def api_jobs():
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    return jsonify([job_payload(job) for job in recent_jobs()])


@app.delete("/api/jobs/<int:job_id>")
def api_job_delete(job_id: int):
    """删除一条任务记录（仅限已结束的历史记录；进行中的任务需先取消）。

    删除仅清理任务日志，不影响文件记录与桶内对象；
    关联文件记录的 job_id 置空，并广播 job_removed 供前端移除该行。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if row is None:
        return jsonify({"error": f"任务 {job_id} 不存在"}), 404
    job = dict(row)
    if job["status"] in ("queued", "uploading"):
        return jsonify({
            "error": "任务进行中，无法删除记录（请先取消）",
            "job": job_payload(job),
        }), 400

    with get_db() as conn:
        conn.execute("UPDATE files SET job_id=NULL WHERE job_id=?", (job_id,))
        conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))

    socketio.emit("job_removed", {"id": job_id})
    return jsonify({"status": "ok", "message": "任务记录已删除"})


@app.post("/api/jobs/<int:job_id>/cancel")
def api_job_cancel(job_id: int):
    """请求取消一个任务（上传/下载/抓取）。

    仅对 queued/uploading 状态的任务有效；已结束的任务忽略。
    返回当前 job 状态，取消后 worker 会通过 job_update 推送 cancelled。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if row is None:
        return jsonify({"error": f"任务 {job_id} 不存在"}), 404
    job = dict(row)
    if job["status"] not in ("queued", "uploading"):
        return jsonify({
            "error": f"任务已处于 {job['status']} 状态，无法取消",
            "job": job_payload(job),
        }), 400

    request_cancel(job_id)

    # 若任务还在队列中尚未被 worker 取出，直接标记取消，避免 worker 取出后白跑
    if job["status"] == "queued":
        with get_db() as conn:
            conn.execute(
                "UPDATE jobs SET status='cancelled', error='任务已取消', finished_at=? WHERE id=?",
                (time.time(), job_id),
            )
        clear_cancel(job_id)
        emit_job_update(job_id)

    with get_db() as conn:
        updated = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    return jsonify({"status": "ok", "message": "已请求取消。", "job": job_payload(dict(updated))})


@app.post("/api/jobs/<int:job_id>/pause")
def api_job_pause(job_id: int):
    """请求暂停一个任务（排队中或上传中）。

    暂停后：排队中的任务会被 worker 跳过；上传中的任务在下次回调时阻塞。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if row is None:
        return jsonify({"error": f"任务 {job_id} 不存在"}), 404
    job = dict(row)
    if job["status"] not in ("queued", "uploading"):
        return jsonify({
            "error": f"任务已处于 {job['status']} 状态，无法暂停",
            "job": job_payload(job),
        }), 400

    request_pause(job_id)
    with get_db() as conn:
        conn.execute("UPDATE jobs SET paused=1 WHERE id=?", (job_id,))
        updated = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    emit_job_update(job_id)
    return jsonify({"status": "ok", "message": "已暂停", "job": job_payload(dict(updated))})


@app.post("/api/jobs/<int:job_id>/resume")
def api_job_resume(job_id: int):
    """恢复已暂停的任务。

    - 排队中的任务：被 worker 跳过后需要重新入队
    - 上传中的任务：解除回调阻塞，传输继续
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if row is None:
        return jsonify({"error": f"任务 {job_id} 不存在"}), 404
    job = dict(row)
    if not job.get("paused"):
        return jsonify({
            "error": f"任务未处于暂停状态（当前 {job['status']}），无法继续",
            "job": job_payload(job),
        }), 400

    request_resume(job_id)
    with get_db() as conn:
        conn.execute("UPDATE jobs SET paused=0 WHERE id=?", (job_id,))
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    job = dict(row)
    if job["status"] == "queued":
        enqueue_job(job_id, job["kind"], bool(job.get("serial")))
    emit_job_update(job_id)
    return jsonify({"status": "ok", "message": "已恢复", "job": job_payload(job)})


def concurrency_payload() -> dict:
    """上传/下载道的当前并发（固定 1：单并发 FIFO，同类任务严格逐个执行）。"""
    return {
        "upload": 1,
        "download": 1,
        "max": 1,
    }


@app.get("/api/concurrency")
def api_concurrency_get():
    """查询上传/下载两条道的并发（兼容保留：固定单并发）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    return jsonify(concurrency_payload())


@app.post("/api/concurrency")
def api_concurrency_set():
    """已退役：上传/下载固定单并发 FIFO，不再支持调整（保留端点兼容旧客户端）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    return jsonify({
        "status": "ok",
        "message": "上传/下载队列已固定为单并发（逐个执行），无需调整",
        **concurrency_payload(),
    })


@app.get("/api/auth")
def api_auth():
    """鉴权探测：返回应用基本信息（bucket、安全状态、默认前缀）。

    前端 AuthGuard 用它校验 apikey 是否有效（无 key → 401）。
    单独端点而非复用 / ：因为前端 Vite dev server / nginx 的 / 是 SPA 入口，
    不会被代理到后端，无法用于鉴权探测。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    buckets = get_buckets()
    default = next((b for b in buckets if b["is_default"]), None)
    beijing = next((b for b in buckets if b.get("legacy_key") == "beijing"), None)
    bucket2 = next((b for b in buckets if b.get("legacy_key") == "bucket2"), None)
    return jsonify({
        "app": "buckethub",
        "bucket": default["bucket_name"] if default else "",
        "default_prefix": default_prefix(),
        "bucket_private": BUCKET_PRIVATE,
        "bucket_private_note": BUCKET_PRIVATE_NOTE,
        "concurrency": concurrency_payload(),
        # 旧字段保留（从 buckets 表派生，旧前端/机器人可用）
        "beijing_enabled": bool(beijing and beijing["enabled"]),
        "beijing_bucket": beijing["bucket_name"] if beijing else "",
        "bucket2_enabled": bool(bucket2 and bucket2["enabled"]),
        "bucket2_bucket": bucket2["bucket_name"] if bucket2 else "",
        "buckets": [
            {
                "id": b["id"],
                "name": b["name"],
                "bucket_name": b["bucket_name"],
                "legacy_key": b.get("legacy_key"),
                "is_default": bool(b["is_default"]),
            }
            for b in buckets
        ],
    })


def _bucket_health_error(exc: Exception) -> dict:
    return {
        "ok": False,
        "error": str(exc),
        "latency_ms": None,
        "status_code": None,
        "endpoint": None,
        "addressing_style": None,
        "region": None,
        "versioning": None,
        "public": None,
        "redundancy": None,
        "storage_class": None,
    }


@app.get("/api/bucket-health")
def api_bucket_health():
    """检测所有已启用桶的连通性 + 元数据（head_bucket 等）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    result = {"buckets": []}
    for b in get_buckets():
        try:
            entry = get_bucket_entry(b)
            endpoint_url, _ = _resolve_bucket_endpoint(b.get("endpoint"), b.get("region"))
            health = check_bucket_health(
                entry["client"], b["bucket_name"], endpoint_url,
                b.get("addressing_style") or "auto",
            )
        except ValueError as exc:
            health = _bucket_health_error(exc)
        result["buckets"].append({
            "id": b["id"],
            "name": b["name"],
            "bucket_name": b["bucket_name"],
            "legacy_key": b.get("legacy_key"),
            "is_default": bool(b["is_default"]),
            "signature_version": b.get("signature_version") or "s3v4",
            "health": health,
        })
    return jsonify(result)


def _file_item(conn, row) -> dict:
    """files 行 → 前端 FileItem（补 uploaded_bucket_ids 派生字段，单行端点共用）。"""
    d = dict(row)
    d["uploaded_bucket_ids"] = [
        u["bucket_id"] for u in conn.execute(
            "SELECT bucket_id FROM file_uploads WHERE file_id=?", (d["id"],)
        ).fetchall()
    ]
    return d


@app.get("/api/files")
def api_files():
    """文件库分页列表（DataTable 数据源）。

    查询参数：
        page      页码，从 1 起（默认 1）
        page_size 每页条数，1~200（默认 20）
        q         可选，按 filename / source_url 模糊匹配
        status    可选，按 status 精确过滤

    返回：
        {
          "items": [ {...file 列...} ],
          "total": 123, "page": 1, "page_size": 20
        }
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    page = max(1, request.args.get("page", 1, type=int))
    page_size = min(200, max(1, request.args.get("page_size", 20, type=int)))
    offset = (page - 1) * page_size
    q = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip()

    where_sql = ""
    params: list = []
    conditions: list[str] = []
    if q:
        # 空格分词：每个词都需命中（filename / source_url / object_key 任一列）
        for tok in q.split():
            like = f"%{tok}%"
            conditions.append(
                "(filename LIKE ? OR source_url LIKE ? OR object_key LIKE ?)"
            )
            params.extend([like, like, like])
    if status:
        conditions.append("status = ?")
        params.append(status)
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM files {where_sql}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM files {where_sql} ORDER BY id DESC LIMIT ? OFFSET ?",
            [*params, page_size, offset],
        ).fetchall()
        # 按页内 file id 批查上传关联 → uploaded_bucket_ids
        uploads_map: dict[int, list[int]] = {}
        file_ids = [r["id"] for r in rows]
        if file_ids:
            placeholders = ", ".join("?" for _ in file_ids)
            for u in conn.execute(
                f"SELECT file_id, bucket_id FROM file_uploads WHERE file_id IN ({placeholders})",
                file_ids,
            ).fetchall():
                uploads_map.setdefault(u["file_id"], []).append(u["bucket_id"])

    items = []
    for r in rows:
        d = dict(r)
        d["uploaded_bucket_ids"] = uploads_map.get(d["id"], [])
        items.append(d)

    return jsonify({
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    })


@app.get("/api/files/<int:file_id>")
def api_file_get(file_id: int):
    """查询单个文件记录（前端行级刷新用，返回结构与列表项一致）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    with get_db() as conn:
        row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
        if row is None:
            return jsonify({"error": f"文件记录 {file_id} 不存在"}), 404
        item = _file_item(conn, row)
    return jsonify(item)


@app.get("/api/scripts")
def api_scripts():
    """数据源列表（供文件表格「数据源」列做 id→名称映射）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    return jsonify(recent_scripts())


def _check_download_bucket(conn, value) -> int:
    """校验下载桶 id 合法且存在于 buckets 表（只查存在性，不查 enabled），返回 int。"""
    raw = str(value if value is not None else "").strip()
    if not raw.isdigit():
        raise ValueError("配置 B2 桶下载源时必须指定有效的桶")
    found = conn.execute("SELECT id FROM buckets WHERE id=?", (int(raw),)).fetchone()
    if found is None:
        raise ValueError(f"桶 {raw} 不存在")
    return int(raw)


@app.post("/api/scripts")
def api_create_script():
    """新增数据源（JSON：name 必填；script_path / description 可选）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"error": "数据源名称必填"}), 400
    script_path = (body.get("script_path") or "").strip()
    description = (body.get("description") or "").strip()
    now = time.time()
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO datasources (name, script_path, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (name, script_path or None, description or None, now, now),
        )
        datasource_id = cursor.lastrowid
    return jsonify({
        "status": "ok",
        "message": f"数据源「{name}」已添加。",
        "datasource_id": datasource_id,
    }), 201


@app.patch("/api/scripts/<int:datasource_id>")
def api_update_script(datasource_id: int):
    """编辑数据源（子集更新：name / script_path / description；可选字段空串即清空）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    body = request.get_json(silent=True) or {}
    with get_db() as conn:
        row = conn.execute("SELECT * FROM datasources WHERE id=?", (datasource_id,)).fetchone()
        if row is None:
            return jsonify({"error": f"数据源 {datasource_id} 不存在"}), 404
        sets: list[str] = []
        values: list = []
        name = (body.get("name") or "").strip()
        if name:
            sets.append("name=?")
            values.append(name)
        for field in ("script_path", "description"):
            if field in body:
                val = (body.get(field) or "").strip()
                sets.append(f"{field}=?")
                values.append(val or None)
        if not sets:
            return jsonify({"error": "没有可更新的字段"}), 400
        sets.append("updated_at=?")
        values.append(time.time())
        values.append(datasource_id)
        conn.execute(f"UPDATE datasources SET {', '.join(sets)} WHERE id=?", values)
    return jsonify({"status": "ok", "message": "数据源已更新。", "datasource_id": datasource_id})


@app.delete("/api/scripts/<int:datasource_id>")
def api_delete_script(datasource_id: int):
    """删除数据源，同时解除 files 的关联。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    with get_db() as conn:
        row = conn.execute("SELECT * FROM datasources WHERE id=?", (datasource_id,)).fetchone()
        if row is None:
            return jsonify({"error": f"数据源 {datasource_id} 不存在"}), 404
        conn.execute("DELETE FROM datasources WHERE id=?", (datasource_id,))
        conn.execute("UPDATE files SET datasource_id=NULL WHERE datasource_id=?", (datasource_id,))
    return jsonify({
        "status": "ok",
        "message": f"数据源「{row['name']}」已删除。",
        "datasource_id": datasource_id,
    })


def _bucket_public_row(b: dict) -> dict:
    """桶行的对外表示：永不返回 application_key 明文。"""
    return {
        "id": b["id"],
        "name": b["name"],
        "bucket_name": b["bucket_name"],
        "application_key_id": b["application_key_id"],
        "has_application_key": True,
        "endpoint": b.get("endpoint"),
        "region": b.get("region"),
        "addressing_style": b.get("addressing_style") or "auto",
        "signature_version": b.get("signature_version") or "s3v4",
        "legacy_key": b.get("legacy_key"),
        "is_default": bool(b["is_default"]),
        "enabled": bool(b["enabled"]),
        "sort_order": b["sort_order"],
        "created_at": b["created_at"],
        "updated_at": b["updated_at"],
    }


@app.get("/api/buckets")
def api_buckets():
    """桶列表（含禁用桶；不返回 application_key）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    return jsonify([_bucket_public_row(b) for b in get_buckets(enabled_only=False)])


@app.post("/api/buckets")
def api_create_bucket():
    """新增桶。

    请求体 JSON：name, bucket_name, application_key_id, application_key（必填）；
    endpoint / region / addressing_style(auto|virtual|path) /
    signature_version(s3v4|s3) / sort_order / is_default（可选）。
    首个桶强制为默认桶；设默认则同事务清掉其它桶的 is_default。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    bucket_name = (body.get("bucket_name") or "").strip()
    key_id = (body.get("application_key_id") or "").strip()
    key = (body.get("application_key") or "").strip()
    if not name:
        return jsonify({"error": "名称必填"}), 400
    if not bucket_name:
        return jsonify({"error": "桶名（bucket_name）必填"}), 400
    if not key_id or not key:
        return jsonify({"error": "application_key_id 与 application_key 必填"}), 400
    addressing = (body.get("addressing_style") or "auto").strip()
    if addressing not in ("auto", "virtual", "path"):
        return jsonify({"error": "addressing_style 仅支持 auto/virtual/path"}), 400
    signature = (body.get("signature_version") or "s3v4").strip()
    if signature not in ("s3", "s3v4"):
        return jsonify({"error": "signature_version 仅支持 s3v4/s3"}), 400
    try:
        sort_order = int(body.get("sort_order") or 0)
    except (TypeError, ValueError):
        sort_order = 0

    now = time.time()
    with get_db() as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM buckets").fetchone()["c"]
        is_default = 1 if (count == 0 or body.get("is_default")) else 0
        if is_default:
            conn.execute("UPDATE buckets SET is_default=0 WHERE is_default=1")
        cursor = conn.execute(
            "INSERT INTO buckets (name, bucket_name, application_key_id, application_key, "
            "endpoint, region, addressing_style, signature_version, is_default, enabled, sort_order, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)",
            (
                name,
                bucket_name,
                key_id,
                key,
                (body.get("endpoint") or "").strip() or None,
                (body.get("region") or "").strip() or None,
                addressing,
                signature,
                is_default,
                sort_order,
                now,
                now,
            ),
        )
        bucket_id = cursor.lastrowid
    return jsonify({
        "status": "ok",
        "message": f"桶「{name}」已添加。",
        "bucket_id": bucket_id,
    }), 201


@app.patch("/api/buckets/<int:bucket_id>")
def api_update_bucket(bucket_id: int):
    """编辑桶（子集更新）。application_key 为空/缺省 = 保留旧值。

    凭证 / endpoint / 桶名 / addressing 变更后失效缓存的客户端。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    body = request.get_json(silent=True) or {}
    with get_db() as conn:
        row = conn.execute("SELECT * FROM buckets WHERE id=?", (bucket_id,)).fetchone()
        if row is None:
            return jsonify({"error": f"桶 {bucket_id} 不存在"}), 404
        current = dict(row)

        sets: list[str] = []
        values: list = []
        for field in ("name", "bucket_name", "application_key_id", "application_key"):
            val = (body.get(field) or "").strip()
            if val:  # 空 = 保留旧值
                sets.append(f"{field}=?")
                values.append(val)
        for field in ("endpoint", "region"):
            if field in body:
                val = (body.get(field) or "").strip()
                sets.append(f"{field}=?")
                values.append(val or None)
        if "addressing_style" in body:
            addressing = (body.get("addressing_style") or "auto").strip()
            if addressing not in ("auto", "virtual", "path"):
                return jsonify({"error": "addressing_style 仅支持 auto/virtual/path"}), 400
            sets.append("addressing_style=?")
            values.append(addressing)
        if "signature_version" in body:
            signature = (body.get("signature_version") or "s3v4").strip()
            if signature not in ("s3", "s3v4"):
                return jsonify({"error": "signature_version 仅支持 s3v4/s3"}), 400
            sets.append("signature_version=?")
            values.append(signature)
        if "sort_order" in body:
            try:
                sets.append("sort_order=?")
                values.append(int(body.get("sort_order") or 0))
            except (TypeError, ValueError):
                return jsonify({"error": "sort_order 必须是整数"}), 400
        if "enabled" in body:
            sets.append("enabled=?")
            values.append(1 if body.get("enabled") else 0)
        if "is_default" in body:
            if body.get("is_default"):
                sets.append("is_default=1")
                conn.execute("UPDATE buckets SET is_default=0 WHERE is_default=1 AND id<>?", (bucket_id,))
            elif current["is_default"]:
                return jsonify({"error": "必须保留一个默认桶"}), 400
        if not sets:
            return jsonify({"error": "没有可更新的字段"}), 400
        sets.append("updated_at=?")
        values.append(time.time())
        values.append(bucket_id)
        conn.execute(f"UPDATE buckets SET {', '.join(sets)} WHERE id=?", values)

    invalidate_bucket_client(bucket_id)
    return jsonify({"status": "ok", "message": "桶配置已更新。", "bucket_id": bucket_id})


@app.post("/api/buckets/reorder")
def api_reorder_buckets():
    """拖动排序：按传入的桶 id 顺序整体重写 sort_order（0..n-1）。

    请求体 JSON：{"bucket_ids": [3, 1, 2]}。
    必须包含全部桶 id（避免遗漏导致排序不一致）。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    body = request.get_json(silent=True) or {}
    ids = body.get("bucket_ids")
    if not isinstance(ids, list) or not all(isinstance(i, int) for i in ids):
        return jsonify({"error": "bucket_ids 必须是整数数组"}), 400
    with get_db() as conn:
        existing = {r["id"] for r in conn.execute("SELECT id FROM buckets").fetchall()}
    if set(ids) != existing or len(ids) != len(existing):
        return jsonify({"error": "bucket_ids 必须与当前桶列表完全一致"}), 400
    with get_db() as conn:
        for idx, bucket_id in enumerate(ids):
            conn.execute(
                "UPDATE buckets SET sort_order=?, updated_at=? WHERE id=?",
                (idx, time.time(), bucket_id),
            )
    return jsonify({"status": "ok", "message": "桶顺序已更新。", "bucket_ids": ids})


@app.delete("/api/buckets/<int:bucket_id>")
def api_delete_bucket(bucket_id: int):
    """删除桶（默认桶拒绝删除）。

    先 fail 该桶 queued/uploading 任务（error='桶已删除'），
    再清 file_uploads 关联、jobs.bucket_id 置空、文件级下载源引用回退未配置、
    删行、失效缓存客户端。
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    with get_db() as conn:
        row = conn.execute("SELECT * FROM buckets WHERE id=?", (bucket_id,)).fetchone()
        if row is None:
            return jsonify({"error": f"桶 {bucket_id} 不存在"}), 404
        if row["is_default"]:
            return jsonify({"error": "不能删除默认桶，请先把其它桶设为默认"}), 400
        now = time.time()
        conn.execute(
            "UPDATE jobs SET status='failed', error='桶已删除', finished_at=? "
            "WHERE bucket_id=? AND status IN ('queued','uploading')",
            (now, bucket_id),
        )
        conn.execute("DELETE FROM file_uploads WHERE bucket_id=?", (bucket_id,))
        conn.execute("UPDATE jobs SET bucket_id=NULL WHERE bucket_id=?", (bucket_id,))
        conn.execute(
            "UPDATE files SET download_kind=NULL, download_bucket_id=NULL WHERE download_bucket_id=?",
            (bucket_id,),
        )
        conn.execute("DELETE FROM buckets WHERE id=?", (bucket_id,))
    invalidate_bucket_client(bucket_id)
    return jsonify({"status": "ok", "message": f"桶「{row['name']}」已删除。", "bucket_id": bucket_id})


@app.post("/api/buckets/<int:bucket_id>/test")
def api_test_bucket(bucket_id: int):
    """连通性测试：用临时 client 跑 check_bucket_health（不写入注册表）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    with get_db() as conn:
        row = conn.execute("SELECT * FROM buckets WHERE id=?", (bucket_id,)).fetchone()
    if row is None:
        return jsonify({"error": f"桶 {bucket_id} 不存在"}), 404
    b = dict(row)
    try:
        client = _build_client(b)
        endpoint_url, _ = _resolve_bucket_endpoint(b.get("endpoint"), b.get("region"))
    except ValueError as exc:
        return jsonify(_bucket_health_error(exc))
    return jsonify(
        check_bucket_health(client, b["bucket_name"], endpoint_url, b.get("addressing_style") or "auto")
    )


@app.delete("/api/objects")
def api_delete_object():
    """删除 bucket 中的一个对象。

    请求体 JSON：
        {"key": "path/to/object.zip", "bucket": <桶 id 或 self|bucket2|beijing 别名>}

    返回：
        {"deleted": true, "key": "path/to/object.zip"}
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    body = request.get_json(silent=True) or {}
    key = (body.get("key") or "").strip().lstrip("/")
    if not key:
        return jsonify({"error": "缺少 key 参数"}), 400

    bucket_target = str(body.get("bucket") or "self").strip()
    try:
        bucket_row = resolve_bucket_ref(bucket_target)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    try:
        entry = get_bucket_entry(bucket_row)
        entry["client"].delete_object(Bucket=entry["bucket_name"], Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchKey", "NotFound"):
            return jsonify({"error": f"对象不存在: {key}"}), 404
        return jsonify({"error": str(exc)}), 502
    except (BotoCoreError, OSError) as exc:
        return jsonify({"error": str(exc)}), 502

    # 同步更新本地 files 表：清该桶上传标记；默认桶另标记 status=deleted
    is_default = bool(bucket_row.get("is_default"))
    with get_db() as conn:
        rows = conn.execute("SELECT id FROM files WHERE object_key=?", (key,)).fetchall()
        for r in rows:
            apply_upload_flag(conn, r["id"], bucket_row, False)
        if is_default and rows:
            conn.execute(
                "UPDATE files SET status='deleted', updated_at=? WHERE object_key=?",
                (time.time(), key),
            )

    return jsonify({"deleted": True, "key": key})


@app.post("/api/objects/rename")
def api_rename_object():
    """重命名/移动桶内对象（copy + delete）。

    请求体 JSON：
        {"bucket": "self|beijing", "from_key": "old/path", "to_key": "new/path"}

    返回：
        {"ok": true, "from_key": "...", "to_key": "..."}
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    body = request.get_json(silent=True) or {}
    target = str(body.get("bucket") or "self").strip()
    from_key = (body.get("from_key") or "").strip().lstrip("/")
    to_key = (body.get("to_key") or "").strip().lstrip("/")

    if not from_key or not to_key:
        return jsonify({"error": "缺少 from_key 或 to_key"}), 400
    if from_key == to_key:
        return jsonify({"error": "新名称与原名称相同"}), 400

    try:
        client, bucket_name = resolve_bucket(target)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    source = {"Bucket": bucket_name, "Key": from_key}
    try:
        # 先取对象大小：>5GB 走 multipart 分段服务端复制（单次 copy_object 上限 5GB）
        head = client.head_object(Bucket=bucket_name, Key=from_key)
        size = int(head.get("ContentLength") or 0)
        if size > 5 * 1024 * 1024 * 1024:
            cfg = TransferConfig(
                multipart_threshold=64 * 1024 * 1024,
                multipart_chunksize=64 * 1024 * 1024,
                max_concurrency=4,
                use_threads=True,
            )
            client.copy(CopySource=source, Bucket=bucket_name, Key=to_key, Config=cfg)
        else:
            client.copy_object(Bucket=bucket_name, Key=to_key, CopySource=source)
        client.delete_object(Bucket=bucket_name, Key=from_key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchKey", "NotFound"):
            return jsonify({"error": f"源对象不存在: {from_key}"}), 404
        return jsonify({"error": str(exc)}), 502
    except (BotoCoreError, OSError) as exc:
        return jsonify({"error": str(exc)}), 502

    # 同步更新 files 表的 object_key
    with get_db() as conn:
        conn.execute(
            "UPDATE files SET object_key=?, updated_at=? WHERE object_key=?",
            (to_key, time.time(), from_key),
        )

    return jsonify({"ok": True, "from_key": from_key, "to_key": to_key})


@app.delete("/api/files/<int:file_id>")
def api_delete_file(file_id: int):
    """删除文件记录（仅删除数据库记录，不删除任何桶中的文件对象）。"""
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    with get_db() as conn:
        row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
    if row is None:
        return jsonify({"error": f"文件记录 {file_id} 不存在"}), 404

    with get_db() as conn:
        conn.execute("DELETE FROM files WHERE id=?", (file_id,))

    return jsonify({"deleted": True, "file_id": file_id})


@app.patch("/api/files/<int:file_id>")
def api_update_file(file_id: int):
    """手动编辑文件记录（所有字段均可选，仅更新提供的字段）。

    请求体 JSON（均可选）：
        filename, object_key, bucket   字符串（object_key/bucket 空串也写入）
        md5, source_url, local_path, error  可空字符串（null/空→NULL）
        size                           整数
        uploaded_bucket_ids            number[]（替换语义：勾选的桶集合）
        uploaded, uploaded_beijing, uploaded_bucket2  旧布尔（机器人兼容，映射 file_uploads）
        status                         pending|synced|failed|deleted|cancelled
        datasource_id                  整数或 null
        download_kind                  文件级下载源 none|url|local|bucket（'none'/空 → 清空）；
                                       kind=bucket 需带 download_bucket_id（校验桶存在）
        download_bucket_id             仅当当前 download_kind=bucket 时可单独更新

    返回：
        {"status": "ok", "file_id": id, "file": {更新后的行}}
    """
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401

    body = request.get_json(silent=True) or {}
    fields: list[str] = []
    params: list = []

    # uploaded_bucket_ids: number[]（替换语义，统一走 apply_upload_flag）
    target_ids: set[int] | None = None
    if "uploaded_bucket_ids" in body:
        ids = body.get("uploaded_bucket_ids") or []
        if not isinstance(ids, list):
            return jsonify({"error": "uploaded_bucket_ids 必须是数组"}), 400
        target_ids = set()
        for bid in ids:
            try:
                target_ids.add(resolve_bucket_ref(bid)["id"])
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400

    # 旧 3 布尔仍接受（映射到对应桶的 file_uploads）
    legacy_updates: list[tuple[dict, bool]] = []
    for key, legacy_key in (
        ("uploaded", "self"),
        ("uploaded_beijing", "beijing"),
        ("uploaded_bucket2", "bucket2"),
    ):
        if key in body:
            try:
                legacy_updates.append((resolve_bucket_ref(legacy_key), bool(body[key])))
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400

    # 可空字符串字段：null/空字符串 → NULL
    for key in ("filename", "md5", "source_url", "local_path", "error"):
        if key in body:
            val = body[key]
            if val:
                fields.append(f"{key} = ?")
                params.append(str(val))
            else:
                fields.append(f"{key} = NULL")

    # 非空字符串字段（空串也写入）
    for key in ("object_key", "bucket"):
        if key in body:
            fields.append(f"{key} = ?")
            params.append(str(body[key]))

    # 整数字段
    if "size" in body:
        fields.append("size = ?")
        params.append(int(body["size"] or 0))

    # status 枚举校验
    if "status" in body:
        status_val = str(body["status"])
        if status_val in ("pending", "synced", "failed", "deleted", "cancelled"):
            fields.append("status = ?")
            params.append(status_val)

    # datasource_id 可空整数
    if "datasource_id" in body:
        val = body["datasource_id"]
        if val:
            fields.append("datasource_id = ?")
            params.append(int(val))
        else:
            fields.append("datasource_id = NULL")

    # 文件级下载源：含 download_kind 时整组更新（none → 两列清空）；
    # 只发 download_bucket_id 时仅当当前类型为 bucket 才接受
    download_updates: list[tuple[str, object]] | None = None
    if "download_kind" in body or "download_bucket_id" in body:
        with get_db() as conn:
            if "download_kind" in body:
                kind_val = (body.get("download_kind") or "").strip()
                if kind_val in ("", "none"):
                    download_updates = [("download_kind", None), ("download_bucket_id", None)]
                elif kind_val in ("url", "local"):
                    download_updates = [("download_kind", kind_val), ("download_bucket_id", None)]
                elif kind_val == "bucket":
                    try:
                        bucket_id_value = _check_download_bucket(
                            conn, body.get("download_bucket_id")
                        )
                    except ValueError as exc:
                        return jsonify({"error": str(exc)}), 400
                    download_updates = [
                        ("download_kind", "bucket"),
                        ("download_bucket_id", bucket_id_value),
                    ]
                else:
                    return jsonify({"error": "download_kind 仅支持 url / local / bucket / none"}), 400
            else:
                current = conn.execute(
                    "SELECT download_kind FROM files WHERE id=?", (file_id,)
                ).fetchone()
                if current is None:
                    return jsonify({"error": f"文件记录 {file_id} 不存在"}), 404
                if current["download_kind"] != "bucket":
                    return jsonify({"error": "仅当下载源类型为 B2 桶时才能单独更新下载桶"}), 400
                try:
                    bucket_id_value = _check_download_bucket(
                        conn, body.get("download_bucket_id")
                    )
                except ValueError as exc:
                    return jsonify({"error": str(exc)}), 400
                download_updates = [("download_bucket_id", bucket_id_value)]
        for name, val in download_updates:
            if val is None:
                fields.append(f"{name} = NULL")
            else:
                fields.append(f"{name} = ?")
                params.append(val)

    if not fields and target_ids is None and not legacy_updates:
        return jsonify({"error": "没有可更新的字段"}), 400

    fields.append("updated_at = ?")
    params.append(time.time())
    params.append(file_id)

    with get_db() as conn:
        row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
        if row is None:
            return jsonify({"error": f"文件记录 {file_id} 不存在"}), 404
        conn.execute(
            f"UPDATE files SET {', '.join(fields)} WHERE id = ?", params
        )
        if target_ids is not None:
            bucket_map = {b["id"]: b for b in get_buckets(enabled_only=False)}
            existing = {
                u["bucket_id"] for u in conn.execute(
                    "SELECT bucket_id FROM file_uploads WHERE file_id=?", (file_id,)
                ).fetchall()
            }
            for bid in target_ids:
                if bid in bucket_map:
                    apply_upload_flag(conn, file_id, bucket_map[bid], True)
            for bid in existing - target_ids:
                if bid in bucket_map:
                    apply_upload_flag(conn, file_id, bucket_map[bid], False)
        for bucket_row, value in legacy_updates:
            apply_upload_flag(conn, file_id, bucket_row, value)
        item = _file_item(conn, row)

    return jsonify({"status": "ok", "file_id": file_id, "file": item})


@app.get("/api/objects")
def api_objects():
    if not apikey_ok():
        return jsonify({"error": "未授权"}), 401
    target = request.args.get("bucket", "self").strip() or "self"
    raw_prefix = request.args.get("prefix", "") or default_prefix()
    q = (request.args.get("q") or "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    page_size = max(1, min(200, request.args.get("page_size", 50, type=int)))
    try:
        prefix = clean_prefix(raw_prefix)
    except argparse.ArgumentTypeError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        client, bucket_name = resolve_bucket(target)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        all_objects = list_objects(prefix, limit=None, client=client, bucket_name=bucket_name)
    except (BotoCoreError, ClientError) as exc:
        return jsonify({"error": str(exc)}), 502
    if q:
        # 空格分词：每个词都需命中完整 key（大小写不敏感）
        tokens = [t.lower() for t in q.split()]
        all_objects = [
            o for o in all_objects
            if all(t in o["key"].lower() for t in tokens)
        ]
    total = len(all_objects)
    start = (page - 1) * page_size
    objects = all_objects[start:start + page_size]
    return jsonify({
        "prefix": prefix,
        "bucket": target,
        "objects": objects,
        "total": total,
        "page": page,
        "page_size": page_size,
    })


@app.post("/scripts")
def add_script():
    """新增数据源（名称必填，脚本路径允许为空）。"""
    blocked = require_auth()
    if blocked:
        return blocked
    apikey = request.args.get("apikey", "")
    name = (request.form.get("name") or "").strip()
    script_path = (request.form.get("script_path") or "").strip()
    description = (request.form.get("description") or "").strip()
    if not name:
        return respond("error", "数据源名称必填。", apikey, "error", 400)
    now = time.time()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO datasources (name, script_path, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (name, script_path or None, description or None, now, now),
        )
    return respond("ok", f"数据源「{name}」已添加。", apikey)


@app.post("/scripts/delete")
def delete_script():
    """删除数据源，同时解除 files 的关联。"""
    blocked = require_auth()
    if blocked:
        return blocked
    apikey = request.args.get("apikey", "")
    datasource_id = request.form.get("id", type=int)
    if datasource_id:
        with get_db() as conn:
            conn.execute("DELETE FROM datasources WHERE id=?", (datasource_id,))
            conn.execute("UPDATE files SET datasource_id=NULL WHERE datasource_id=?", (datasource_id,))
        return respond("ok", "数据源已删除。", apikey)
    return respond("error", "缺少数据源 id。", apikey, "error", 400)


def main() -> int:
    parser = argparse.ArgumentParser(description="启动 BucketHub（开发模式）")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="文件变更时自动重启（热加载），仅用于开发",
    )
    args = parser.parse_args()

    config_error = validate_config()
    if config_error:
        print(f"配置错误: {config_error}", file=sys.stderr)
        return 1

    init_runtime()

    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "5000"))
    print(f"BucketHub 已启动: http://{host}:{port}/?apikey=<APP_API_KEY>")
    socketio.run(app, host=host, port=port, use_reloader=args.reload)
    return 0


def init_runtime() -> None:
    """初始化运行环境：建表、恢复任务、清理残留分片、启动上传 worker。

    main()（开发模式）和 wsgi.py（gunicorn）都会调用；
    gunicorn 必须保持单 worker，保证上传队列在同一个进程内。
    """
    init_db()
    recover_jobs()
    # 清理每个已启用桶的残留分片（单桶失败可忽略）
    for row in get_buckets():
        try:
            entry = get_bucket_entry(row)
            cleanup_stale_multipart(
                client=entry["client"], bucket_name=row["bucket_name"]
            )
        except (BotoCoreError, ClientError, ValueError) as exc:
            print(f"清理桶「{row['name']}」的残留分片失败（可忽略）: {exc}")
    if default_bucket() is None:
        print(
            "⚠️ 未配置任何桶，请通过 POST /api/buckets（或页面「桶管理」）添加",
            file=sys.stderr,
        )
    # 上传/下载两条单并发 FIFO 道：各一个常驻 worker 线程，
    # 同一道内严格按提交顺序逐个执行；上传与下载可同时进行。
    threading.Thread(
        target=worker_loop, args=(UPLOAD_QUEUE, None),
        name="upload-worker", daemon=True,
    ).start()
    threading.Thread(
        target=worker_loop, args=(DOWNLOAD_QUEUE, None),
        name="download-worker", daemon=True,
    ).start()

    global BUCKET_PRIVATE, BUCKET_PRIVATE_NOTE
    BUCKET_PRIVATE, BUCKET_PRIVATE_NOTE = check_bucket_private()
    if BUCKET_PRIVATE is False:
        print(f"⚠️ 安全警告: {BUCKET_PRIVATE_NOTE}", file=sys.stderr)
    else:
        print(f"安全检测: {BUCKET_PRIVATE_NOTE}")


if __name__ == "__main__":
    raise SystemExit(main())

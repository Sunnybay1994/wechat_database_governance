#!/usr/bin/env python3
"""从解密后的微信数据库构建治理库 governed.db。

设计依据见同目录 `数据库治理方案.md`。只读源库，不改动原始数据。

产出 5 张表（4 张业务表 + 构建内部 `build_meta`）：
  contacts            联系人（好友/群/公众号/企业微信/群成员）
  chatrooms           群聊专属信息
  chatroom_members    群—成员关系（直接用 username）
  messages            全部聊天记录（合并所有分表，含媒体外链/引用）
  build_meta          增量水位（仅构建脚本使用，下游可忽略）

用法：
  cp accounts.local.example.py accounts.local.py   # 首次：填入本地 wxid 与解密目录
  python3 build_governed_db.py [--limit-chats N] [--out path]
  python3 build_governed_db.py --staging-tmp   # 先写 /tmp，完成再 mv 到 --out
  python3 build_governed_db.py --incremental   # 增量：仅追加新消息，保留已有 governed.db
  python3 build_governed_db.py --incremental --reset-meta  # 重扫全部源表，靠去重键对齐
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path

import zstandard as zstd

# --------------------------------------------------------------------------- #
# 配置（v2：双账号）
# 真实 wxid / 路径请写在 accounts.local.py（见 accounts.local.example.py，已 gitignore）
# --------------------------------------------------------------------------- #
DEFAULT_ACCOUNTS = [
    {
        "account": "personal",
        "owner_wxid": "wxid_personal_owner",
        "decrypted_dir": "../wechat-decrypt-personal/decrypted",
    },
    {
        "account": "work",
        "owner_wxid": "wxid_work_owner",
        "decrypted_dir": "../wechat-decrypt-work/decrypted",
    },
]


def load_accounts() -> list[dict]:
    local = Path(__file__).resolve().parent / "accounts.local.py"
    if not local.exists():
        return DEFAULT_ACCOUNTS
    spec = importlib.util.spec_from_file_location("wechat_accounts_local", local)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载 {local}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.ACCOUNTS


ACCOUNTS = load_accounts()

DEFAULT_OUT = Path(__file__).resolve().parent / "output" / "governed.db"

_zstd = zstd.ZstdDecompressor()

MSG_TYPE_NAMES = {
    1: "text", 3: "image", 34: "voice", 42: "card", 43: "video",
    47: "sticker", 48: "location", 49: "link", 50: "call",
    10000: "system", 10002: "system",
}


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #
def connect_ro(db_path: Path | str) -> sqlite3.Connection:
    """只读 + immutable 连接，避免锁与 -wal/-shm 副作用。"""
    return sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)


def configure_output_db(out: sqlite3.Connection) -> None:
    """写入 governed.db 时的 SQLite 调优（不改变 schema / 下游语义）。"""
    out.execute("PRAGMA journal_mode=WAL")
    out.execute("PRAGMA synchronous=NORMAL")
    out.execute("PRAGMA temp_store=MEMORY")
    out.execute("PRAGMA cache_size=-64000")


def decode_content(raw, ct_flag: int = 0) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        if ct_flag == 4:
            try:
                return _zstd.decompress(raw).decode("utf-8", "replace")
            except Exception:
                return ""
        return raw.decode("utf-8", "replace")
    return str(raw)


def split_msg_type(t) -> tuple[int, int]:
    try:
        t = int(t)
    except (TypeError, ValueError):
        return 0, 0
    if t > 0xFFFFFFFF:
        return t & 0xFFFFFFFF, t >> 32
    return t, 0


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def message_shards(message_dir: Path) -> list[str]:
    """仅返回 message_<数字>.db 分片，排除 message_fts.db / message_resource.db。"""
    out = []
    for p in glob.glob(str(message_dir / "message_*.db")):
        if re.fullmatch(r"message_\d+\.db", os.path.basename(p)):
            out.append(p)
    return sorted(out)


# ---- protobuf (extra_buffer) ---------------------------------------------- #
def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    v = shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        v |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return v, pos


def parse_protobuf(data: bytes) -> list[tuple[int, str, object]]:
    if not data:
        return []
    rows, pos = [], 0
    while pos < len(data):
        try:
            tag, pos = _read_varint(data, pos)
        except Exception:
            break
        fn, wt = tag >> 3, tag & 7
        if wt == 0:
            v, pos = _read_varint(data, pos)
            rows.append((fn, "varint", v))
        elif wt == 2:
            ln, pos = _read_varint(data, pos)
            chunk = data[pos:pos + ln]
            pos += ln
            try:
                s = chunk.decode("utf-8")
                printable = sum(1 for c in s if c.isprintable() or c in "\n\r\t")
                if len(s) > 0 and printable / len(s) > 0.85:
                    rows.append((fn, "string", s))
                else:
                    rows.append((fn, "bytes", chunk))
            except Exception:
                rows.append((fn, "bytes", chunk))
        elif wt == 1:
            pos += 8
        elif wt == 5:
            pos += 4
        else:
            break
    return rows


def parse_extra_buffer(buf: bytes, label_map: dict[int, str]) -> dict:
    """从 extra_buffer 提取：签名/性别/国家/省/市/标签名列表。"""
    out = {"signature": None, "gender": 0, "country": None,
           "province": None, "city": None, "labels": []}
    if not buf:
        return out
    for fn, typ, val in parse_protobuf(buf):
        if fn == 2 and typ == "varint":
            out["gender"] = val            # 0未知 1男 2女（已用数据验证）
        elif fn == 4 and typ == "string":
            out["signature"] = val or None
        elif fn == 5 and typ == "string":
            out["country"] = val or None
        elif fn == 6 and typ == "string":
            out["province"] = val or None
        elif fn == 7 and typ == "string":
            out["city"] = val or None
        elif fn == 30 and typ == "string":
            ids = [x.strip() for x in str(val).split(",") if x.strip().isdigit()]
            out["labels"] = [label_map.get(int(i), str(i)) for i in ids]
    return out


# ---- XML 提取（预编译 regex）------------------------------------------------ #
_TAG_RES: dict[str, re.Pattern[str]] = {}
_ATTR_RES: dict[tuple[str, str], re.Pattern[str]] = {}
_REFERMSG_RE = re.compile(r"<refermsg>(.*?)</refermsg>", re.DOTALL)


def _tag_re(tag: str) -> re.Pattern[str]:
    if tag not in _TAG_RES:
        _TAG_RES[tag] = re.compile(rf"<{re.escape(tag)}>(.*?)</{re.escape(tag)}>", re.DOTALL)
    return _TAG_RES[tag]


def _attr_re(tag: str, attr: str) -> re.Pattern[str]:
    key = (tag, attr)
    if key not in _ATTR_RES:
        _ATTR_RES[key] = re.compile(
            rf"<{re.escape(tag)}\b[^>]*?\b{re.escape(attr)}\s*=\s*\"([^\"]*)\"", re.DOTALL)
    return _ATTR_RES[key]


def xml_tag(content: str, *tags: str) -> str:
    for tag in tags:
        m = _tag_re(tag).search(content)
        if m:
            return m.group(1).strip()
    return ""


def xml_attr(content: str, tag: str, attr: str) -> str:
    m = _attr_re(tag, attr).search(content)
    return m.group(1).strip() if m else ""


def extract_refermsg_block(raw_text: str) -> str | None:
    m = _REFERMSG_RE.search(raw_text)
    return m.group(1) if m else None


def _clean(d: dict) -> dict:
    return {k: v for k, v in d.items() if v not in ("", None)}


def _media_json(media_ref: dict | None) -> str | None:
    if not media_ref:
        return None
    cleaned = _clean(media_ref)
    return json.dumps(cleaned, ensure_ascii=False) if cleaned else None


def interpret_message(base_type: int, raw_text: str) -> tuple[str, str, str | None, int | None]:
    """返回 (type, content, media_ref_json, reply_to_server_id)。"""
    media_ref = None
    reply_to = None
    msg_type = MSG_TYPE_NAMES.get(base_type, f"type_{base_type}")
    content = ""

    if base_type == 1:
        content = raw_text

    elif base_type == 3:  # 图片
        media_ref = _clean({
            "md5": xml_attr(raw_text, "img", "md5"),
            "aeskey": xml_attr(raw_text, "img", "aeskey"),
            "cdnurl": xml_attr(raw_text, "img", "cdnmidimgurl")
                      or xml_attr(raw_text, "img", "cdnbigimgurl")
                      or xml_attr(raw_text, "img", "cdnthumburl"),
            "length": xml_attr(raw_text, "img", "length"),
        })

    elif base_type == 34:  # 语音
        media_ref = _clean({
            "aeskey": xml_attr(raw_text, "voicemsg", "aeskey"),
            "voiceurl": xml_attr(raw_text, "voicemsg", "voiceurl"),
            "voicelength": xml_attr(raw_text, "voicemsg", "voicelength"),
            "length": xml_attr(raw_text, "voicemsg", "length"),
        })

    elif base_type == 43:  # 视频
        media_ref = _clean({
            "md5": xml_attr(raw_text, "videomsg", "md5"),
            "newmd5": xml_attr(raw_text, "videomsg", "newmd5"),
            "aeskey": xml_attr(raw_text, "videomsg", "aeskey"),
            "cdnvideourl": xml_attr(raw_text, "videomsg", "cdnvideourl"),
            "playlength": xml_attr(raw_text, "videomsg", "playlength"),
        })

    elif base_type == 47:  # 表情
        media_ref = _clean({
            "md5": xml_attr(raw_text, "emoji", "md5"),
            "cdnurl": xml_attr(raw_text, "emoji", "cdnurl"),
            "productid": xml_attr(raw_text, "emoji", "productid"),
        })

    elif base_type == 42:  # 名片
        content = xml_attr(raw_text, "msg", "nickname")
        media_ref = _clean({
            "username": xml_attr(raw_text, "msg", "username"),
            "nickname": xml_attr(raw_text, "msg", "nickname"),
        })

    elif base_type == 48:  # 位置
        content = xml_attr(raw_text, "location", "poiname") or xml_attr(raw_text, "location", "label")
        media_ref = _clean({
            "x": xml_attr(raw_text, "location", "x"),
            "y": xml_attr(raw_text, "location", "y"),
            "poiname": xml_attr(raw_text, "location", "poiname"),
            "label": xml_attr(raw_text, "location", "label"),
        })

    elif base_type == 49:  # appmsg：引用 / 链接 / 文件 等
        appmsg_type = xml_tag(raw_text, "type")
        title = xml_tag(raw_text, "title")
        des = xml_tag(raw_text, "des")
        url = xml_tag(raw_text, "url")
        if appmsg_type == "57":
            msg_type = "quote"
            content = title
            rb = extract_refermsg_block(raw_text)
            svrid = xml_tag(rb or "", "svrid")
            reply_to = int(svrid) if svrid.isdigit() else None
        else:
            msg_type = "link"
            parts = [title] if title else []
            if url:
                parts.append(url)
            content = "\n".join(parts)
            media_ref = _clean({"title": title, "des": des, "url": url, "appmsg_type": appmsg_type})

    elif base_type in (10000, 10002):
        content = raw_text

    else:
        content = raw_text[:1000]

    media_json = _media_json(media_ref)
    return msg_type, content, media_json, reply_to


def refermsg_snapshot_from_block(rb: str) -> dict | None:
    rtype = xml_tag(rb, "type")
    base = int(rtype) if rtype.isdigit() else 0
    snap = _clean({
        "sender": xml_tag(rb, "displayname"),
        "type": MSG_TYPE_NAMES.get(base, f"type_{base}"),
        "content": xml_tag(rb, "content"),
        "time": int(xml_tag(rb, "createtime")) if xml_tag(rb, "createtime").isdigit() else None,
    })
    return snap or None


def parse_refermsg_snapshot(raw_text: str) -> dict | None:
    rb = extract_refermsg_block(raw_text)
    return refermsg_snapshot_from_block(rb) if rb else None


# --------------------------------------------------------------------------- #
# 目标库 schema
# --------------------------------------------------------------------------- #
SCHEMA_DROP = """
DROP TABLE IF EXISTS build_meta;
DROP TABLE IF EXISTS contacts;
DROP TABLE IF EXISTS chatrooms;
DROP TABLE IF EXISTS chatroom_members;
DROP TABLE IF EXISTS messages;
"""

SCHEMA_CREATE = """
CREATE TABLE contacts (
    account      TEXT NOT NULL,
    username     TEXT NOT NULL,
    type         TEXT,
    is_friend    INTEGER,
    alias        TEXT,
    nick_name    TEXT,
    remark       TEXT,
    description  TEXT,
    signature    TEXT,
    gender       INTEGER,
    country      TEXT,
    province     TEXT,
    city         TEXT,
    labels       TEXT,
    head_img_url TEXT,
    PRIMARY KEY (account, username)
);

CREATE TABLE chatrooms (
    account           TEXT NOT NULL,
    username          TEXT NOT NULL,
    group_owner       TEXT,
    announcement      TEXT,
    announcement_time INTEGER,
    PRIMARY KEY (account, username)
);

CREATE TABLE chatroom_members (
    account         TEXT NOT NULL,
    room_username   TEXT NOT NULL,
    member_username TEXT NOT NULL,
    PRIMARY KEY (account, room_username, member_username)
);

CREATE TABLE messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account         TEXT NOT NULL,
    chat_username   TEXT,
    local_id        INTEGER,
    server_id       INTEGER,
    timestamp       INTEGER,
    sender_username TEXT,
    is_self         INTEGER,
    type            TEXT,
    content         TEXT,
    media_ref       TEXT,
    reply_to        INTEGER,
    reply_quote     TEXT
);

CREATE TABLE build_meta (
    account       TEXT NOT NULL,
    msg_table     TEXT NOT NULL,
    max_local_id  INTEGER NOT NULL DEFAULT 0,
    max_timestamp INTEGER,
    updated_at    INTEGER NOT NULL,
    PRIMARY KEY (account, msg_table)
);

CREATE UNIQUE INDEX idx_messages_account_server_id
    ON messages(account, server_id) WHERE server_id != 0;
CREATE UNIQUE INDEX idx_messages_account_chat_local_id
    ON messages(account, chat_username, local_id);
CREATE INDEX idx_messages_account_chat ON messages(account, chat_username);
CREATE INDEX idx_messages_reply ON messages(reply_to) WHERE reply_to IS NOT NULL;
CREATE INDEX idx_contacts_account ON contacts(account);
"""

SCHEMA_CREATE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS contacts (
    account      TEXT NOT NULL,
    username     TEXT NOT NULL,
    type         TEXT,
    is_friend    INTEGER,
    alias        TEXT,
    nick_name    TEXT,
    remark       TEXT,
    description  TEXT,
    signature    TEXT,
    gender       INTEGER,
    country      TEXT,
    province     TEXT,
    city         TEXT,
    labels       TEXT,
    head_img_url TEXT,
    PRIMARY KEY (account, username)
);

CREATE TABLE IF NOT EXISTS chatrooms (
    account           TEXT NOT NULL,
    username          TEXT NOT NULL,
    group_owner       TEXT,
    announcement      TEXT,
    announcement_time INTEGER,
    PRIMARY KEY (account, username)
);

CREATE TABLE IF NOT EXISTS chatroom_members (
    account         TEXT NOT NULL,
    room_username   TEXT NOT NULL,
    member_username TEXT NOT NULL,
    PRIMARY KEY (account, room_username, member_username)
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account         TEXT NOT NULL,
    chat_username   TEXT,
    local_id        INTEGER,
    server_id       INTEGER,
    timestamp       INTEGER,
    sender_username TEXT,
    is_self         INTEGER,
    type            TEXT,
    content         TEXT,
    media_ref       TEXT,
    reply_to        INTEGER,
    reply_quote     TEXT
);

CREATE TABLE IF NOT EXISTS build_meta (
    account       TEXT NOT NULL,
    msg_table     TEXT NOT NULL,
    max_local_id  INTEGER NOT NULL DEFAULT 0,
    max_timestamp INTEGER,
    updated_at    INTEGER NOT NULL,
    PRIMARY KEY (account, msg_table)
);
"""

INDEXES_IF_NOT_EXISTS = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_account_server_id
    ON messages(account, server_id) WHERE server_id != 0;
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_account_chat_local_id
    ON messages(account, chat_username, local_id);
CREATE INDEX IF NOT EXISTS idx_messages_account_chat ON messages(account, chat_username);
CREATE INDEX IF NOT EXISTS idx_messages_reply ON messages(reply_to) WHERE reply_to IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_contacts_account ON contacts(account);
"""


def init_full_schema(out: sqlite3.Connection) -> None:
    out.executescript(SCHEMA_DROP + SCHEMA_CREATE)


def _index_exists(out: sqlite3.Connection, name: str) -> bool:
    return out.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name = ?", (name,)
    ).fetchone() is not None


def _dedupe_messages_local_id(out: sqlite3.Connection) -> int:
    """旧库迁移：同一 (account, chat_username, local_id) 保留最小 rowid。"""
    before = out.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    out.execute("""
        DELETE FROM messages
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM messages
            GROUP BY account, chat_username, local_id
        )
    """)
    out.commit()
    removed = before - out.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    if removed:
        print(f"  迁移：去重 messages (account, chat_username, local_id) 删除 {removed:,} 行")
    return removed


def ensure_incremental_schema(out: sqlite3.Connection) -> None:
    """增量模式：保留已有数据，补齐 build_meta 与索引。"""
    required = {"contacts", "chatrooms", "chatroom_members", "messages"}
    existing = {r[0] for r in out.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if not required.issubset(existing):
        missing = required - existing
        raise SystemExit(
            f"governed.db 缺少表 {sorted(missing)}，请先全量构建（不带 --incremental）"
        )
    out.executescript(SCHEMA_CREATE_IF_NOT_EXISTS)
    if not _index_exists(out, "idx_messages_account_chat_local_id"):
        _dedupe_messages_local_id(out)
    out.executescript(INDEXES_IF_NOT_EXISTS)
    out.commit()


def bootstrap_watermarks_if_empty(out: sqlite3.Connection, account: str,
                                    message_dir: Path, md5map: dict[str, str]) -> None:
    """旧库首次增量：从已有 messages 反推各 Msg_* 表水位，避免全表重扫。"""
    n = out.execute(
        "SELECT COUNT(*) FROM build_meta WHERE account = ?", (account,)).fetchone()[0]
    if n > 0:
        return
    chat_max = dict(out.execute("""
        SELECT chat_username, MAX(local_id) FROM messages
        WHERE account = ? GROUP BY chat_username
    """, (account,)))
    if not chat_max:
        return
    print(f"  [{account}] build_meta 为空，从已有 messages 反推水位 ...")
    bootstrapped = 0
    for db in message_shards(message_dir):
        conn = connect_ro(db)
        msg_tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'Msg\\_%' ESCAPE '\\'")]
        conn.close()
        for tbl in msg_tables:
            chat = md5map.get(tbl[4:])
            if not chat:
                continue
            mx = chat_max.get(chat)
            if mx is not None:
                upsert_watermark(out, account, tbl, int(mx), None)
                bootstrapped += 1
    out.commit()
    print(f"  [{account}] 反推 {bootstrapped} 条水位（跳过无历史消息的会话表）")


def load_watermarks(out: sqlite3.Connection, account: str) -> dict[str, int]:
    return dict(out.execute(
        "SELECT msg_table, max_local_id FROM build_meta WHERE account = ?",
        (account,)))


def upsert_watermark(out: sqlite3.Connection, account: str, msg_table: str,
                     max_local_id: int, max_timestamp: int | None) -> None:
    now = int(time.time())
    out.execute("""
        INSERT INTO build_meta (account, msg_table, max_local_id, max_timestamp, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(account, msg_table) DO UPDATE SET
            max_local_id = MAX(build_meta.max_local_id, excluded.max_local_id),
            max_timestamp = CASE
                WHEN excluded.max_timestamp IS NOT NULL AND (
                    build_meta.max_timestamp IS NULL
                    OR excluded.max_timestamp > build_meta.max_timestamp
                ) THEN excluded.max_timestamp
                ELSE build_meta.max_timestamp
            END,
            updated_at = excluded.updated_at
    """, (account, msg_table, max_local_id, max_timestamp, now))


# 兼容旧常量名
SCHEMA = SCHEMA_DROP + SCHEMA_CREATE


# --------------------------------------------------------------------------- #
# 构建步骤
# --------------------------------------------------------------------------- #
def is_chatroom(username: str) -> bool:
    """群聊判断，覆盖两种后缀：`xxx@chatroom` 与新格式 `xxx@im.chatroom`。"""
    return username.endswith("@chatroom") or username.endswith("@im.chatroom")


def is_group_chat(username: str) -> bool:
    """群聊或企业微信会话（用于消息正文去前缀、自我识别时跳过多人会话）。"""
    return is_chatroom(username) or username.endswith("@openim")


def derive_type(username: str) -> str:
    if is_chatroom(username):
        return "chatroom"
    if username.endswith("@openim"):
        return "openim"
    if username.startswith("gh_"):
        return "official"
    return "person"


def build_contacts(out: sqlite3.Connection, account: str,
                   contact_db: Path) -> tuple[dict[int, str], dict[int, str], set[str]]:
    """写 contacts 表，返回 (contact_id→username, room_id→username, 有效username集合)。"""
    src = connect_ro(contact_db)
    label_map = {int(k): v for k, v in src.execute(
        "SELECT label_id_, label_name_ FROM contact_label")}

    id2user = dict(src.execute("SELECT id, username FROM contact WHERE username IS NOT NULL"))
    room_id2user = dict(src.execute("SELECT id, username FROM chat_room WHERE username IS NOT NULL"))

    member_ids = {r[0] for r in src.execute("SELECT DISTINCT member_id FROM chatroom_member")}
    member_usernames = {id2user[m] for m in member_ids if m in id2user}

    rows = src.execute("""
        SELECT username, local_type, alias, nick_name, remark, description,
               big_head_url, extra_buffer
        FROM contact WHERE username IS NOT NULL AND length(username) > 0
    """).fetchall()
    src.close()

    kept = []
    valid = set()
    for (username, local_type, alias, nick, remark, desc, head, extra) in rows:
        is_stranger = (local_type == 3)
        if is_stranger and username not in member_usernames:
            continue  # 真·孤立陌生人，丢弃
        eb = parse_extra_buffer(extra, label_map)
        kept.append((
            account, username, derive_type(username), 0 if is_stranger else 1,
            alias, nick, remark, desc,
            eb["signature"], eb["gender"], eb["country"], eb["province"], eb["city"],
            json.dumps(eb["labels"], ensure_ascii=False) if eb["labels"] else None,
            head,
        ))
        valid.add(username)

    out.executemany("""
        INSERT OR REPLACE INTO contacts
        (account, username, type, is_friend, alias, nick_name, remark, description,
         signature, gender, country, province, city, labels, head_img_url)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, kept)
    out.commit()
    print(f"  [{account}] contacts: 写入 {len(kept):,} 行")
    return id2user, room_id2user, valid


def build_chatrooms(out: sqlite3.Connection, account: str, contact_db: Path) -> None:
    src = connect_ro(contact_db)
    detail = {}
    for room_id_, username_, ann, ann_time in src.execute(
            "SELECT room_id_, username_, announcement_, announcement_publish_time_ "
            "FROM chat_room_info_detail"):
        if username_:
            detail[username_] = (ann or None, ann_time or None)

    rows = []
    for username, owner in src.execute("SELECT username, owner FROM chat_room WHERE username IS NOT NULL"):
        ann, ann_time = detail.get(username, (None, None))
        rows.append((account, username, owner or None, ann, ann_time))
    src.close()

    out.executemany("""
        INSERT OR REPLACE INTO chatrooms
        (account, username, group_owner, announcement, announcement_time)
        VALUES (?,?,?,?,?)
    """, rows)
    out.commit()
    print(f"  [{account}] chatrooms: 写入 {len(rows):,} 行")


def build_chatroom_members(out: sqlite3.Connection, account: str, contact_db: Path,
                           id2user: dict, room_id2user: dict) -> None:
    src = connect_ro(contact_db)
    rows = []
    skipped = 0
    for room_id, member_id in src.execute("SELECT room_id, member_id FROM chatroom_member"):
        ru = room_id2user.get(room_id)
        mu = id2user.get(member_id)
        if ru and mu:
            rows.append((account, ru, mu))
        else:
            skipped += 1
    src.close()

    out.executemany("""
        INSERT OR IGNORE INTO chatroom_members (account, room_username, member_username)
        VALUES (?,?,?)
    """, rows)
    out.commit()
    print(f"  [{account}] chatroom_members: 写入 {len(rows):,} 行（跳过 {skipped}）")


def build_username_table_map(valid: set[str]) -> dict[str, str]:
    """md5(username) → username，用于把 Msg_<md5> 表名还原成会话归属。"""
    return {md5_hex(u): u for u in valid}


def build_chat_name_map(message_dir: Path) -> dict[str, str]:
    """从分片 Name2Id 收集会话 username（含已退群）。"""
    out = {}
    for db in message_shards(message_dir):
        conn = connect_ro(db)
        for (u,) in conn.execute(
                "SELECT user_name FROM Name2Id WHERE user_name IS NOT NULL AND user_name != ''"):
            out[md5_hex(u)] = u
        conn.close()
    return out


def detect_self_wxid(shards: list[str], md5map: dict[str, str]) -> str | None:
    """私聊表里非空且 != 对端 username 的发送者，多数即本人。"""
    counter: Counter = Counter()
    sampled = 0
    for db in shards:
        conn = connect_ro(db)
        name2id = {rid: u for rid, u in conn.execute("SELECT rowid, user_name FROM Name2Id")}
        msg_tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg\\_%' ESCAPE '\\'")]
        for tbl in msg_tables:
            peer = md5map.get(tbl[4:])
            if not peer or is_group_chat(peer):
                continue
            try:
                senders = conn.execute(f"SELECT DISTINCT real_sender_id FROM [{tbl}]").fetchall()
            except sqlite3.OperationalError:
                continue
            for (sid,) in senders:
                u = name2id.get(sid, "")
                if u and u != peer:
                    counter[u] += 1
            sampled += 1
            if sampled >= 200:
                conn.close()
                return counter.most_common(1)[0][0] if counter else None
        conn.close()
    return counter.most_common(1)[0][0] if counter else None


def _insert_stub_contacts(out: sqlite3.Connection, account: str,
                          extra_chats: set[str]) -> None:
    if not extra_chats:
        return
    stubs = [(account, u, derive_type(u), 0, None, None, None, None, None, 0,
              None, None, None, None, None) for u in extra_chats]
    out.executemany("""
        INSERT OR IGNORE INTO contacts
        (account, username, type, is_friend, alias, nick_name, remark, description,
         signature, gender, country, province, city, labels, head_img_url)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, stubs)
    out.commit()
    print(f"  [{account}] stub 联系人: {len(extra_chats)} 个")


def build_messages(out: sqlite3.Connection, account: str, owner_wxid: str,
                   message_dir: Path, md5map: dict[str, str],
                   valid: set[str], limit_chats: int | None = None,
                   incremental: bool = False, reset_meta: bool = False) -> None:
    shards = message_shards(message_dir)
    mode = "增量" if incremental else "全量"
    print(f"  [{account}] messages ({mode}): {len(shards)} 个分片，owner_wxid={owner_wxid}")

    self_wxid = owner_wxid
    watermarks = {} if (not incremental or reset_meta) else load_watermarks(out, account)
    if incremental and reset_meta:
        print(f"  [{account}] reset-meta: 忽略水位，重扫全部 Msg_* 表")

    total_attempted = 0
    total_inserted = 0
    total_ignored = 0
    tables_with_updates = 0
    unresolved_tables = 0
    chats_done = 0
    extra_chats: set[str] = set()
    insert_sql = """
        INSERT OR IGNORE INTO messages
        (account, chat_username, local_id, server_id, timestamp, sender_username,
         is_self, type, content, media_ref, reply_to, reply_quote)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """

    for db in shards:
        conn = connect_ro(db)
        name2id = {rid: u for rid, u in conn.execute("SELECT rowid, user_name FROM Name2Id")}
        msg_tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg\\_%' ESCAPE '\\'")]

        for tbl in msg_tables:
            chat_username = md5map.get(tbl[4:])
            if not chat_username:
                unresolved_tables += 1
                continue
            if chat_username not in valid:
                extra_chats.add(chat_username)
            is_group = is_group_chat(chat_username)

            wm = 0 if (not incremental or reset_meta) else watermarks.get(tbl, 0)
            try:
                if incremental and not reset_meta and wm > 0:
                    rows = conn.execute(f"""
                        SELECT local_id, server_id, local_type, create_time,
                               real_sender_id, message_content, WCDB_CT_message_content
                        FROM [{tbl}]
                        WHERE local_id > ?
                    """, (wm,)).fetchall()
                else:
                    rows = conn.execute(f"""
                        SELECT local_id, server_id, local_type, create_time,
                               real_sender_id, message_content, WCDB_CT_message_content
                        FROM [{tbl}]
                    """).fetchall()
            except sqlite3.OperationalError:
                continue

            table_max_local_id = wm
            table_max_ts: int | None = None
            batch = []
            for (local_id, server_id, local_type, create_time,
                 sender_id, mc, ct) in rows:
                table_max_local_id = max(table_max_local_id, local_id)
                if create_time is not None:
                    table_max_ts = max(table_max_ts or 0, int(create_time))

                raw_text = decode_content(mc, ct or 0)
                base_type, _ = split_msg_type(local_type)

                text_for_content = raw_text
                if is_group and base_type == 1 and ":\n" in raw_text:
                    text_for_content = raw_text.split(":\n", 1)[1]

                reply_quote = None
                if base_type == 1:
                    msg_type, content, media_ref, reply_to = "text", text_for_content, None, None
                elif base_type in (10000, 10002):
                    msg_type, content, media_ref, reply_to = "system", text_for_content, None, None
                else:
                    msg_type, content, media_ref, reply_to = interpret_message(
                        base_type, text_for_content)
                    if msg_type == "quote":
                        rb = extract_refermsg_block(text_for_content)
                        if rb:
                            snap = refermsg_snapshot_from_block(rb)
                            if snap:
                                reply_quote = json.dumps(snap, ensure_ascii=False)

                sender_wxid = name2id.get(sender_id, "") or None
                is_self = 1 if (sender_wxid and sender_wxid == self_wxid) else 0
                sender_out = "me" if is_self else sender_wxid

                batch.append((
                    account, chat_username, local_id, server_id, create_time, sender_out,
                    is_self, msg_type, content or None, media_ref, reply_to, reply_quote,
                ))

            if batch:
                before = out.total_changes
                out.executemany(insert_sql, batch)
                inserted = out.total_changes - before
                total_inserted += inserted
                total_attempted += len(batch)
                total_ignored += len(batch) - inserted
                if inserted > 0:
                    tables_with_updates += 1
            elif incremental and wm > 0:
                pass  # 无新消息，水位不变

            if table_max_local_id > wm or not incremental or tbl not in watermarks:
                upsert_watermark(out, account, tbl, table_max_local_id, table_max_ts)

            chats_done += 1
            if chats_done % 50 == 0:
                out.commit()
                if incremental:
                    print(f"    ...已扫描 {chats_done} 个会话表，"
                          f"新增 {total_inserted:,} 条，去重跳过 {total_ignored:,} 条")
                else:
                    print(f"    ...已处理 {chats_done} 个会话，写入 {total_inserted:,} 条")
            if limit_chats and chats_done >= limit_chats:
                conn.close()
                out.commit()
                _insert_stub_contacts(out, account, extra_chats)
                print(f"  [{account}] [调试] limit-chats={limit_chats}，"
                      f"{'新增' if incremental else '写入'} {total_inserted:,} 条")
                return
        conn.close()

    _insert_stub_contacts(out, account, extra_chats)
    out.commit()
    if incremental:
        print(f"  [{account}] messages: 新增 {total_inserted:,} 条，"
              f"去重跳过 {total_ignored:,} 条"
              f"（{tables_with_updates} 个会话表有更新，扫描 {chats_done} 表，"
              f"未解析 {unresolved_tables}）")
    else:
        ignored_note = f"，去重跳过 {total_ignored:,} 条" if total_ignored else ""
        print(f"  [{account}] messages: 写入 {total_inserted:,} 条{ignored_note}"
              f"（{chats_done} 会话，未解析表 {unresolved_tables}）")


def backfill_reply_quote(out: sqlite3.Connection, incremental: bool = False) -> None:
    """引用消息插入时已带 refermsg 快照（reply_quote）。
    若被引用原文确实存在于本库（reply_to 能 JOIN 到 server_id），则把快照置空，省冗余；
    JOIN 不到（原文已删/不在库）则保留快照作为兜底。"""
    scope = " AND reply_quote IS NOT NULL" if incremental else ""
    total = out.execute(
        "SELECT COUNT(*) FROM messages WHERE type='quote' AND reply_to IS NOT NULL" + scope
    ).fetchone()[0]

    cur = out.execute(f"""
        UPDATE messages SET reply_quote = NULL
        WHERE type = 'quote'
          AND reply_to IS NOT NULL
          {scope}
          AND EXISTS (
              SELECT 1 FROM messages o
              WHERE o.account = messages.account
                AND o.server_id = messages.reply_to
                AND o.server_id != 0
          )
    """)
    cleared = cur.rowcount
    out.commit()

    kept = out.execute(
        "SELECT COUNT(*) FROM messages WHERE type='quote' AND reply_quote IS NOT NULL"
    ).fetchone()[0]
    label = "增量" if incremental else "全量"
    print(f"  reply_quote ({label}): 待处理 {total:,} 条引用；"
          f"原文在库 {cleared:,} 条已置空，"
          f"原文缺失 {kept:,} 条保留快照")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="输出 governed.db 路径")
    ap.add_argument("--staging-tmp", action="store_true",
                    help="先在 Linux /tmp 构建，成功后再 mv 到 --out（规避 /mnt 慢盘）")
    ap.add_argument("--incremental", action="store_true",
                    help="增量模式：保留已有 governed.db，仅追加新消息并刷新维表")
    ap.add_argument("--reset-meta", action="store_true",
                    help="配合 --incremental：忽略 build_meta 水位，重扫全部 Msg_* 表（靠去重键对齐）")
    ap.add_argument("--limit-chats", type=int, default=None, help="调试：每个 account 仅处理前 N 个会话")
    ap.add_argument("--account", default=None, help="仅构建指定 account（如 personal / work）")
    args = ap.parse_args()

    if args.reset_meta and not args.incremental:
        print("--reset-meta 须与 --incremental 一起使用", file=sys.stderr)
        return 1

    accounts = ACCOUNTS
    if args.account:
        accounts = [a for a in ACCOUNTS if a["account"] == args.account]
        if not accounts:
            print(f"未知 account: {args.account}", file=sys.stderr)
            return 1

    for acct in accounts:
        contact_db = Path(acct["decrypted_dir"]) / "contact" / "contact.db"
        if not contact_db.exists():
            print(f"找不到 contact.db: {contact_db}", file=sys.stderr)
            return 1

    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.incremental and not out_path.exists() and not args.staging_tmp:
        print(f"governed.db 不存在: {out_path}\n请先全量构建（不带 --incremental）",
              file=sys.stderr)
        return 1

    if args.staging_tmp:
        work_path = Path(f"/tmp/governed_build_{os.getpid()}_{int(time.time())}.db")
        if work_path.exists():
            work_path.unlink()
        if args.incremental:
            if not out_path.exists():
                print(f"增量 + staging-tmp 需要已有 governed.db: {out_path}",
                      file=sys.stderr)
                return 1
            shutil.copy2(out_path, work_path)
            print(f"staging-tmp + incremental: 复制 {out_path} -> {work_path}")
        else:
            print(f"staging-tmp: 构建于 {work_path}，完成后移至 {out_path}")
    else:
        work_path = out_path
        if args.incremental:
            print(f"incremental: 更新 {out_path}")
        elif out_path.exists():
            out_path.unlink()

    t0 = time.time()
    out: sqlite3.Connection | None = None
    try:
        out = sqlite3.connect(work_path)
        configure_output_db(out)
        if args.incremental:
            ensure_incremental_schema(out)
        else:
            init_full_schema(out)
        out.commit()
        mode = "incremental" if args.incremental else "full"
        print(f"目标库: {work_path}  |  mode={mode}  |  accounts: {[a['account'] for a in accounts]}")

        for acct in accounts:
            account = acct["account"]
            owner_wxid = acct["owner_wxid"]
            decrypted = Path(acct["decrypted_dir"])
            contact_db = decrypted / "contact" / "contact.db"
            message_dir = decrypted / "message"

            print(f"\n=== account={account} ===")

            print("[1/5] contacts ...")
            id2user, room_id2user, valid = build_contacts(out, account, contact_db)

            print("[2/5] chatrooms ...")
            build_chatrooms(out, account, contact_db)

            print("[3/5] chatroom_members ...")
            build_chatroom_members(out, account, contact_db, id2user, room_id2user)

            print("[4/5] messages ...")
            md5map = build_chat_name_map(message_dir)
            md5map.update(build_username_table_map(valid))
            if args.incremental:
                bootstrap_watermarks_if_empty(out, account, message_dir, md5map)
            build_messages(out, account, owner_wxid, message_dir, md5map, valid,
                           limit_chats=args.limit_chats,
                           incremental=args.incremental,
                           reset_meta=args.reset_meta)

        print("\n[5/5] 回填 reply_quote ...")
        backfill_reply_quote(out, incremental=args.incremental)

        print("[校验] contacts.type 与 username 后缀一致性 ...")
        validate_contact_types(out)

        print("\n=== 完成 ===")
        for tbl in ("contacts", "chatrooms", "chatroom_members", "messages"):
            n = out.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  {tbl}: {n:,}")
        meta_n = out.execute("SELECT COUNT(*) FROM build_meta").fetchone()[0]
        print(f"  build_meta: {meta_n:,} 条水位")
        print("\n按 account 统计 messages:")
        for row in out.execute(
                "SELECT account, COUNT(*) FROM messages GROUP BY account ORDER BY account"):
            print(f"  {row[0]}: {row[1]:,}")
        out.close()
        out = None

        if args.staging_tmp:
            if out_path.exists():
                out_path.unlink()
            shutil.move(str(work_path), str(out_path))
            print(f"已移至 {out_path}")

        print(f"耗时 {time.time() - t0:.1f}s -> {out_path}")
        return 0
    except Exception:
        if out is not None:
            out.close()
        if args.staging_tmp and work_path.exists():
            work_path.unlink(missing_ok=True)
        raise


def validate_contact_types(out: sqlite3.Connection) -> None:
    bad = []
    for account, username, typ in out.execute(
            "SELECT account, username, type FROM contacts"):
        if derive_type(username) != typ:
            bad.append((account, username, typ, derive_type(username)))
    if bad:
        print(f"  ⚠️ 发现 {len(bad)} 行 type 与后缀不一致（示例前 5 条）：")
        for ac, u, got, exp in bad[:5]:
            print(f"     [{ac}] {u}  type={got}  应为={exp}")
    else:
        print("  ✅ 全部一致")


if __name__ == "__main__":
    raise SystemExit(main())

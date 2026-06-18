# 微信聊天记录数据治理模块（wechat_database_governance）

> **给接手 agent 的话**：本文件是本模块的**唯一入口文档**。读完它你就能理解模块的功能、
> 产出、目录结构与规范。更深的细节在 `docs/` 下；**下游消费 governed.db 的 agent 必读
> `docs/下游迁移指南.md`**（v2 双账号 schema 与查询规范）。

---

## 1. 这个模块是干什么的

把**解密后的微信数据库**治理成**一个干净、自洽、面向模型训练的 SQLite 库** `output/governed.db`，供下游「**数字分身**」等项目使用。

- **输入（v2）**：两个解密库（只读）：
  - `wechat-decrypt-personal/decrypted/` — 个人号 → **`account='personal'`**
  - `wechat-decrypt-work/decrypted/` — 工作号 → **`account='work'`**
- **输出**：`output/governed.db` — **4 张业务表** + 构建内部 `build_meta`；双号合并后 messages **约 238 万行**（跨分片去重后；源库扫描约 310 万行，其余为 `INSERT OR IGNORE` 跳过）。
- **边界**：本模块只做「解密库 → 治理库」。不解密、不训练。

> ⚠️ **schema**：v2（双账号 + `account` 列）已落地。下游按 `docs/下游迁移指南.md` 改。

---

## 2. 目录结构

```
wechat_database_governance/
├── README.md                       # ← 入口（本文件）
├── build_governed_db.py            # 构建脚本（v2 双账号 + 增量）
├── accounts.local.example.py       # 账号配置模板（复制为 accounts.local.py）
├── accounts.local.py               # 本地真实配置（gitignore，勿提交）
├── requirements.txt
├── output/
│   └── governed.db                 # 产出（gitignore）
├── notebooks/
│   └── explore_wechat_db.ipynb
└── docs/
    ├── 数据库治理方案.md            # 设计依据、决策
    ├── 数据字典.md                  # v2 字段手册
    ├── 下游迁移指南.md              # ★ 给 digital_twin 等下游 agent
    ├── 数据库开发小技巧.md
    └── 微信聊天记录解析项目分析报告.md
```

| 你的角色 | 读什么 |
|----------|--------|
| 改治理逻辑 | `docs/数据库治理方案.md` |
| 消费 governed.db（digital_twin 等） | **`docs/下游迁移指南.md`** + `docs/数据字典.md` |
| 探查原始数据 | `notebooks/explore_wechat_db.ipynb` |
| SQLite 工程规范 | `docs/数据库开发小技巧.md` |

---

## 3. 快速开始

```bash
pip install -r requirements.txt

# 全量构建（首次或 schema 变更后）
python3 build_governed_db.py --staging-tmp          # 推荐：先写 /tmp 再 mv，规避 /mnt 慢盘
python3 build_governed_db.py                        # 直接写 output/governed.db

# 增量更新（解密库有新消息后）
python3 build_governed_db.py --incremental --staging-tmp
python3 build_governed_db.py --incremental --account work   # 只更新 work 号

# 调试
python3 build_governed_db.py --limit-chats 30 --out /tmp/test.db
python3 build_governed_db.py --incremental --reset-meta     # 重扫源表、靠去重键对齐（不删库）
```

| 模式 | 说明 |
|------|------|
| 默认（全量） | 删除旧库，DROP 重建 4 业务表 + `build_meta` |
| `--incremental` | 保留已有库，维表全量刷新；messages 按 `build_meta` 水位只读 `local_id` 更大的行 |
| `--incremental --reset-meta` | 忽略水位，重扫全部 `Msg_*` 表，`INSERT OR IGNORE` 去重 |
| `--staging-tmp` | 在 Linux `/tmp` 构建，完成后 mv 到 `--out`；可与增量联用（先复制现有库到 tmp） |

构建配置：复制 `accounts.local.example.py` → `accounts.local.py` 并填入本地路径与 owner wxid（**勿提交**）。示例：

```python
ACCOUNTS = [
    {"account": "personal",  "owner_wxid": "wxid_personal_owner",
     "decrypted_dir": "/path/to/wechat-decrypt-personal/decrypted"},
    {"account": "work", "owner_wxid": "wxid_work_owner",
     "decrypted_dir": "/path/to/wechat-decrypt-work/decrypted"},
]
```

---

## 4. v2 表结构速览

> 逐字段见 **`docs/数据字典.md`**。下游查询**必须带 `account`**。

| 表 | 主键 | 要点 |
|----|------|------|
| `contacts` | `(account, username)` | `account`=`personal`/`work`；`type` 含 `@im.chatroom`→chatroom |
| `chatrooms` | `(account, username)` | 群主列名 **`group_owner`**（不是 `owner_username`） |
| `chatroom_members` | `(account, room, member)` | |
| `messages` | `id`；唯一 `(account, server_id)`；唯一 `(account, chat_username, local_id)` | `sender_username='me'` 须配合 `account` |
| `build_meta` | `(account, msg_table)` | 增量水位（构建内部用，下游可忽略） |

**示例**

```sql
SELECT * FROM messages
WHERE account = 'personal' AND chat_username = 'wxid_xxx'
ORDER BY timestamp;
```

---

## 5. 核心规范（v2）

1. **`account` 是第一过滤维度** — 所有查询、JOIN、引用自关联、媒体回查都要带。
2. **`server_id` 仅在同一 `account` 内唯一** — 跨号可能碰撞；去重键是 `(account, server_id)`。
3. **`local_id` 仅在 `(account, chat_username)` 内有效** — 增量去重键；跨分片排序仍用 `timestamp`。
4. **不存原始媒体** — `media_ref` + 按 `account` 回查对应 `decrypted/message/`。
5. **公众号推送**（`biz_message_*.db`）不纳入。
6. **隐私** — 真实 wxid/路径放 `accounts.local.py`（gitignore）；公开库仅用占位符；勿提交 `.db`。

---

## 6. 二次开发

- **改治理规则**：`build_governed_db.py`；改 schema 后须**全量**重跑；仅规则微调可试 `--incremental --reset-meta`。
- **构建性能**：text/system 快路径、XML regex 预编译、输出库 WAL；日志含「去重跳过 N 条」；详见 `docs/数据库开发小技巧.md` §11–§12。
- **下游改 digital_twin**：按 **`docs/下游迁移指南.md` §5** 清单加 `account` 过滤；个人分身默认 `account='personal'`。
- **验证**：`notebooks/explore_wechat_db.ipynb` §7（含 `account` 维度与 `build_meta` 行数）。

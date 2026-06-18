"""本地账号配置示例。复制为 `accounts.local.py` 并填入真实值（该文件已在 .gitignore 中）。

build_governed_db.py 会优先加载 accounts.local.py；不存在时使用脚本内占位默认值。
"""

ACCOUNTS = [
    {
        "account": "personal",
        "owner_wxid": "wxid_personal_owner",  # 个人号 wxid / 微信号对应 id
        "decrypted_dir": "/path/to/wechat-decrypt-personal/decrypted",
    },
    {
        "account": "work",
        "owner_wxid": "wxid_work_owner",
        "decrypted_dir": "/path/to/wechat-decrypt-work/decrypted",
    },
]

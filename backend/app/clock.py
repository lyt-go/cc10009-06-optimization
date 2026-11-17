"""统一时钟：稳定窗口、再平衡间隔、迁移审计都以这里的时间为准。

测试里通过 `set_now()` 把时间钉在稳定窗口边界上，业务代码不要直接调用
`datetime.now()`，否则「窗口刚过」和「窗口内」两种分支没法稳定复现。
"""
from __future__ import annotations

from datetime import datetime, timezone

_override: datetime | None = None


def now() -> datetime:
    if _override is not None:
        return _override
    return datetime.now(timezone.utc)


def set_now(value: datetime | None) -> None:
    """只给测试与演练使用；传 None 恢复真实时钟。"""
    global _override
    if value is not None and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    _override = value


def reset() -> None:
    set_now(None)

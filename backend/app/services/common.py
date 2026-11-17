"""各业务服务共用的小工具：分页、编号、数值与时间解析。

请求分配、采集节点和增量再平衡三个模块的分页与字段口径都从这里取，
避免每个模块各写一份略有差异的实现。
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any, Iterable


def as_int(value: Any, default: int = 0) -> int:
    """尽量把界面上的字符串指标转成整数，转不动时落到默认值。"""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip().replace("%", "")
    if not text:
        return default
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return default


def as_float(value: Any, default: float = 0.0) -> float:
    """把 CPU、阈值这类指标转成浮点数，保留一位小数。"""
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return round(float(value), 1)
    text = str(value or "").strip().replace("%", "")
    if not text:
        return default
    try:
        return round(float(text), 1)
    except (TypeError, ValueError):
        return default


def parse_time(value: Any) -> datetime | None:
    """解析 ISO 时间；界面和种子数据里既可能出现带时区，也可能出现裸时间。"""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_time(value: datetime | None = None) -> str:
    return (value or utc_now()).astimezone(timezone.utc).isoformat(timespec="seconds")


def paginate(rows: Iterable[dict[str, Any]], page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
    """统一的列表分页口径：先算总数，再按页切片。"""
    materialized = list(rows)
    total = len(materialized)
    start = max(page - 1, 0) * size
    return materialized[start:start + size], total


def next_id(rows: Iterable[dict[str, Any]]) -> int:
    return max((as_int(row.get("id", 0)) for row in rows), default=0) + 1


def copy_row(row: dict[str, Any]) -> dict[str, Any]:
    """复制一条记录，避免落点清单与结果快照互相带引用。"""
    return copy.deepcopy(row)


def copy_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [copy_row(row) for row in rows]


def contains_any(row: dict[str, Any], fields: Iterable[str], keyword: str | None) -> bool:
    if not keyword:
        return True
    needle = keyword.strip()
    return any(needle in str(row.get(field, "")) for field in fields)


def text(value: Any) -> str:
    return str(value or "").strip()


def match_keyword(row: dict[str, Any], fields: Iterable[str], keyword: str | None) -> bool:
    """按给定字段做包含匹配；空关键词视为全部命中。"""
    return contains_any(row, fields, keyword)

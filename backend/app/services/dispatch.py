"""请求分配业务规则：状态流转、字段校验与筛选口径都收在这里。

分配策略只管「怎么分」：目标域名、算法、亲和性、阈值、稳定窗口、优先级。
策略编号同时决定分配结果数据集的名字（DIST-RES-<策略编号>），便于回查。
"""
from __future__ import annotations

from typing import Any

from app.services.common import as_int, match_keyword, next_id, paginate, text
from app.store import store

MODULE = "dispatch"
REQUIRED_FIELDS = ["策略编号", "目标域名", "分配算法"]
STATUS_ORDER = ["草稿", "策略验证中", "已启用", "已停用"]
ACTION_RULES = {"提交策略验证": "策略验证中", "启用策略": "已启用", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]
SEARCH_FIELDS = ["策略编号", "目标域名"]


class DispatchService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        rows = [row for row in rows if match_keyword(row, SEARCH_FIELDS, keyword)]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        rows = sorted(rows, key=lambda row: as_int(row.get("优先级"), 99))
        return paginate(rows, page, size)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not text(values.get(field))]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {
            "id": next_id(rows),
            "status": STATUS_ORDER[0],
            "pending": True,
            "abnormal": False,
            "策略编号": text(values.get("策略编号")),
            "目标域名": text(values.get("目标域名")),
            "分配算法": text(values.get("分配算法")),
            "权重因子": text(values.get("权重因子")) or "CPU使用率",
            "节点亲和性": text(values.get("节点亲和性")),
            "负载阈值": as_int(values.get("负载阈值"), 80),
            "再平衡间隔": as_int(values.get("再平衡间隔"), 300),
            "稳定窗口": as_int(values.get("稳定窗口"), 600),
            "溢出处理": text(values.get("溢出处理")) or "允许溢出",
            "优先级": as_int(values.get("优先级"), len(rows) + 1),
            "请求数量": max(as_int(values.get("请求数量"), 1), 0),
            "当前结果编号": "",
            "当前结果版本": 0,
            "待复核结果": 0,
            "最近再平衡时间": "",
        }
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"分配策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于请求分配可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"分配策略已{action}"

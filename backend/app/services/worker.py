"""采集节点业务规则：状态流转、字段校验与筛选口径都收在这里。

节点清单是请求分配结果的「另一业务面」：已占用槽位、剩余槽位、当前结果编号
和版本都由调度落点侧的活动落点反推，登记与状态流转只改节点自身属性。
"""
from __future__ import annotations

from typing import Any

from app.services.common import as_int, next_id, paginate, text
from app.store import store

MODULE = "worker"
REQUIRED_FIELDS = ["节点编号", "主机地址", "并发槽位数"]
STATUS_ORDER = ["草稿", "注册中", "在线", "离线"]
ACTION_RULES = {"提交注册": "注册中", "标记在线": "在线", "标记离线": "离线"}
NEGATIVE_ACTIONS = ["标记离线"]


class WorkerService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        load_state: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in text(row.get("节点编号"))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if load_state:
            rows = [row for row in rows if row.get("负载状态") == load_state]
        rows = sorted(rows, key=lambda row: as_int(row.get("id")))
        return paginate(rows, page, size)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not text(values.get(field))]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        slots = max(as_int(values.get("并发槽位数")), 0)
        entry = {
            "id": next_id(rows),
            "status": STATUS_ORDER[0],
            "pending": True,
            "abnormal": False,
            "节点编号": text(values.get("节点编号")),
            "主机地址": text(values.get("主机地址")),
            "并发槽位数": slots,
            "地域标签": text(values.get("地域标签")),
            "CPU使用率": "0",
            "内存可用MB": text(values.get("内存可用MB")) or "0",
            "上次心跳时间": "",
            "节点状态标签": "待接入",
            "负载阈值": as_int(values.get("负载阈值"), 80),
            "稳定窗口": as_int(values.get("稳定窗口"), 600),
            "负载状态": "正常",
            "异常开始时间": "",
            "最近恢复时间": "",
            "已占用槽位": 0,
            "剩余槽位": slots,
            "当前结果编号": "",
            "当前结果版本": 0,
            "落点请求数": 0,
            "最近迁移原因": "",
        }
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"工作节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集节点可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        if action == "标记在线":
            # 重新上线的节点从头计算稳定窗口，避免沿用下线前的异常开始时间
            entry["负载状态"] = "正常"
            entry["异常开始时间"] = ""
            entry["最近恢复时间"] = ""
        return entry, f"工作节点已{action}"

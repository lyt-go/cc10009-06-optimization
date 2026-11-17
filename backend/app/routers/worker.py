"""采集节点接口：维护工作节点，覆盖提交注册、标记在线、标记离线等动作。

节点清单同时是「另一业务面」：已占用/剩余槽位、当前结果版本由调度落点侧
清单反推，只读不手改。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import rebalance
from app.services.worker import WorkerService

router = APIRouter(prefix="/api/worker", tags=["采集节点"])

service = WorkerService()

LIST_FIELDS = [
    "节点编号", "主机地址", "并发槽位数", "地域标签", "CPU使用率", "内存可用MB",
    "上次心跳时间", "节点状态标签", "负载状态", "已占用槽位", "剩余槽位",
    "当前结果编号", "当前结果版本", "最近迁移原因",
]
STATUSES = ["草稿", "注册中", "在线", "离线"]
LOAD_STATES = ["正常", "观察中", "异常"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按节点编号检索"),
    status: str | None = Query(default=None, description="草稿、注册中、在线、离线"),
    load_state: str | None = Query(default=None, description="正常、观察中、异常"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按节点编号、状态与负载状态过滤采集节点；剩余容量取自调度落点侧清单。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(
        keyword=keyword, status=status, load_state=load_state, page=page, size=size
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/capacity", response_model=dict)
def get_capacity() -> dict:
    """节点容量快照与剩余容量：和请求分配结果共用同一个计算入口。"""
    return {
        "taken_at": rebalance.capacity_snapshot()["taken_at"],
        "fingerprint": rebalance.capacity_snapshot()["fingerprint"],
        "items": rebalance.worker_capacity_view(),
    }


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集节点清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "worker", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条工作节点明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"工作节点 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条工作节点，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="工作节点已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条工作节点执行提交注册、标记在线、标记离线；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

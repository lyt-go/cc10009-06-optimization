"""请求分配结果接口：读取影响面与结果数据集、执行增量再平衡、复核与回滚。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import dispatch_result, rebalance
from app.services.common import as_int, parse_time, text

router = APIRouter(prefix="/api/dispatch-result", tags=["请求分配结果"])

STATUSES = ["已生成", "待复核", "已生效", "迁移失败", "已回滚", "已被替代"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按结果编号或受影响节点检索"),
    status: str | None = Query(default=None, description="、".join(STATUSES)),
    domain: str | None = Query(default=None, description="按目标域名过滤"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """请求分配结果列表：按版本倒序，同名结果的多版本都在这里可见。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = dispatch_result.list_results(
        keyword=keyword, status=status, domain=domain, page=page, size=size
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/impact", response_model=dict)
def get_impact(node_ids: str | None = Query(default=None, description="逗号分隔的节点 id")) -> dict:
    """读取受影响节点、受影响域名与调度落点侧现状，不产生任何写入。"""
    ids = [as_int(item) for item in node_ids.split(",") if text(item)] if node_ids else None
    return rebalance.impact(ids)


@router.get("/check", response_model=dict)
def check_rebalance(node_ids: str | None = None) -> dict:
    """检查迁移影响：受影响域名、槽位校验、幂等命中情况。"""
    ids = [as_int(item) for item in node_ids.split(",") if text(item)] if node_ids else None
    return dispatch_result.check_existing(node_ids=ids)


@router.get("/moves", response_model=dict)
def list_moves(
    domain: str | None = Query(default=None, description="按目标域名过滤迁移记录"),
    result_no: str | None = Query(default=None, description="按结果编号过滤"),
) -> dict:
    """迁移审计：谁从哪个节点搬到哪个节点、原因是什么、当时的稳定窗口多长。"""
    return rebalance.moves(domain, result_no)


@router.get("/capacity", response_model=dict)
def get_capacity(node_ids: str | None = None) -> dict:
    """节点容量快照：与分流结果共用同一个计算入口。"""
    ids = [as_int(item) for item in node_ids.split(",") if text(item)] if node_ids else None
    return rebalance.capacity_snapshot(ids)


@router.get("/manifest", response_model=dict)
def get_sink_manifest() -> dict:
    """调度落点侧清单：节点 + 结果编号维度的生效落点与剩余槽位。"""
    return rebalance.sink_manifest()


@router.post("/rebalance", response_model=ActionResult)
def run_rebalance(payload: EntryPayload) -> ActionResult:
    """执行增量再平衡：只重算受影响域名，生成新的请求分配结果数据集。"""
    values = payload.values
    node_ids = values.get("node_ids")
    rule_ids = values.get("rule_ids")
    at = values.get("at")
    operator = text(values.get("operator")) or "值班管理员"
    force = bool(values.get("force"))
    outcome = dispatch_result.rebalance(
        node_ids=[as_int(item) for item in node_ids] if node_ids else None,
        rule_ids=[as_int(item) for item in rule_ids] if rule_ids else None,
        operator=operator,
        at=parse_time(at) if at else None,
        force=force,
    )
    return ActionResult(
        ok=bool(outcome.get("ok")),
        message=text((outcome.get("message"))),
        entry=outcome.get("entry"),  # type: ignore[arg-type]
        extra={
            key: outcome.get(key)
            for key in (
                "skipped", "idempotent", "batch_no", "affected_nodes", "stable_window_hold",
                "untouched_domains", "entries", "skipped_domains", "failures", "per_domain",
            )
        },
    )


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出请求分配结果清单：返回全量版本数据，含调度落点侧清单。"""
    items, total = dispatch_result.list_results(page=1, size=10000)
    return {
        "module": "dispatch_result",
        "total": total,
        "items": items,
        "sink_manifest": rebalance.sink_manifest()["items"],
    }


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条分配结果数据集；不存在时给出可读的错误说明。"""
    entry = dispatch_result.get_result(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分配结果 {entry_id} 不存在或已归档")
    return entry


@router.get("/{entry_id}/review", response_model=dict)
def get_review(entry_id: int) -> dict:
    """复核视图：分配策略、目标域名、节点亲和性、多版本分流结果与追溯信息。"""
    payload = dispatch_result.review_payload(entry_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"分配结果 {entry_id} 不存在或已归档")
    return payload


@router.get("/{entry_id}/versions", response_model=dict)
def get_versions(entry_id: int) -> dict:
    """同名结果的全部版本，便于对照迁移前后的分流差异。"""
    entry = dispatch_result.get_result(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分配结果 {entry_id} 不存在或已归档")
    versions = dispatch_result.result_versions(text(entry.get("result_no")))
    return {
        "result_no": text(entry.get("result_no")),
        "total": len(versions),
        "items": versions,
    }


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """复核通过、驳回、回滚：状态流转只在这里触发。"""
    action = text(payload.values.get("action"))
    reviewer = text(payload.values.get("reviewer")) or "复核人员"
    note = text(payload.values.get("note"))
    if action == "复核通过":
        entry, message = dispatch_result.approve(entry_id, reviewer=reviewer, note=note)
    elif action == "驳回":
        entry, message = dispatch_result.reject(entry_id, reviewer=reviewer, note=note)
    elif action == "回滚":
        entry, message = dispatch_result.rollback(entry_id, operator=reviewer)
    else:
        return ActionResult(
            ok=False,
            message=f"动作「{action}」不属于请求分配结果可执行范围（复核通过 / 驳回 / 回滚）",
        )
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

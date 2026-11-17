"""增量再平衡对外的只读视图：影响面、容量快照、调度落点侧清单与另一业务面清单。

调度落点侧 = `dispatch_sink` 的生效落点；
另一业务面 = 节点清单（`worker`）的剩余容量与当前结果编号。
两边都从同一份落点事实推导，接口只做读投影。
"""
from __future__ import annotations

from typing import Any, Iterable

from app.clock import now as clock_now
from app.services import dispatch_result, nodestate
from app.services.allocation import build_capacity_snapshot
from app.services.common import as_int, iso_time, text
from app.services.sink import sink_service
from app.store import store


def capacity_snapshot(node_ids: Iterable[int] | None = None, *, at=None) -> dict[str, Any]:
    """节点容量快照：和分流结果共用同一个计算入口。"""
    moment = at or clock_now()
    workers = store.rows("worker")
    occupied = sink_service.occupied_by_node()
    thresholds = {
        as_int(node.get("id")): as_int(node.get("负载阈值"), 80) for node in workers
    }
    snapshot = build_capacity_snapshot(
        workers,
        occupied=occupied,
        thresholds=thresholds,
        node_ids=node_ids,
        taken_at=iso_time(moment),
        fingerprint_prefix="capacity",
    )
    return {
        "taken_at": snapshot.taken_at,
        "fingerprint": snapshot.fingerprint,
        "nodes": snapshot.as_list(),
    }


def worker_capacity_view() -> list[dict[str, Any]]:
    """节点清单加工后的容量视图：实际剩余容量与结果版本。"""
    snapshot = capacity_snapshot()
    rows = snapshot["nodes"]
    workers = {as_int(node.get("id")): node for node in store.rows("worker")}
    for row in rows:
        node = workers.get(row["node_id"], {})
        row["status_text"] = text(node.get("status"))
        row["current_result_no"] = text(node.get("当前结果编号"))
        row["current_result_version"] = as_int(node.get("当前结果版本"))
        row["last_move_reason"] = text(node.get("最近迁移原因"))
        row["load_state"] = text(node.get("负载状态")) or nodestate.NORMAL
        row["heartbeat"] = text(node.get("上次心跳时间"))
    return rows


def sink_manifest(node_ids: Iterable[int] | None = None) -> dict[str, Any]:
    rows = sink_service.manifest_rows(node_ids=node_ids)
    return {
        "taken_at": iso_time(clock_now()),
        "total": len(rows),
        "items": rows,
        "integrity": sink_service.integrity_problems(),
    }


def impact(node_ids: Iterable[int] | None = None) -> dict[str, Any]:
    preview = dispatch_result.impact_preview(node_ids)
    return {
        **preview,
        "capacity_snapshot": capacity_snapshot(node_ids),
        "sink_manifest": sink_manifest(),
    }


def moves(domain: str | None = None, result_no: str | None = None) -> dict[str, Any]:
    rows = dispatch_result.moves_for(domain=domain, result_no=result_no)
    return {"total": len(rows), "items": rows}


def bootstrap() -> list[dict[str, Any]]:
    return dispatch_result.bootstrap()

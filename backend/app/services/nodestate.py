"""节点负载状态：何时进入观察、何时真正退出分流、何时恢复。

去抖口径
--------
1. CPU 刚越过阈值时只把节点记为「观察中」，稳定窗口内不迁移任何请求，
   短时抖动不会触发迁移；
2. 连续超阈值满一个稳定窗口仍未恢复 → 记为「异常」，节点退出分流目标，
   其上的请求才算受影响；
3. CPU 回到阈值以内 → 记为「正常」，并写下最近恢复时间；恢复后的节点在稳定
   窗口内仍进观察名单，可以留旧落点，但不接收新请求，避免来回搬；
4. 节点状态不是在线、或健康标签不正常时直接退出分流，不等稳定窗口。

`project_node_load` 是纯计算，用于影响面预览；`apply_node_load` 才写回节点字段。
"""
from __future__ import annotations

from typing import Any, Iterable

from app.services.common import as_float, as_int, iso_time, parse_time, text

NORMAL = "正常"
OBSERVING = "观察中"
ABNORMAL = "异常"
DEFAULT_THRESHOLD = 80.0
DEFAULT_WINDOW = 600


def threshold_of(node: dict[str, Any]) -> float:
    return as_float(node.get("负载阈值"), DEFAULT_THRESHOLD)


def window_of(node: dict[str, Any]) -> int:
    return max(as_int(node.get("稳定窗口"), DEFAULT_WINDOW), 0)


def project_node_load(node: dict[str, Any], *, now, cpu: float | None = None) -> dict[str, Any]:
    """推算节点此刻应有的负载状态，不改任何字段。"""
    cpu_value = as_float(node.get("CPU使用率") if cpu is None else cpu)
    threshold = threshold_of(node)
    window = window_of(node)
    state_before = text(node.get("负载状态")) or NORMAL
    started = parse_time(node.get("异常开始时间"))
    recovered_at = parse_time(node.get("最近恢复时间"))

    state = state_before
    transition = "稳定"
    elapsed = 0.0
    if cpu_value > threshold:
        if started is None:
            state = OBSERVING
            transition = "开始观察"
        else:
            elapsed = (now - started).total_seconds()
            if elapsed < window:
                state = OBSERVING
                transition = "稳定窗口内"
            else:
                state = ABNORMAL
                transition = "退出分流"
    else:
        if state_before != NORMAL:
            state = NORMAL
            transition = "已恢复"
        else:
            state = NORMAL
            transition = "稳定"

    recovering = (
        state == NORMAL
        and recovered_at is not None
        and 0 <= (now - recovered_at).total_seconds() < window
    )
    if recovering:
        transition = "恢复观察"

    notes: list[str] = []
    code = text(node.get("节点编号"))
    if transition == "开始观察":
        notes.append(
            f"{code} CPU {cpu_value:g} 越过阈值 {threshold:g}，进入 {window} 秒稳定窗口观察，暂不迁移"
        )
    elif transition == "稳定窗口内":
        notes.append(
            f"{code} 仍在稳定窗口内（已 {int(elapsed)} 秒 / {window} 秒），短时超阈值不重复迁移"
        )
    elif transition == "退出分流":
        notes.append(
            f"{code} 连续超阈值 {int(elapsed)} 秒，超出稳定窗口 {window} 秒，退出分流目标"
        )
    elif transition == "已恢复":
        notes.append(f"{code} 负载恢复到 {cpu_value:g}，观察 {window} 秒后再接收新请求")

    return {
        "node_id": as_int(node.get("id")),
        "node_code": code,
        "state": state,
        "state_before": state_before,
        "cpu": cpu_value,
        "threshold": threshold,
        "window": window,
        "elapsed": int(elapsed),
        "transition": transition,
        "recovering": recovering,
        "notes": notes,
    }


def apply_node_load(node: dict[str, Any], *, now, cpu: float | None = None) -> dict[str, Any]:
    """把投影结果写回节点；只有真正执行再平衡时才调用。"""
    outcome = project_node_load(node, now=now, cpu=cpu)
    if cpu is not None:
        node["CPU使用率"] = round(float(cpu), 1)
    if outcome["transition"] == "已恢复":
        # 恢复是「回落到阈值以内」的瞬时事实，先落恢复时间再算观察期
        node["最近恢复时间"] = iso_time(now)
        node["异常开始时间"] = ""
        outcome = project_node_load(node, now=now)
    node["负载状态"] = outcome["state"]
    if outcome["transition"] == "开始观察":
        node["异常开始时间"] = iso_time(now)
    elif outcome["transition"] == "退出分流":
        node["异常开始时间"] = node.get("异常开始时间") or iso_time(now)
    if outcome["transition"] in ("退出分流", "已恢复"):
        node["abnormal"] = outcome["transition"] == "退出分流"
    return outcome


def evaluate_nodes(
    workers: Iterable[dict[str, Any]],
    node_ids: Iterable[int] | None,
    *,
    now,
    apply: bool = False,
) -> dict[str, Any]:
    """按需刷新一批节点的负载状态；不传 node_ids 表示全部节点。"""
    selected = None if node_ids is None else {as_int(item) for item in node_ids}
    outcomes: list[dict[str, Any]] = []
    notes: list[str] = []
    for node in workers:
        node_id = as_int(node.get("id"))
        if selected is not None and node_id not in selected:
            continue
        outcome = apply_node_load(node, now=now) if apply else project_node_load(node, now=now)
        outcomes.append(outcome)
        notes.extend(outcome["notes"])
    return {"outcomes": outcomes, "notes": notes}


def recovering_node_ids(workers: Iterable[dict[str, Any]], *, now) -> set[int]:
    """仍在恢复观察期内的节点：保留旧落点，但暂不接收新请求。"""
    result: set[int] = set()
    for node in workers:
        if project_node_load(node, now=now)["recovering"]:
            result.add(as_int(node.get("id")))
    return result

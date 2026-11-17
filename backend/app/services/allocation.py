"""请求分配的唯一计算入口：容量快照、域名分流、槽位校验都从这里走一遍。

为什么收成一个入口
------------------
节点容量快照和分流结果必须基于同一份节点状态计算，否则会出现「快照里还剩 2 个空槽、
分流却排进去 3 条请求」这种对不上的情况。因此这里只放纯函数，不读写仓库、不感知
当前时间：

* `build_capacity_snapshot`：把节点清单 + 已占槽位折算成一次容量快照；
* `plan_domain`：只接受这份快照，按亲和性 → 剩余容量 → 稳定排序给域名排落点；
* `check_slots`：对整份分流方案复核准入、槽位范围、槽位冲突与重复请求；
* `diff_assignments`：对比迁移前后，谁换了节点、谁原地不动。

采集节点页的容量快照和请求分配页的分流结果调用的是同一套写法。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from app.services.common import as_float, as_int, text

ASSIGNABLE_STATUS = "在线"
HEALTHY_LABEL = "健康"
NORMAL_LOAD = "正常"
OBSERVING_LOAD = "观察中"
ABNORMAL_LOAD = "异常"
DEFAULT_THRESHOLD = 80.0


@dataclass(frozen=True)
class NodeCapacity:
    """单个节点的容量快照行，采集节点清单与分流结果共用同一组字段。"""

    node_id: int
    node_code: str
    host: str
    region: str
    status: str
    health: str
    load_state: str
    slots: int
    occupied_slots: tuple[int, ...]
    cpu: float
    threshold: float
    stable_window: int
    eligible: bool
    accepting: bool
    reason: str

    @property
    def free_slots(self) -> int:
        return max(self.slots - len(self.occupied_slots), 0)

    def as_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_code": self.node_code,
            "host": self.host,
            "region": self.region,
            "status": self.status,
            "health": self.health,
            "load_state": self.load_state,
            "slots": self.slots,
            "occupied_slots": list(self.occupied_slots),
            "used_slots": len(self.occupied_slots),
            "free_slots": self.free_slots,
            "cpu": self.cpu,
            "threshold": self.threshold,
            "stable_window": self.stable_window,
            "eligible": self.eligible,
            "accepting": self.accepting,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class CapacitySnapshot:
    """一次分配用到的全部节点容量，带指纹供结果与复核引用。"""

    node_ids: tuple[int, ...]
    nodes: tuple[NodeCapacity, ...]
    taken_at: str
    fingerprint: str

    def by_id(self) -> dict[int, NodeCapacity]:
        return {node.node_id: node for node in self.nodes}

    def occupied(self) -> dict[int, set[int]]:
        return {node.node_id: set(node.occupied_slots) for node in self.nodes}

    def as_list(self) -> list[dict[str, Any]]:
        return [node.as_dict() for node in self.nodes]

    def row_for(self, node_id: int) -> NodeCapacity | None:
        return self.by_id().get(node_id)


@dataclass(frozen=True)
class Assignment:
    """一条请求的落点：一个请求在任一时刻只占一个节点上的一个槽位。"""

    request_key: str
    domain: str
    node_id: int
    node_code: str
    slot: int
    rule_id: int
    priority: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "request_key": self.request_key,
            "domain": self.domain,
            "node_id": self.node_id,
            "node_code": self.node_code,
            "slot": self.slot,
            "rule_id": self.rule_id,
            "priority": self.priority,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Assignment":
        return cls(
            request_key=text(row.get("request_key")),
            domain=text(row.get("domain")),
            node_id=as_int(row.get("node_id")),
            node_code=text(row.get("node_code")),
            slot=as_int(row.get("slot")),
            rule_id=as_int(row.get("rule_id")),
            priority=as_int(row.get("priority")),
        )


def node_eligibility(node: dict[str, Any]) -> tuple[bool, bool, str]:
    """节点能否保留既有落点、能否接收新落点。

    返回 (eligible, accepting, reason)：
    * eligible 为假表示节点已退出分流，原本落在上面的请求必须迁走；
    * accepting 为真才允许新请求落上来。

    「观察中」节点在稳定窗口内不迁走任何请求（eligible 为真），也不接收新请求，
    短时超阈值因此不会引发迁移；只有连续超阈值满一个稳定窗口、状态变成「异常」
    的节点才 eligible 为假，真正退出分流目标。
    """
    if text(node.get("status")) != ASSIGNABLE_STATUS:
        return False, False, f"节点状态为{text(node.get('status')) or '未知'}，退出分流目标"
    health = text(node.get("节点状态标签"))
    if health and health != HEALTHY_LABEL:
        return False, False, f"节点状态标签为{health}，未通过健康检查"
    load_state = text(node.get("负载状态"))
    if load_state == ABNORMAL_LOAD:
        return False, False, "稳定窗口结束后仍未恢复，退出分流目标"
    if load_state and load_state not in (NORMAL_LOAD, OBSERVING_LOAD):
        return False, False, f"节点负载状态为{load_state}，暂不参与分流"
    if as_int(node.get("并发槽位数")) <= 0:
        return False, False, "并发槽位数为 0，无法承接请求"
    cpu = as_float(node.get("CPU使用率"))
    threshold = as_float(node.get("负载阈值"), DEFAULT_THRESHOLD)
    if load_state == OBSERVING_LOAD:
        return True, False, ""
    if threshold and cpu > threshold:
        return True, False, ""
    return True, True, ""


def make_node_capacity(
    node: dict[str, Any],
    *,
    occupied_slots: Iterable[int] = (),
    threshold: float | None = None,
) -> NodeCapacity:
    """把一条节点记录折算成容量行；快照与分流结果复用这一种写法。

    `threshold` 只用于按分配策略覆盖节点自身的阈值，覆盖后重新判定能否接收新请求。
    """
    slots = max(as_int(node.get("并发槽位数")), 0)
    occupied = tuple(
        sorted({as_int(slot) for slot in occupied_slots if 1 <= as_int(slot) <= slots})
    )
    eligible, accepting, reason = node_eligibility(node)
    effective_threshold = (
        threshold if threshold is not None
        else as_float(node.get("负载阈值"), DEFAULT_THRESHOLD)
    )
    if threshold is not None:
        accepting = eligible and not (
            effective_threshold and as_float(node.get("CPU使用率")) > effective_threshold
        )
        if eligible and not accepting:
            reason = f"CPU {as_float(node.get('CPU使用率')):g} 高于阈值 {effective_threshold:g}"
    return NodeCapacity(
        node_id=as_int(node.get("id")),
        node_code=text(node.get("节点编号")),
        host=text(node.get("主机地址")),
        region=text(node.get("地域标签")),
        status=text(node.get("status")),
        health=text(node.get("节点状态标签")),
        load_state=text(node.get("负载状态")),
        slots=slots,
        occupied_slots=occupied,
        cpu=as_float(node.get("CPU使用率")),
        threshold=effective_threshold,
        stable_window=as_int(node.get("稳定窗口"), 600),
        eligible=eligible,
        accepting=accepting,
        reason=reason,
    )


def build_capacity_snapshot(
    workers: Iterable[dict[str, Any]],
    *,
    occupied: dict[int, Iterable[int]] | None = None,
    thresholds: dict[int, float] | None = None,
    node_ids: Iterable[int] | None = None,
    taken_at: str = "",
    fingerprint_prefix: str = "snap",
    extra_salt: str = "",
) -> CapacitySnapshot:
    """从节点清单算出容量快照，可以只取受影响节点，但计算口径与全量一致。"""
    selected = None if node_ids is None else {as_int(item) for item in node_ids}
    occupied = occupied or {}
    thresholds = thresholds or {}
    rows: list[NodeCapacity] = []
    for node in workers:
        node_id = as_int(node.get("id"))
        if selected is not None and node_id not in selected:
            continue
        rows.append(
            make_node_capacity(
                node,
                occupied_slots=occupied.get(node_id, ()),
                threshold=thresholds.get(node_id),
            )
        )
    rows.sort(key=lambda item: item.node_id)
    signature = "|".join(
        f"{row.node_id}:{row.slots}:{','.join(map(str, row.occupied_slots))}:"
        f"{row.status}:{row.health}:{row.load_state}:{row.cpu}:{row.threshold:g}"
        for row in rows
    )
    digest = 0
    for index, char in enumerate(f"{signature}|{extra_salt}"):
        digest = (digest * 131 + ord(char) + index) & 0xFFFFFFFF
    return CapacitySnapshot(
        node_ids=tuple(row.node_id for row in rows),
        nodes=tuple(rows),
        taken_at=taken_at,
        fingerprint=f"{fingerprint_prefix}-{digest:08x}-{len(rows)}",
    )


def _pick_slot(reserved: set[int], slots: int) -> int | None:
    for slot in range(1, slots + 1):
        if slot not in reserved:
            return slot
    return None


def plan_domain(
    *,
    domain: str,
    request_count: int,
    snapshot: CapacitySnapshot,
    affinity: str = "",
    rule_id: int = 0,
    priority: int = 0,
    threshold: float = DEFAULT_THRESHOLD,
    allow_overflow: bool = True,
    keep: Iterable[Assignment] = (),
    frozen_keys: Iterable[str] = (),
    reserved: dict[int, set[int]] | None = None,
    avoid_node_ids: Iterable[int] = (),
) -> tuple[list[Assignment], list[str]]:
    """给一个域名的请求重排落点，沿用仍可用的旧落点。

    `keep` 是本域名当前生效的落点：节点仍可承接、槽位仍空着就原地保留，
    未受影响的请求不会因为重算而换节点。`frozen_keys` 是稳定窗口内刚迁移过的
    请求，只要原节点还能承接就继续保留，避免短时间内反复迁移。`reserved`
    由调用方传入时表示「本次重算之外」的槽位占用（其他域名或未受影响的生效结果），
    多个域名共用这一份，批次内不会互相撞槽；保留的旧落点会自己补回占用。
    `avoid_node_ids` 是刚恢复、仍在观察的节点：只影响新落点，不影响原地保留。
    """
    notes: list[str] = []
    frozen = {text(key) for key in frozen_keys}
    avoid = {as_int(item) for item in avoid_node_ids}
    if reserved is None:
        reserved = snapshot.occupied()
    # 调用方传入的占用字典按引用使用：同一批次里后算的域名能看到前面域名已占的槽位
    for node in snapshot.nodes:
        reserved.setdefault(node.node_id, set())
    by_id = snapshot.by_id()
    keep_map = {item.request_key: item for item in keep}

    # 旧落点先占回自己的槽位；同一请求只保留一次，绝不同时占新旧两个槽位
    kept: dict[str, Assignment] = {}
    for key, item in keep_map.items():
        node = by_id.get(item.node_id)
        if node is None or not node.eligible:
            continue
        if not 1 <= item.slot <= node.slots:
            continue
        if item.slot in reserved[node.node_id]:
            continue
        reserved[node.node_id].add(item.slot)
        kept[key] = item

    candidates = [
        node for node in snapshot.nodes
        if node.eligible and len(reserved[node.node_id]) < node.slots
    ]
    candidates.sort(
        key=lambda node: (
            0 if affinity and node.region == affinity else 1,
            -(node.slots - len(reserved[node.node_id])),
            node.cpu,
            node.node_id,
        )
    )
    saturated = {node.node_id for node in candidates if not node.accepting}
    if saturated:
        notes.append(
            "、".join(sorted(by_id[node_id].node_code for node_id in saturated))
            + " 已触及负载阈值，只保留既有落点，本次不新增"
        )
    fresh_candidates = [
        node for node in candidates
        if node.accepting and node.node_id not in avoid
    ]
    if avoid:
        notes.append(
            "、".join(sorted(by_id[node_id].node_code for node_id in avoid if node_id in by_id))
            + " 刚恢复且仍在观察期，本次不回流"
        )
    preferred = [node for node in fresh_candidates if not affinity or node.region == affinity]
    if not preferred and fresh_candidates:
        preferred = fresh_candidates
        notes.append(f"亲和地域 {affinity} 暂无剩余容量，按剩余容量跨区兜底")

    result: list[Assignment] = []
    for index in range(request_count):
        key = f"{domain}#{index + 1}"
        current = kept.get(key)
        if current is not None:
            result.append(current)
            if key in frozen:
                notes.append(f"{key} 在稳定窗口内刚迁移过，保留在 {current.node_code} 不再搬动")
            continue
        chosen = next(
            (node for node in preferred if len(reserved[node.node_id]) < node.slots),
            None,
        )
        if chosen is None:
            if not allow_overflow:
                notes.append(f"{key} 没有剩余槽位，策略拒绝溢出")
                continue
            # 溢出：亲和地域没有空位时按剩余容量跨到其他地域，避免请求悬空
            chosen = next(
                (node for node in candidates if len(reserved[node.node_id]) < node.slots),
                None,
            )
            if chosen is None:
                notes.append(f"{key} 没有可用槽位，等待节点容量释放后重算")
                continue
            notes.append(f"{key} 在亲和地域没有剩余槽位，按溢出处理落到 {chosen.node_code}")
        slot = _pick_slot(reserved[chosen.node_id], chosen.slots)
        if slot is None:
            notes.append(f"{key} 在 {chosen.node_code} 上找不到空槽位")
            continue
        reserved[chosen.node_id].add(slot)
        result.append(
            Assignment(
                request_key=key,
                domain=domain,
                node_id=chosen.node_id,
                node_code=chosen.node_code,
                slot=slot,
                rule_id=rule_id,
                priority=priority,
            )
        )
    return result, notes


def check_slots(
    assignments: Iterable[Assignment],
    snapshot: CapacitySnapshot,
    occupied: dict[int, Iterable[int]] | None = None,
) -> list[str]:
    """整份分流方案的槽位复核：准入、范围、冲突、重复请求四件事。

    `occupied` 是本次变更之外仍然生效的槽位占用（其他域名或未受影响的结果）；
    传入时按它判断冲突，不传则按整份快照判断。
    """
    by_id = snapshot.by_id()
    if occupied is None:
        occupied = snapshot.occupied()
    occupied = {as_int(node_id): set(slots) for node_id, slots in occupied.items()}
    problems: list[str] = []
    per_node: dict[int, int] = {}
    seen_keys: set[str] = set()
    seen_slots: set[tuple[int, int]] = set()
    for item in assignments:
        if item.request_key in seen_keys:
            problems.append(f"{item.request_key} 在方案里出现了两次，同一请求不能同时占新旧槽位")
            continue
        seen_keys.add(item.request_key)
        node = by_id.get(item.node_id)
        if node is None:
            problems.append(f"{item.request_key} 指向的节点 {item.node_id} 不在容量快照里")
            continue
        if not node.eligible:
            problems.append(f"{item.request_key} 落到不可承接的节点 {node.node_code}：{node.reason}")
        if not 1 <= item.slot <= node.slots:
            problems.append(
                f"{item.request_key} 的槽位 {item.slot} 超出 {node.node_code} 的容量 {node.slots}"
            )
        pair = (item.node_id, item.slot)
        if pair in seen_slots:
            problems.append(f"{node.node_code} 的槽位 {item.slot} 被两条请求同时占用")
        seen_slots.add(pair)
        if item.slot in occupied.get(node.node_id, set()):
            problems.append(
                f"{node.node_code} 的槽位 {item.slot} 已被其他已生效结果占用，不能重复分叉"
            )
        per_node[item.node_id] = per_node.get(item.node_id, 0) + 1
    for node_id, used in per_node.items():
        node = by_id[node_id]
        if used > node.free_slots:
            problems.append(
                f"{node.node_code} 分配了 {used} 条请求，剩余槽位只有 {node.free_slots}"
            )
    return problems


def diff_assignments(
    previous: Iterable[Assignment], current: Iterable[Assignment]
) -> list[dict[str, Any]]:
    """对比迁移前后：谁换了节点、谁原地不动，供复核与责任追溯。"""
    before = {item.request_key: item for item in previous}
    after = {item.request_key: item for item in current}
    changes: list[dict[str, Any]] = []
    for key in sorted(set(before) | set(after)):
        old = before.get(key)
        new = after.get(key)
        if old and new and (old.node_id, old.slot) == (new.node_id, new.slot):
            continue
        changes.append({
            "request_key": key,
            "from_node": old.node_code if old else "",
            "from_node_id": old.node_id if old else 0,
            "from_slot": old.slot if old else 0,
            "to_node": new.node_code if new else "",
            "to_node_id": new.node_id if new else 0,
            "to_slot": new.slot if new else 0,
        })
    return changes

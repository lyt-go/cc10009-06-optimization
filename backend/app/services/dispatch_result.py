"""请求分配结果：增量再平衡、版本、幂等、回滚与复核。

一次再平衡的完整链路
--------------------
1. 读取受影响节点（连续超阈值超出稳定窗口、离线、心跳异常）和当前生效的分配结果；
2. 按同一份容量快照只重算受影响域名，未受影响的域名与请求原地保留；
3. 校验槽位占用：节点不超容量、同一请求不占两个槽位；
4. 生成新的请求分配结果数据集，记录迁移前后的版本、责任节点、容量快照与迁移原因；
5. 归档时带回调度落点侧清单，同步落点并刷新节点清单剩余容量。

结果命名
--------
一个域名一份结果数据集，结果编号固定为 `DIST-RES-<策略编号>`，每次再平衡生成新版本，
因此「同名结果」天然可追溯；版本冲突时两份都保留，复核时人工选择生效版本。

约束
----
* 稳定窗口内短时超阈值不迁移（判定在 nodestate，这里只消费结论）；
* 同一份请求分配结果反复处理只算一次：批量幂等键（域名 + 受影响节点 + 容量快照指纹）
  先拦，内容级幂等（分流逐条比对）再兜底；
* 并发生成时结果不分叉：计算到落地全程持同一把可重入锁，落库前再查一次同名版本；
* 迁移失败回到上一份完整结果，未受影响的域名不发生无依据换节点。
"""
from __future__ import annotations

import threading
from typing import Any, Iterable

from app.clock import now as clock_now
from app.services import nodestate
from app.services.allocation import (
    Assignment,
    CapacitySnapshot,
    build_capacity_snapshot,
    check_slots,
    diff_assignments,
    plan_domain,
)
from app.services.common import as_int, copy_rows, iso_time, next_id, parse_time, text
from app.services.sink import sink_service
from app.store import store

MODULE = "dispatch_result"
RULE_MODULE = "dispatch"
MOVE_MODULE = "dispatch_move"
WORKER_MODULE = "worker"

STATUS_GENERATED = "已生成"
STATUS_PENDING_REVIEW = "待复核"
STATUS_APPLIED = "已生效"
STATUS_FAILED = "迁移失败"
STATUS_ROLLED_BACK = "已回滚"
STATUS_SUPERSEDED = "已被替代"

# 「计算 → 校验 → 落地 → 刷新节点」对并发调用是一个整体：
# 两个并发请求不会各算一份再互相覆盖，后来的一次会命中幂等键直接复用。
_LOCK = threading.RLock()

# 乐观并发的观察点：规划完成后、落库之前调用一次，用于演练「另一个写入方
# 在我算完前提交了同名结果」。生产部署里由多实例写入自然触发，默认不启用。
concurrency_hook: Any = None


def _result_rows() -> list[dict[str, Any]]:
    return store.rows(MODULE)


def _rule_rows() -> list[dict[str, Any]]:
    return store.rows(RULE_MODULE)


def _move_rows() -> list[dict[str, Any]]:
    return store.rows(MOVE_MODULE)


def _workers() -> list[dict[str, Any]]:
    return store.rows(WORKER_MODULE)


def result_no_for(rule: dict[str, Any]) -> str:
    return f"DIST-RES-{text(rule.get('策略编号')) or as_int(rule.get('id'))}"


def _rule_by_domain(domain: str) -> dict[str, Any] | None:
    for row in _rule_rows():
        if text(row.get("目标域名")) == domain:
            return row
    return None


def _rule_by_id(rule_id: int) -> dict[str, Any] | None:
    return store.find(RULE_MODULE, rule_id)


def _request_count(rule: dict[str, Any]) -> int:
    return max(as_int(rule.get("请求数量"), 1), 0)


def _thresholds() -> dict[int, float]:
    return {
        as_int(node.get("id")): as_int(node.get("负载阈值"), 80)
        for node in _workers()
    }


def _versions(result_no: str) -> list[dict[str, Any]]:
    return [row for row in _result_rows() if row.get("result_no") == result_no]


def _latest_version(result_no: str) -> int:
    return max((as_int(row.get("version")) for row in _versions(result_no)), default=0)


def _active_result(result_no: str) -> dict[str, Any] | None:
    actives = [row for row in _versions(result_no) if row.get("status") == STATUS_APPLIED]
    if not actives:
        return None
    return max(actives, key=lambda row: as_int(row.get("version")))


CURRENT_STATUSES = (STATUS_GENERATED, STATUS_PENDING_REVIEW, STATUS_APPLIED)


def _current_results_by_domain() -> dict[str, dict[str, Any]]:
    """每个域名当前落到调度落点侧的结果：取版本最高的一份待复核/已生成/已生效结果。

    已被回滚、迁移失败、已被替代的版本不参与，复核中的新版本参与，
    这样下一次增量再平衡仍能以上一次的落点为基线比对。
    """
    current: dict[str, dict[str, Any]] = {}
    for row in _result_rows():
        if row.get("status") not in CURRENT_STATUSES:
            continue
        for domain in row.get("domains") or [text(row.get("domain"))]:
            existing = current.get(domain)
            if existing is None or as_int(row.get("version")) >= as_int(existing.get("version")):
                current[domain] = row
    return current


def _snapshot_for(
    domains: Iterable[str],
    *,
    taken_at: str,
    extra_salt: str = "",
    node_ids: Iterable[int] | None = None,
) -> CapacitySnapshot:
    """容量快照的占用口径：要重算的域名先不算，它们的旧落点会随迁移释放。"""
    occupied = sink_service.occupied_outside(list(domains))
    return build_capacity_snapshot(
        _workers(),
        occupied=occupied,
        thresholds=_thresholds(),
        node_ids=node_ids,
        taken_at=taken_at,
        extra_salt=extra_salt,
    )


def impact_preview(node_ids: Iterable[int] | None = None, *, at=None) -> dict[str, Any]:
    """只读地预览影响面：哪些节点受影响、哪些域名要重算，不做任何写入。"""
    moment = at or clock_now()
    selected = None if node_ids is None else {as_int(item) for item in node_ids}
    projections: list[dict[str, Any]] = []
    affected_nodes: list[dict[str, Any]] = []
    for node in _workers():
        node_id = as_int(node.get("id"))
        if selected is not None and node_id not in selected:
            continue
        outcome = nodestate.project_node_load(node, now=moment)
        projections.append(outcome)
        if outcome["state"] == nodestate.ABNORMAL or text(node.get("status")) != "在线":
            affected_nodes.append(outcome)
    affected_ids = {item["node_id"] for item in affected_nodes}
    current = _current_results_by_domain()
    affected_domains: list[str] = []
    for domain, result in sorted(current.items()):
        node_set = {as_int(item.get("node_id")) for item in result.get("assignments") or []}
        if node_set & affected_ids:
            affected_domains.append(domain)
    untouched = sorted(set(current) - set(affected_domains))
    return {
        "affected_nodes": affected_nodes,
        "affected_domains": affected_domains,
        "untouched_domains": untouched,
        "node_projections": projections,
        "stable_window_hold": [
            item for item in projections if item["state"] == nodestate.OBSERVING
        ],
    }


def list_results(
    *,
    keyword: str | None = None,
    status: str | None = None,
    domain: str | None = None,
    page: int = 1,
    size: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    rows = list(_result_rows())
    if keyword:
        needle = keyword.strip()
        rows = [
            row for row in rows
            if needle in text(row.get("result_no"))
            or any(needle in text(item) for item in row.get("affected_nodes") or [])
        ]
    if status:
        rows = [row for row in rows if row.get("status") == status]
    if domain:
        rows = [
            row for row in rows
            if domain in (row.get("domains") or []) or domain == text(row.get("domain"))
        ]
    rows.sort(
        key=lambda row: (
            text(row.get("created_at")),
            as_int(row.get("version")),
            as_int(row.get("id")),
        ),
        reverse=True,
    )
    total = len(rows)
    start = max(page - 1, 0) * size
    return rows[start:start + size], total


def get_result(entry_id: int) -> dict[str, Any] | None:
    return store.find(MODULE, entry_id)


def result_versions(result_no: str) -> list[dict[str, Any]]:
    return sorted(_versions(result_no), key=lambda row: as_int(row.get("version")), reverse=True)


def _frozen_keys(domain: str, moment) -> set[str]:
    """稳定窗口内刚迁移过的请求：节点没退出分流就保持原地，不来回搬。"""
    frozen: set[str] = set()
    for move in _move_rows():
        if text(move.get("domain")) != domain:
            continue
        moved_at = parse_time(move.get("moved_at"))
        window = as_int(move.get("stable_window"), 600)
        if moved_at is None:
            continue
        if (moment - moved_at).total_seconds() < window:
            frozen.add(text(move.get("request_key")))
    return frozen


def _plan_batch(
    *,
    domains: list[str],
    current: dict[str, dict[str, Any]],
    snapshot: CapacitySnapshot,
    recovering: set[int],
    moment,
) -> dict[str, dict[str, Any]]:
    """在共享槽位占用上按优先级逐个域名排落点，批次内不会互相撞槽。"""
    reserved = sink_service.occupied_outside(domains)
    ordered = sorted(
        domains,
        key=lambda item: (
            as_int((_rule_by_domain(item) or {}).get("优先级"), 99),
            item,
        ),
    )
    plans: dict[str, dict[str, Any]] = {}
    for domain in ordered:
        rule = _rule_by_domain(domain)
        if rule is None:
            continue
        keep = [
            Assignment.from_row(item)
            for item in (current.get(domain) or {}).get("assignments") or []
        ]
        assignments, notes = plan_domain(
            domain=domain,
            request_count=_request_count(rule),
            snapshot=snapshot,
            affinity=text(rule.get("节点亲和性")),
            rule_id=as_int(rule.get("id")),
            priority=as_int(rule.get("优先级")),
            threshold=as_int(rule.get("负载阈值"), 80),
            allow_overflow=text(rule.get("溢出处理")) != "拒绝溢出",
            keep=keep,
            frozen_keys=_frozen_keys(domain, moment),
            reserved=reserved,
            avoid_node_ids=recovering,
        )
        problems = check_slots(
            assignments, snapshot, occupied=sink_service.occupied_outside(domains)
        )
        expected = _request_count(rule)
        if len(assignments) < expected:
            problems.append(
                f"{domain} 只排到 {len(assignments)}/{expected} 条请求，"
                "剩余容量不足，整份结果不落地"
            )
        plans[domain] = {
            "rule": rule,
            "assignments": assignments,
            "notes": notes,
            "changes": diff_assignments(keep, assignments),
            "problems": problems,
        }
    return plans


def _idempotency_key(
    domain: str, affected_nodes: list[dict[str, Any]], snapshot: CapacitySnapshot
) -> str:
    """同一份分配结果只算一次：域名、受影响节点、容量快照都相同即同一份。"""
    node_part = ",".join(
        str(item["node_id"]) for item in sorted(affected_nodes, key=lambda x: x["node_id"])
    )
    return f"{domain}//{node_part}//{snapshot.fingerprint}"


def _existing_by_key(key: str) -> dict[str, Any] | None:
    """同一份分配结果是否处理过；迁移失败的记录也算处理过，避免反复重算同一批。"""
    for row in _result_rows():
        if row.get("batch_key") == key or row.get("idempotency_key") == key:
            return row
    return None


def _assignment_signature(assignments: Iterable[Assignment]) -> tuple[tuple[str, int, int], ...]:
    return tuple(sorted(
        (item.request_key, item.node_id, item.slot) for item in assignments
    ))


def _content_match(
    result_no: str,
    domain: str,
    assignments: Iterable[Assignment],
) -> dict[str, Any] | None:
    """内容级幂等：同一域名算出的分流与当前版本完全一致，就不再生成新版本。

    只比「每条请求落在哪个节点的哪个槽位」，不比容量快照指纹——快照会因为
    其他域名的落点变化而变化，但这并不代表本域名的分配结果变了。
    """
    signature = _assignment_signature(assignments)
    latest: dict[str, Any] | None = None
    for row in _versions(result_no):
        if text(row.get("domain")) != domain:
            continue
        if row.get("status") not in CURRENT_STATUSES:
            continue
        if latest is None or as_int(row.get("version")) > as_int(latest.get("version")):
            latest = row
    if latest is None:
        return None
    existing = _assignment_signature(
        Assignment.from_row(item) for item in latest.get("assignments") or []
    )
    return latest if existing == signature else None


def _failure_signature(domain: str, problems: list[str]) -> str:
    return f"{domain}//{'|'.join(sorted(problems))}"


def _existing_failure(signature: str) -> dict[str, Any] | None:
    for row in _result_rows():
        if row.get("failure_signature") == signature:
            return row
    return None


def check_existing(
    *,
    rule_ids: Iterable[int] | None = None,
    node_ids: Iterable[int] | None = None,
    at=None,
) -> dict[str, Any]:
    """只做影响面、槽位与幂等判断，给前端「检查迁移影响」用。"""
    moment = at or clock_now()
    preview = impact_preview(node_ids, at=moment)
    domains = _filter_domains(preview["affected_domains"], rule_ids)
    current = _current_results_by_domain()
    snapshot = _snapshot_for(domains, taken_at=iso_time(moment), extra_salt="check")
    plans = _plan_batch(
        domains=domains,
        current=current,
        snapshot=snapshot,
        recovering=nodestate.recovering_node_ids(_workers(), now=moment),
        moment=moment,
    )
    per_domain: list[dict[str, Any]] = []
    already: list[dict[str, Any]] = []
    for domain, plan in plans.items():
        key = _idempotency_key(domain, preview["affected_nodes"], snapshot)
        content = _content_match(result_no_for(plan["rule"]), domain, plan["assignments"])
        existing = _existing_by_key(key) or content
        per_domain.append({
            "domain": domain,
            "rule_no": text(plan["rule"].get("策略编号")),
            "request_count": _request_count(plan["rule"]),
            "changes": plan["changes"],
            "notes": plan["notes"],
            "slot_problems": plan["problems"],
            "idempotency_key": key,
            "already_processed": existing is not None,
            "existing_result_no": text(existing.get("result_no")) if existing else "",
        })
        if existing is not None:
            already.append({
                "domain": domain,
                "result_no": text(existing.get("result_no")),
                "version": as_int(existing.get("version")),
            })
    return {
        "checked_at": iso_time(moment),
        "domains": domains,
        "untouched_domains": preview["untouched_domains"],
        "affected_nodes": preview["affected_nodes"],
        "stable_window_hold": preview["stable_window_hold"],
        "per_domain": per_domain,
        "slot_problems": sorted({
            problem for plan in plans.values() for problem in plan["problems"]
        }),
        "capacity_fingerprint": snapshot.fingerprint,
        "already_processed": already,
        "message": (
            f"{len(already)} 个域名对应的分配结果已处理过，再次提交只算一次"
            if already else "影响面已检查，可以执行增量再平衡"
        ),
    }


def _filter_domains(
    domains: list[str], rule_ids: Iterable[int] | None
) -> list[str]:
    if not rule_ids:
        return list(domains)
    wanted = {as_int(item) for item in rule_ids}
    return [
        domain for domain in domains
        if as_int((_rule_by_domain(domain) or {}).get("id")) in wanted
    ]


def _move_reason(
    change: dict[str, Any],
    affected_nodes: list[dict[str, Any]],
    snapshot: CapacitySnapshot,
) -> str:
    from_node = text(change.get("from_node"))
    for item in affected_nodes:
        if item.get("node_code") != from_node:
            continue
        if item.get("transition") == "退出分流":
            return f"{from_node} 连续超阈值超出稳定窗口，请求迁出"
        return f"{from_node} 状态异常，请求迁出"
    if not from_node:
        return "新增请求落点"
    row = snapshot.row_for(as_int(change.get("from_node_id")))
    if row is not None and not row.eligible:
        return f"{from_node} 不可承接：{row.reason}"
    return f"{from_node} 容量不足，按剩余容量迁移"


def _compose_result(
    *,
    domain: str,
    rule: dict[str, Any],
    plan: dict[str, Any],
    result_no: str,
    version: int,
    snapshot: CapacitySnapshot,
    affected_nodes: list[dict[str, Any]],
    untouched_domains: list[str],
    previous: dict[str, Any] | None,
    operator: str,
    moment,
    idempotency_key: str,
    conflicts: dict[str, Any],
    batch_no: str,
    revision: int = 1,
) -> dict[str, Any]:
    """把迁移前后的版本、责任节点和容量快照收进一条结果记录。"""
    moves = [
        {**change, "domain": domain, "reason": _move_reason(change, affected_nodes, snapshot)}
        for change in plan["changes"]
    ]
    return {
        "id": next_id(_result_rows()),
        "result_no": result_no,
        "batch_no": batch_no,
        "version": version,
        "revision": revision,
        "status": STATUS_GENERATED,
        "pending": True,
        "abnormal": False,
        "domain": domain,
        "domains": [domain],
        "affected_domains": [domain],
        "untouched_domains": sorted(untouched_domains),
        "affected_nodes": sorted({text(item.get("node_code")) for item in affected_nodes}),
        "rule_ids": [as_int(rule.get("id"))],
        "rule_no": text(rule.get("策略编号")),
        "priority": as_int(rule.get("优先级")),
        "target_domain": text(rule.get("目标域名")),
        "affinity": text(rule.get("节点亲和性")),
        "assignments": [item.as_dict() for item in plan["assignments"]],
        "previous_version": as_int((previous or {}).get("version")),
        "previous_result_no": text((previous or {}).get("result_no")),
        "previous_versions": {domain: as_int((previous or {}).get("version"))},
        "version_conflicts": conflicts,
        "capacity_snapshot": {
            "fingerprint": snapshot.fingerprint,
            "taken_at": snapshot.taken_at,
            "nodes": snapshot.as_list(),
        },
        "notes": plan["notes"],
        "moves": moves,
        "operator": operator,
        "created_at": iso_time(moment),
        "applied_at": "",
        "reviewed_at": "",
        "reviewer": "",
        "review_note": "",
        "sink_sync": {},
        "sink_manifest": [],
        "idempotency_key": idempotency_key,
        "rollback_to": "",
        "rollback_versions": [],
        "failure_reason": "",
        "attribution": {
            "generated_by": operator,
            "generated_at": iso_time(moment),
            "affected_node_owner": "值班管理员",
            "review_required": True,
        },
    }


def _detect_version_conflicts(
    result_no: str, version: int
) -> dict[str, Any]:
    """同名结果版本撞车（例如稳定窗口边界反复变化、多实例同时写入）：两版都留，不覆盖。

    冲突时后写入的一份带着同一个版本号但 revision 加一落库，复核时并排展示，
    由人工选择哪一份生效，绝不出现「后者静默覆盖前者」。
    """
    existing = [row for row in _versions(result_no) if as_int(row.get("version")) == version]
    if not existing:
        return {}
    fingerprints = sorted({
        (row.get("capacity_snapshot") or {}).get("fingerprint", "") for row in existing
    })
    return {
        result_no: {
            "version": version,
            "kept_versions": sorted({as_int(row.get("version")) for row in _versions(result_no)}),
            "kept_revisions": sorted(
                {as_int(row.get("revision"), 1) for row in existing}
            ),
            "capacity_fingerprints": fingerprints,
            "note": "稳定窗口边界反复变化时，新旧分流结果对应不同容量快照",
            "resolution": "保留两个版本，复核后人工选择生效版本",
        }
    }


def _archive_version_history(result: dict[str, Any]) -> None:
    """同域名的旧生效版本标为已被替代，但保留可查，不删除。"""
    result_no = text(result.get("result_no"))
    domain = text(result.get("domain"))
    for row in _result_rows():
        if as_int(row.get("id")) == as_int(result.get("id")):
            continue
        if domain not in (row.get("domains") or []) and text(row.get("domain")) != domain:
            continue
        if row.get("status") == STATUS_APPLIED:
            row["status"] = STATUS_SUPERSEDED
            row["superseded_by"] = result_no


def _record_moves(
    result: dict[str, Any], rule: dict[str, Any], operator: str, moment
) -> None:
    """迁移审计：换节点记原因与稳定窗口，短时间内的重复迁移据此被拦住。"""
    domain = text(result.get("domain"))
    for move in result.get("moves") or []:
        _move_rows().append({
            "id": next_id(_move_rows()),
            "result_no": text(result.get("result_no")),
            "version": as_int(result.get("version")),
            "domain": domain,
            "rule_no": text(rule.get("策略编号")),
            "priority": as_int(rule.get("优先级")),
            "request_key": text(move.get("request_key")),
            "from_node": text(move.get("from_node")),
            "to_node": text(move.get("to_node")),
            "reason": text(move.get("reason")),
            "stable_window": as_int(rule.get("稳定窗口"), 600),
            "moved_at": iso_time(moment),
            "operator": operator,
            "pending": False,
            "abnormal": False,
        })
        # 节点清单上留一句最近迁移原因，运营在采集节点页就能看到
        target = next(
            (node for node in _workers() if text(node.get("节点编号")) == text(move.get("to_node"))),
            None,
        )
        if target is not None:
            target["最近迁移原因"] = (
                f"{text(move.get('request_key'))} 由 {text(move.get('from_node'))}"
                f" 迁入（{text(move.get('reason'))}）"
            )
        source = next(
            (node for node in _workers() if text(node.get("节点编号")) == text(move.get("from_node"))),
            None,
        )
        if source is not None:
            source["最近迁移原因"] = (
                f"{text(move.get('request_key'))} 迁出至 {text(move.get('to_node'))}"
                f"（{text(move.get('reason'))}）"
            )


def _update_rule(result: dict[str, Any], rule: dict[str, Any], moment) -> None:
    rule["当前结果编号"] = text(result.get("result_no"))
    rule["当前结果版本"] = as_int(result.get("version"))
    rule["待复核结果"] = as_int(rule.get("待复核结果")) + 1
    rule["最近再平衡时间"] = iso_time(moment)
    rule["pending"] = True


def _apply_one(
    result: dict[str, Any], rule: dict[str, Any], operator: str, moment
) -> dict[str, Any]:
    """把一份域名方案写到落点侧并回读校验；失败返回原因，由调用方回滚。"""
    domain = text(result.get("domain"))
    assignments = [Assignment.from_row(item) for item in result.get("assignments") or []]
    problems = check_slots(
        assignments,
        _snapshot_for([domain], taken_at=iso_time(moment), extra_salt="apply"),
        occupied=sink_service.occupied_outside([domain]),
    )
    if problems:
        return {"ok": False, "message": "落地前槽位回读失败：" + "；".join(problems[:3])}
    outcome = sink_service.replace_domain(
        result_no=text(result.get("result_no")),
        version=as_int(result.get("version")),
        domain=domain,
        assignments=assignments,
        operator=operator,
    )
    expected = {item.request_key for item in assignments}
    active_for_domain = [
        row for row in sink_service.active_rows() if text(row.get("domain")) == domain
    ]
    actual = {text(row.get("request_key")) for row in active_for_domain}
    if expected != actual:
        return {
            "ok": False,
            "message": f"{domain} 落点回读不一致：期望 {len(expected)} 条，实际 {len(actual)} 条",
        }
    result["released_slots"] = outcome["released"]
    result["occupied_slots"] = outcome["created"]
    result["sink_sync"] = {
        domain: {
            "released": outcome["released"],
            "created": outcome["created"],
            "synced_at": outcome["synced_at"],
        },
    }
    result["applied_at"] = iso_time(moment)
    return {"ok": True}


def _exact_version(result_no: str, version: int) -> dict[str, Any] | None:
    for row in _versions(result_no):
        if as_int(row.get("version")) == version:
            return row
    return None


def _restore_previous(result: dict[str, Any], operator: str) -> list[dict[str, Any]]:
    """把一个域名恢复成上一份完整结果的落点（按版本号精确定位，不取最新）。"""
    domain = text(result.get("domain"))
    previous_no = text(result.get("previous_result_no"))
    previous_version = as_int(result.get("previous_version"))
    previous = _exact_version(previous_no, previous_version) if previous_no else None
    if previous is None:
        sink_service.replace_domain(
            result_no=text(result.get("result_no")),
            version=as_int(result.get("version")),
            domain=domain,
            assignments=[],
            operator=operator,
        )
        return []
    sink_service.replace_domain(
        result_no=previous_no,
        version=previous_version,
        domain=domain,
        assignments=[Assignment.from_row(item) for item in previous.get("assignments") or []],
        operator=operator,
    )
    return [{"domain": domain, "result_no": previous_no, "version": previous_version}]


def _record_validation_failure(
    *,
    domain: str,
    rule: dict[str, Any],
    plan: dict[str, Any],
    snapshot: CapacitySnapshot,
    affected_nodes: list[dict[str, Any]],
    untouched_domains: list[str],
    previous: dict[str, Any] | None,
    operator: str,
    moment,
    batch_key: str,
    batch_no: str,
) -> dict[str, Any]:
    """校验没过也留一条可查的记录，但绝不落地，旧结果继续生效。"""
    failure = _compose_result(
        domain=domain,
        rule=rule,
        plan=plan,
        result_no=result_no_for(rule),
        version=_latest_version(result_no_for(rule)) + 1,
        snapshot=snapshot,
        affected_nodes=affected_nodes,
        untouched_domains=untouched_domains,
        previous=previous,
        operator=operator,
        moment=moment,
        idempotency_key=_idempotency_key(domain, affected_nodes, snapshot),
        conflicts={},
        batch_no=batch_no,
    )
    failure["status"] = STATUS_FAILED
    failure["pending"] = False
    failure["batch_key"] = batch_key
    failure["failure_signature"] = _failure_signature(domain, plan["problems"])
    failure["failure_reason"] = "槽位校验未通过：" + "；".join(plan["problems"][:5])
    failure["assignments"] = []
    if previous is not None:
        failure["rollback_to"] = text(previous.get("result_no"))
        failure["rollback_versions"] = [{
            "domain": domain,
            "result_no": text(previous.get("result_no")),
            "version": as_int(previous.get("version")),
        }]
    _result_rows().append(failure)
    return failure


def rebalance(
    *,
    node_ids: Iterable[int] | None = None,
    rule_ids: Iterable[int] | None = None,
    operator: str = "值班管理员",
    at=None,
    force: bool = False,
) -> dict[str, Any]:
    """执行一次增量再平衡：只重算受影响域名，生成并落地新的分配结果。"""
    moment = at or clock_now()
    with _LOCK:
        selected_nodes = None if node_ids is None else [as_int(item) for item in node_ids]
        nodestate.evaluate_nodes(_workers(), selected_nodes, now=moment, apply=True)
        preview = impact_preview(selected_nodes, at=moment)
        domains = _filter_domains(preview["affected_domains"], rule_ids)
        if not domains:
            affected_codes = {text(item.get("node_code")) for item in preview["affected_nodes"]}
            already_moved = any(
                affected_codes & set(row.get("affected_nodes") or [])
                for row in _result_rows()
                if row.get("status") in CURRENT_STATUSES
            )
            return {
                "ok": True,
                "skipped": True,
                "idempotent": already_moved,
                "message": (
                    "受影响节点的请求已迁出，本轮无需重复再平衡"
                    if already_moved
                    else "受影响节点上没有生效的分配结果，本轮无需再平衡"
                ),
                "affected_nodes": preview["affected_nodes"],
                "stable_window_hold": preview["stable_window_hold"],
                "untouched_domains": preview["untouched_domains"],
                "entries": [],
                "entry": None,
                "skipped_domains": [],
                "failures": [],
                "per_domain": [],
            }

        current = _current_results_by_domain()
        snapshot = _snapshot_for(domains, taken_at=iso_time(moment))
        plans = _plan_batch(
            domains=domains,
            current=current,
            snapshot=snapshot,
            recovering=nodestate.recovering_node_ids(_workers(), now=moment),
            moment=moment,
        )
        # 幂等键优先：同一批受影响节点与容量快照处理过就整批跳过，不重复算
        batch_key = "|".join(sorted(
            _idempotency_key(domain, preview["affected_nodes"], snapshot) for domain in plans
        ))
        if plans and not force:
            existing = _existing_by_key(batch_key)
            if existing is not None:
                return {
                    "ok": True,
                    "skipped": True,
                    "idempotent": True,
                    "message": (
                        "同一份请求分配结果已处理过，本轮不重复生成"
                        f"（{text(existing.get('result_no'))} 第 {as_int(existing.get('version'))} 版）"
                    ),
                    "batch_no": text(existing.get("batch_no")),
                    "affected_nodes": preview["affected_nodes"],
                    "stable_window_hold": preview["stable_window_hold"],
                    "untouched_domains": preview["untouched_domains"],
                    "entries": [],
                    "entry": existing,
                    "skipped_domains": [],
                    "failures": [],
                    "per_domain": [],
                }

        batch_no = f"DIST-BATCH-{moment.strftime('%Y%m%d%H%M%S')}-{len(_result_rows()) + 1:04d}"
        entries: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        per_domain: list[dict[str, Any]] = []

        for domain, plan in plans.items():
            rule = plan["rule"]
            key = _idempotency_key(domain, preview["affected_nodes"], snapshot)
            existing = _existing_by_key(key)
            per_domain.append({
                "domain": domain,
                "rule_no": text(rule.get("策略编号")),
                "request_count": _request_count(rule),
                "changes": plan["changes"],
                "notes": plan["notes"],
                "slot_problems": plan["problems"],
            })
            if existing is not None and not force:
                skipped.append({
                    "domain": domain,
                    "result_no": text(existing.get("result_no")),
                    "version": as_int(existing.get("version")),
                    "reason": "同一份请求分配结果已处理过，只算一次",
                })
                continue
            if plan["problems"]:
                signature = _failure_signature(domain, plan["problems"])
                previous_failure = _existing_failure(signature)
                if previous_failure is not None and not force:
                    skipped.append({
                        "domain": domain,
                        "result_no": text(previous_failure.get("result_no")),
                        "version": as_int(previous_failure.get("version")),
                        "reason": "同一份校验失败结果已处理过，只算一次",
                    })
                    continue
                failure = _record_validation_failure(
                    domain=domain,
                    rule=rule,
                    plan=plan,
                    snapshot=snapshot,
                    affected_nodes=preview["affected_nodes"],
                    untouched_domains=preview["untouched_domains"],
                    previous=current.get(domain),
                    operator=operator,
                    moment=moment,
                    batch_key=batch_key,
                    batch_no=batch_no,
                )
                failures.append({
                    "domain": domain,
                    "message": "槽位校验未通过：" + "；".join(plan["problems"][:3]),
                    "result_id": as_int(failure.get("id")),
                })
                entries.append(failure)
                continue

            result_no = result_no_for(rule)
            version = _latest_version(result_no) + 1
            if concurrency_hook is not None:
                # 演练点：另一个写入方在本次落库前用同一个版本号写了同名结果
                concurrency_hook(result_no, version)
            # 落库前再查一次同名版本：并发写入方可能刚提交过同一版本号，
            # 撞车时两份都保留，各带 revision，绝不静默覆盖。
            conflicts = _detect_version_conflicts(result_no, version)
            revision = 1
            if conflicts:
                revision = max(
                    as_int(row.get("revision"), 1)
                    for row in _versions(result_no)
                    if as_int(row.get("version")) == version
                ) + 1
                conflicts = {
                    **conflicts,
                    result_no: {
                        **next(iter(conflicts.values())),
                        "capacity_fingerprints": sorted(
                            set(next(iter(conflicts.values()))["capacity_fingerprints"])
                            | {snapshot.fingerprint}
                        ),
                        "new_revision": revision,
                    },
                }
            # 内容级幂等：同样的分流已经存在过，就不再生成新版本
            existing = _content_match(result_no, domain, plan["assignments"])
            if existing is not None and not force:
                skipped.append({
                    "domain": domain,
                    "result_no": text(existing.get("result_no")),
                    "version": as_int(existing.get("version")),
                    "reason": "同一份请求分配结果已处理过，只算一次",
                })
                continue
            result = _compose_result(
                domain=domain,
                rule=rule,
                plan=plan,
                result_no=result_no,
                version=version,
                revision=revision,
                snapshot=snapshot,
                affected_nodes=preview["affected_nodes"],
                untouched_domains=preview["untouched_domains"],
                previous=current.get(domain),
                operator=operator,
                moment=moment,
                idempotency_key=key,
                conflicts=conflicts,
                batch_no=batch_no,
            )
            _result_rows().append(result)
            applied = _apply_one(result, rule, operator, moment)
            if not applied["ok"]:
                result["failure_reason"] = applied["message"]
                restored = _restore_previous(result, operator)
                result["status"] = STATUS_FAILED
                result["batch_key"] = batch_key
                result["rollback_versions"] = restored
                result["rollback_to"] = restored[0]["result_no"] if restored else ""
                sink_service.refresh_workers()
                failures.append({"domain": domain, "message": applied["message"]})
                entries.append(result)
                continue

            result["status"] = STATUS_PENDING_REVIEW
            # 整批幂等键写在本批全部结果上，重复提交同一批只算一次
            result["batch_key"] = batch_key
            # 新版本已落到调度侧，旧版本标为已被替代但保留可查
            _archive_version_history(result)
            _record_moves(result, rule, operator, moment)
            _update_rule(result, rule, moment)
            # 归档结果带回调度落点侧清单，另一业务面读到的就是这份清单
            result["sink_manifest"] = sink_service.archived_manifest(result)
            result["sink_view"] = sink_service.manifest_rows()
            entries.append(result)

        generated = [item for item in entries if item.get("status") == STATUS_PENDING_REVIEW]
        ok = not failures
        if ok and not entries and skipped:
            message = f"{len(skipped)} 个域名对应的分配结果已处理过，本轮不重复生成"
        elif ok:
            request_total = sum(len(item.get("assignments") or []) for item in generated)
            message = (
                f"增量再平衡完成：重算 {len(generated)} 个域名、{request_total} 条请求"
            )
        else:
            message = (
                f"增量再平衡部分失败：{len(failures)} 个域名已回滚到上一份完整结果"
                + (f"，{len(generated)} 个域名已生成待复核结果" if generated else "")
            )
        return {
            "ok": ok,
            "skipped": not generated and not failures,
            "idempotent": not generated and not failures and bool(skipped),
            "message": message,
            "batch_no": batch_no,
            "affected_nodes": preview["affected_nodes"],
            "stable_window_hold": preview["stable_window_hold"],
            "untouched_domains": preview["untouched_domains"],
            "entries": entries,
            "entry": generated[0] if generated else (entries[0] if entries else None),
            "skipped_domains": skipped,
            "failures": failures,
            "per_domain": per_domain,
        }


def bootstrap(at=None) -> list[dict[str, Any]]:
    """首次启动时给已启用策略生成基线分配结果，后续再平衡都以它为「上一份完整结果」。

    走的是和正式再平衡同一条计算与落地链路，只是不触发迁移、不算复核工作量。
    """
    moment = at or clock_now()
    with _LOCK:
        if _result_rows():
            return []
        domains = [
            text(rule.get("目标域名"))
            for rule in _rule_rows()
            if rule.get("status") == "已启用" and text(rule.get("目标域名"))
        ]
        if not domains:
            return []
        current: dict[str, dict[str, Any]] = {}
        snapshot = _snapshot_for(domains, taken_at=iso_time(moment), extra_salt="bootstrap")
        plans = _plan_batch(
            domains=domains,
            current=current,
            snapshot=snapshot,
            recovering=set(),
            moment=moment,
        )
        created: list[dict[str, Any]] = []
        for domain, plan in plans.items():
            rule = plan["rule"]
            result_no = result_no_for(rule)
            result = _compose_result(
                domain=domain,
                rule=rule,
                plan=plan,
                result_no=result_no,
                version=1,
                snapshot=snapshot,
                affected_nodes=[],
                untouched_domains=[],
                previous=None,
                operator="系统初始化",
                moment=moment,
                idempotency_key=f"{domain}//bootstrap//{snapshot.fingerprint}",
                conflicts={},
                batch_no=f"DIST-BATCH-{moment.strftime('%Y%m%d%H%M%S')}-INIT",
            )
            _result_rows().append(result)
            applied = _apply_one(result, rule, "系统初始化", moment)
            if not applied["ok"]:
                result["status"] = STATUS_FAILED
                result["failure_reason"] = applied["message"]
                created.append(result)
                continue
            result["status"] = STATUS_APPLIED
            result["pending"] = False
            result["reviewed_at"] = iso_time(moment)
            result["reviewer"] = "系统初始化"
            result["review_note"] = "基线分配结果"
            rule["当前结果编号"] = result_no
            rule["当前结果版本"] = 1
            rule["待复核结果"] = 0
            rule["pending"] = False
            rule["最近再平衡时间"] = iso_time(moment)
            result["sink_manifest"] = sink_service.archived_manifest(result)
            created.append(result)
        return created


def moves_for(*, domain: str | None = None, result_no: str | None = None) -> list[dict[str, Any]]:
    rows = list(_move_rows())
    if domain:
        rows = [row for row in rows if text(row.get("domain")) == domain]
    if result_no:
        rows = [row for row in rows if text(row.get("result_no")) == result_no]
    rows.sort(key=lambda row: (text(row.get("moved_at")), as_int(row.get("id"))), reverse=True)
    return rows


def review_payload(entry_id: int) -> dict[str, Any] | None:
    """复核视图：分配策略、目标域名、节点亲和性、多版本分流结果一次给全。"""
    result = get_result(entry_id)
    if result is None:
        return None
    rules = []
    for rule_id in result.get("rule_ids") or []:
        rule = _rule_by_id(as_int(rule_id))
        if rule is not None:
            rules.append(copy_rows([rule])[0])
    current = _current_results_by_domain()
    domain = text(result.get("domain"))
    history: list[dict[str, Any]] = []
    for row in _versions(text(result.get("result_no"))):
        if row.get("status") not in (
            STATUS_APPLIED, STATUS_PENDING_REVIEW, STATUS_SUPERSEDED, STATUS_ROLLED_BACK,
            STATUS_GENERATED, STATUS_FAILED,
        ):
            continue
        active_for_domain = current.get(domain)
        history.append({
            "result_id": as_int(row.get("id")),
            "result_no": text(row.get("result_no")),
            "version": as_int(row.get("version")),
            "status": text(row.get("status")),
            "created_at": text(row.get("created_at")),
            "is_current": bool(
                active_for_domain is not None
                and as_int(row.get("id")) == as_int(active_for_domain.get("id"))
            ),
            "assignments": copy_rows(
                [item for item in row.get("assignments") or [] if text(item.get("domain")) == domain]
            ),
        })
    history.sort(key=lambda item: (item["created_at"], item["version"]), reverse=True)
    return {
        "result": result,
        "rules": rules,
        "target_domains": sorted({text(rule.get("目标域名")) for rule in rules}),
        "affinities": {
            text(rule.get("目标域名")): text(rule.get("节点亲和性")) for rule in rules
        },
        "versions": history,
        "moves": moves_for(result_no=text(result.get("result_no"))),
        "capacity_snapshot": result.get("capacity_snapshot") or {},
        "sink_manifest": result.get("sink_manifest") or sink_service.archived_manifest(result),
        "sink_view": sink_service.manifest_rows(),
        "traceability": {
            "generated_by": (result.get("attribution") or {}).get("generated_by"),
            "generated_at": (result.get("attribution") or {}).get("generated_at"),
            "reviewer": result.get("reviewer"),
            "reviewed_at": result.get("reviewed_at"),
            "previous_versions": result.get("previous_versions") or {},
            "previous_result_no": result.get("previous_result_no"),
            "affected_nodes": result.get("affected_nodes") or [],
            "operator": result.get("operator"),
            "batch_no": result.get("batch_no"),
        },
    }


def approve(entry_id: int, *, reviewer: str, note: str = "", at=None) -> tuple[dict[str, Any] | None, str]:
    """复核通过：这一版设为域名当前生效，旧版本标为已被替代但保留可查。"""
    moment = at or clock_now()
    with _LOCK:
        result = get_result(entry_id)
        if result is None:
            return None, f"分配结果 {entry_id} 不存在或已归档"
        if result.get("status") not in (STATUS_PENDING_REVIEW, STATUS_GENERATED):
            return None, f"分配结果当前状态为{result.get('status')}，不能复核通过"
        _archive_version_history(result)
        result["status"] = STATUS_APPLIED
        result["pending"] = False
        result["reviewed_at"] = iso_time(moment)
        result["reviewer"] = reviewer
        result["review_note"] = note
        result["applied_at"] = result.get("applied_at") or iso_time(moment)
        domain = text(result.get("domain"))
        rule = _rule_by_domain(domain)
        if rule is not None:
            rule["当前结果编号"] = text(result.get("result_no"))
            rule["当前结果版本"] = as_int(result.get("version"))
            rule["待复核结果"] = max(as_int(rule.get("待复核结果")) - 1, 0)
            rule["pending"] = as_int(rule.get("待复核结果")) > 0
        result["sink_manifest"] = sink_service.archived_manifest(result)
        result["sink_view"] = sink_service.manifest_rows()
        return result, f"分配结果 {text(result.get('result_no'))} 第 {as_int(result.get('version'))} 版已复核通过并生效"


def reject(entry_id: int, *, reviewer: str, note: str = "", at=None) -> tuple[dict[str, Any] | None, str]:
    """复核驳回：回到上一份完整结果，未受影响的域名保持原样。"""
    moment = at or clock_now()
    with _LOCK:
        result = get_result(entry_id)
        if result is None:
            return None, f"分配结果 {entry_id} 不存在或已归档"
        if result.get("status") not in (STATUS_PENDING_REVIEW, STATUS_GENERATED):
            return None, f"分配结果当前状态为{result.get('status')}，不能驳回"
        restored = _restore_previous(result, reviewer)
        result["status"] = STATUS_ROLLED_BACK
        result["pending"] = False
        result["reviewed_at"] = iso_time(moment)
        result["reviewer"] = reviewer
        result["review_note"] = note
        result["rollback_versions"] = restored
        result["rollback_to"] = restored[0]["result_no"] if restored else ""
        if restored:
            # 回到上一份完整结果后，它重新成为当前生效版本
            previous = _exact_version(restored[0]["result_no"], as_int(restored[0]["version"]))
            if previous is not None:
                previous["status"] = STATUS_APPLIED
                previous["superseded_by"] = ""
        rule = _rule_by_domain(text(result.get("domain")))
        if rule is not None:
            previous = _exact_version(
                text(result.get("previous_result_no")), as_int(result.get("previous_version"))
            )
            rule["当前结果编号"] = text(previous.get("result_no")) if previous else ""
            rule["当前结果版本"] = as_int(previous.get("version")) if previous else 0
            rule["待复核结果"] = max(as_int(rule.get("待复核结果")) - 1, 0)
            rule["pending"] = as_int(rule.get("待复核结果")) > 0
        result["sink_manifest"] = sink_service.archived_manifest(result)
        return result, "分配结果已驳回并回到上一份完整结果"


def rollback(entry_id: int, *, operator: str, at=None) -> tuple[dict[str, Any] | None, str]:
    """手动回滚到上一份完整结果，给运营在异常时兜底。"""
    moment = at or clock_now()
    with _LOCK:
        result = get_result(entry_id)
        if result is None:
            return None, f"分配结果 {entry_id} 不存在或已归档"
        if not text(result.get("previous_result_no")):
            return None, "该结果没有更早的完整版本，无法回滚"
        restored = _restore_previous(result, operator)
        if not restored:
            return None, "上一份完整结果已不存在，无法回滚"
        # 回滚到的是精确的上一版，而不是「最新的已生效版本」
        previous = _exact_version(restored[0]["result_no"], as_int(restored[0]["version"]))
        if previous is not None:
            previous["status"] = STATUS_APPLIED
            previous["superseded_by"] = ""
        result["status"] = STATUS_ROLLED_BACK
        result["pending"] = False
        result["reviewed_at"] = iso_time(moment)
        result["reviewer"] = operator
        rule = _rule_by_domain(text(result.get("domain")))
        if rule is not None:
            rule["当前结果编号"] = restored[0]["result_no"]
            rule["当前结果版本"] = as_int(restored[0]["version"])
            rule["待复核结果"] = max(as_int(rule.get("待复核结果")) - 1, 0)
            rule["pending"] = as_int(rule.get("待复核结果")) > 0
        result["rollback_to"] = restored[0]["result_no"]
        result["rollback_versions"] = restored
        result["sink_manifest"] = sink_service.archived_manifest(result)
        sink_service.refresh_workers()
        return result, "已回滚到上一份完整分配结果"

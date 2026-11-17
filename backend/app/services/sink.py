"""调度落点侧清单：请求分配结果真正落地的地方。

职责边界
--------
* 请求分配服务只负责「算出」新的分配结果，不直接改节点槽位；
* 调度落点侧消费结果：先释放该域名下旧的落点，再占用新的槽位；
* 节点清单的已占用/剩余槽位由这里的生效落点反推，不再各自维护一份计数。

落点行按「一条请求一个槽位」记录，状态只有 已落点 / 已释放。同一域名迁移时，
释放旧行与写入新行在同一次调用内完成（外层还有一把重入锁），因此同一请求
不会同时留下两条 已落点 行。
"""
from __future__ import annotations

from typing import Any, Iterable

from app.clock import now as clock_now
from app.services.allocation import Assignment
from app.services.common import as_int, copy_rows, iso_time, next_id, text
from app.store import store

MODULE = "dispatch_sink"
ACTIVE = "已落点"
RELEASED = "已释放"
WORKER_MODULE = "worker"


class SinkService:
    """调度落点清单的读写口径。"""

    def rows(self) -> list[dict[str, Any]]:
        return store.rows(MODULE)

    def active_rows(self, *, result_no: str | None = None) -> list[dict[str, Any]]:
        rows = [row for row in self.rows() if row.get("state") == ACTIVE]
        if result_no is not None:
            rows = [row for row in rows if row.get("result_no") == result_no]
        return rows

    def occupied_by_node(
        self, *, exclude_result_no: str | None = None, exclude_domain: str | None = None
    ) -> dict[int, set[int]]:
        """当前生效的槽位占用；迁移计算时排除要重算的域名/结果。"""
        occupied: dict[int, set[int]] = {}
        for row in self.active_rows():
            if exclude_result_no is not None and row.get("result_no") == exclude_result_no:
                continue
            if exclude_domain is not None and row.get("domain") == exclude_domain:
                continue
            node_id = as_int(row.get("node_id"))
            occupied.setdefault(node_id, set()).add(as_int(row.get("slot")))
        return occupied

    def occupied_outside(self, domains: Iterable[str]) -> dict[int, set[int]]:
        """本次要重算的域名之外、仍然生效的槽位占用。"""
        excluded = {text(domain) for domain in domains}
        occupied: dict[int, set[int]] = {}
        for row in self.active_rows():
            if text(row.get("domain")) in excluded:
                continue
            node_id = as_int(row.get("node_id"))
            occupied.setdefault(node_id, set()).add(as_int(row.get("slot")))
        return occupied

    def integrity_problems(self) -> list[str]:
        """落点清单自检：同一请求只有一条生效行、同一节点同一槽位不撞车。"""
        problems: list[str] = []
        seen_requests: dict[str, dict[str, Any]] = {}
        seen_slots: dict[tuple[int, int], str] = {}
        workers = {as_int(row.get("id")): row for row in store.rows(WORKER_MODULE)}
        for row in self.active_rows():
            key = text(row.get("request_key"))
            if key in seen_requests:
                problems.append(
                    f"{key} 同时占用了 {seen_requests[key].get('node_code')} 与 "
                    f"{row.get('node_code')} 两个槽位"
                )
            seen_requests[key] = row
            slot_key = (as_int(row.get("node_id")), as_int(row.get("slot")))
            if slot_key in seen_slots:
                problems.append(
                    f"{row.get('node_code')} 的槽位 {slot_key[1]} 被两条请求同时占用"
                )
            seen_slots[slot_key] = key
            node = workers.get(slot_key[0])
            if node is not None:
                capacity = as_int(node.get("并发槽位数"))
                if not 1 <= slot_key[1] <= capacity:
                    problems.append(
                        f"{row.get('node_code')} 的槽位 {slot_key[1]} 超出容量 {capacity}"
                    )
        return problems

    def replace_domain(
        self,
        *,
        result_no: str,
        version: int,
        domain: str,
        assignments: Iterable[Assignment],
        operator: str,
    ) -> dict[str, Any]:
        """用新方案替换一个域名的落点：先释放旧槽位，再占用新槽位。"""
        timestamp = iso_time(clock_now())
        released: list[dict[str, Any]] = []
        for row in self.rows():
            if row.get("domain") != domain or row.get("state") != ACTIVE:
                continue
            row["state"] = RELEASED
            row["updated_at"] = timestamp
            row["released_by_version"] = version
            released.append({
                "request_key": text(row.get("request_key")),
                "node_id": as_int(row.get("node_id")),
                "node_code": text(row.get("node_code")),
                "slot": as_int(row.get("slot")),
                "result_no": text(row.get("result_no")),
            })
        created: list[dict[str, Any]] = []
        seen_requests: set[str] = set()
        seen_slots: set[tuple[int, int]] = set()
        for item in assignments:
            if item.request_key in seen_requests:
                # 同一请求只允许占一个槽位，重复行在入口就拦掉
                continue
            if (item.node_id, item.slot) in seen_slots:
                continue
            seen_requests.add(item.request_key)
            seen_slots.add((item.node_id, item.slot))
            row = {
                "id": next_id(self.rows()),
                "node_id": item.node_id,
                "node_code": item.node_code,
                "result_no": result_no,
                "version": version,
                "domain": item.domain or domain,
                "request_key": item.request_key,
                "slot": item.slot,
                "state": ACTIVE,
                "operator": operator,
                "updated_at": timestamp,
            }
            self.rows().append(row)
            created.append(dict(row))
        self.refresh_workers()
        return {
            "released": released,
            "created": created,
            "replaced_domain": domain,
            "synced_at": timestamp,
        }

    def release_domain(self, domain: str, operator: str) -> list[dict[str, Any]]:
        """释放一个域名的全部生效落点，用于回滚前的清场。"""
        timestamp = iso_time(clock_now())
        released: list[dict[str, Any]] = []
        for row in self.rows():
            if row.get("domain") != domain or row.get("state") != ACTIVE:
                continue
            row["state"] = RELEASED
            row["updated_at"] = timestamp
            row["released_by"] = operator
            released.append(dict(row))
        self.refresh_workers()
        return released

    def release_result(self, result_no: str, operator: str) -> list[dict[str, Any]]:
        """释放一个结果编号下所有生效落点。"""
        timestamp = iso_time(clock_now())
        released: list[dict[str, Any]] = []
        for row in self.rows():
            if row.get("result_no") != result_no or row.get("state") != ACTIVE:
                continue
            row["state"] = RELEASED
            row["updated_at"] = timestamp
            row["released_by"] = operator
            released.append(dict(row))
        self.refresh_workers()
        return released

    def refresh_workers(self) -> None:
        """把生效落点折算回节点清单，剩余容量始终取自同一份事实。"""
        occupied = self.occupied_by_node()
        latest_by_node: dict[int, dict[str, Any]] = {}
        for row in self.active_rows():
            node_id = as_int(row.get("node_id"))
            current = latest_by_node.get(node_id)
            if current is None or text(row.get("updated_at")) >= text(current.get("updated_at")):
                latest_by_node[node_id] = row
        for node in store.rows(WORKER_MODULE):
            node_id = as_int(node.get("id"))
            slots = max(as_int(node.get("并发槽位数")), 0)
            used = len(occupied.get(node_id, set()))
            node["已占用槽位"] = used
            node["剩余槽位"] = max(slots - used, 0)
            node["落点请求数"] = used
            latest = latest_by_node.get(node_id)
            node["当前结果编号"] = text(latest.get("result_no")) if latest else ""
            node["当前结果版本"] = as_int(latest.get("version")) if latest else 0

    def assignments_for(self, result_no: str, *, version: int | None = None) -> list[Assignment]:
        rows = self.active_rows(result_no=result_no)
        if version is not None:
            rows = [row for row in rows if as_int(row.get("version")) == version]
        return [
            Assignment.from_row(row)
            for row in sorted(rows, key=lambda item: (text(item.get("domain")), as_int(item.get("slot"))))
        ]

    def manifest_rows(self, *, node_ids: Iterable[int] | None = None) -> list[dict[str, Any]]:
        """按「节点 + 结果编号」汇总的落点清单，另一业务面直接读这张清单。

        每行给出该节点上的这一份结果，同时带上节点整体的占用与剩余容量，
        避免同一节点多份结果各自算剩余容量、加起来对不上。
        """
        selected = None if node_ids is None else {as_int(item) for item in node_ids}
        workers = {as_int(row.get("id")): row for row in store.rows(WORKER_MODULE)}
        node_totals = self.occupied_by_node()
        grouped: dict[tuple[int, str], list[dict[str, Any]]] = {}
        for row in self.active_rows():
            key = (as_int(row.get("node_id")), text(row.get("result_no")))
            grouped.setdefault(key, []).append(row)
        result: list[dict[str, Any]] = []
        for (node_id, result_no), rows in sorted(grouped.items()):
            if selected is not None and node_id not in selected:
                continue
            node = workers.get(node_id, {})
            slots = max(as_int(node.get("并发槽位数")), 0)
            slots_used = sorted({as_int(row.get("slot")) for row in rows})
            node_used = sorted(node_totals.get(node_id, set()))
            versions = sorted({as_int(row.get("version")) for row in rows})
            result.append({
                "node_id": node_id,
                "node_code": text(node.get("节点编号")) or text(rows[0].get("node_code")),
                "host": text(node.get("主机地址")),
                "result_no": result_no,
                "version": versions[-1] if versions else 0,
                "domains": sorted({text(row.get("domain")) for row in rows}),
                "request_count": len(rows),
                "occupied_slots": slots_used,
                "node_occupied_slots": node_used,
                "node_free_slots": max(slots - len(node_used), 0),
                "free_slots": max(slots - len(node_used), 0),
                "slots": slots,
                "requests": sorted(text(row.get("request_key")) for row in rows),
                "updated_at": max(text(row.get("updated_at")) for row in rows),
            })
        return result

    def archived_manifest(self, result: dict[str, Any]) -> list[dict[str, Any]]:
        """归档时带回调度落点侧清单：这份结果的落点、释放情况与剩余容量。"""
        result_no = text(result.get("result_no"))
        version = as_int(result.get("version"))
        domain = text(result.get("domain"))
        wanted = {text(item.get("request_key")) for item in result.get("assignments") or []}
        workers = {as_int(row.get("id")): row for row in store.rows(WORKER_MODULE)}
        node_totals = self.occupied_by_node()
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in self.rows():
            if text(row.get("request_key")) not in wanted:
                continue
            if text(row.get("domain")) != domain:
                continue
            if as_int(row.get("version")) != version:
                continue
            grouped.setdefault(as_int(row.get("node_id")), []).append(row)
        manifest: list[dict[str, Any]] = []
        for node_id, items in sorted(grouped.items()):
            node = workers.get(node_id, {})
            slots = max(as_int(node.get("并发槽位数")), 0)
            active = [item for item in items if item.get("state") == ACTIVE]
            node_used = sorted(node_totals.get(node_id, set()))
            manifest.append({
                "node_id": node_id,
                "node_code": text(node.get("节点编号")) or text(items[0].get("node_code")),
                "result_no": result_no,
                "version": version,
                "state": ACTIVE if active else RELEASED,
                "request_count": len(active),
                "slots": slots,
                "occupied_slots": sorted({as_int(item.get("slot")) for item in active}),
                "node_occupied_slots": node_used,
                "free_slots": max(slots - len(node_used), 0),
                "released_slots": sorted({
                    as_int(item.get("slot"))
                    for item in items
                    if item.get("state") == RELEASED
                }),
                "requests": sorted(text(item.get("request_key")) for item in active),
                "updated_at": max(text(item.get("updated_at")) for item in items),
            })
        return manifest

    def snapshot_rows(self) -> list[dict[str, Any]]:
        return copy_rows(self.manifest_rows())


sink_service = SinkService()

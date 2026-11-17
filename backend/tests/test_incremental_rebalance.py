"""增量再平衡与请求分配结果的核心行为测试。

用固定时钟把「稳定窗口边界」这类时间点钉死，避免用例受运行时刻影响。
每个用例前重置内存仓库，用例之间互不串数据。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.clock import set_now
from app.main import app
from app.services import dispatch_result, nodestate
from app.services.allocation import (
    Assignment,
    build_capacity_snapshot,
    check_slots,
    make_node_capacity,
    plan_domain,
)
from app.store import store

BASE = datetime(2026, 11, 17, 3, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def fresh_store():
    store.reset()
    set_now(BASE)
    dispatch_result.concurrency_hook = None
    dispatch_result.bootstrap()
    yield
    dispatch_result.concurrency_hook = None
    set_now(None)


def at(seconds: int) -> None:
    set_now(BASE + timedelta(seconds=seconds))


def spike(node_id: int, cpu: str = "95") -> None:
    store.find("worker", node_id)["CPU使用率"] = cpu


def active_rows() -> list[dict]:
    return dispatch_result.sink_service.active_rows()


def worker(node_id: int) -> dict:
    return store.find("worker", node_id)


def test_bootstrap_lands_every_enabled_domain_within_slots():
    results = store.rows("dispatch_result")
    assert {row["status"] for row in results} == {"已生效"}
    assert dispatch_result.sink_service.integrity_problems() == []
    for node in store.rows("worker"):
        assert node["已占用槽位"] + node["剩余槽位"] == node["并发槽位数"]
    # 每个已启用策略的各条请求都只出现一次
    keys = [row["request_key"] for row in active_rows()]
    assert len(keys) == len(set(keys))


def test_short_spike_inside_stable_window_does_not_migrate():
    spike(1)
    at(10)
    before = {row["id"]: dict(row) for row in active_rows()}
    outcome = dispatch_result.rebalance(node_ids=[1])
    assert outcome["skipped"] is True
    assert worker(1)["负载状态"] == nodestate.OBSERVING
    assert {row["id"]: dict(row) for row in active_rows()} == before
    assert store.rows("dispatch_move") == []


def test_spike_across_window_boundary_migrates_once_and_recovers_node():
    spike(1)
    at(10)
    dispatch_result.rebalance(node_ids=[1])
    at(9 * 60 + 10)  # 稳定窗口内
    first = dispatch_result.rebalance(node_ids=[1])
    assert first["skipped"] is True
    at(10 * 60 + 11)  # 刚好越过 600 秒窗口
    outcome = dispatch_result.rebalance(node_ids=[1])
    assert outcome["ok"] is True
    assert worker(1)["负载状态"] == nodestate.ABNORMAL
    assert worker(1)["已占用槽位"] == 0
    entry = outcome["entries"][0]
    assert entry["status"] == "待复核"
    assert entry["domain"] == "news.example.com"
    assert all(move["reason"].startswith("WORK-0001") for move in entry["moves"])
    assert dispatch_result.sink_service.integrity_problems() == []


def test_oscillation_across_window_boundary_restarts_window_each_time():
    spike(1, "90")
    at(10)
    dispatch_result.rebalance(node_ids=[1])
    worker(1)["CPU使用率"] = "30"
    at(200)
    dispatch_result.rebalance(node_ids=[1])
    assert worker(1)["负载状态"] == nodestate.NORMAL
    spike(1, "92")
    at(350)
    dispatch_result.rebalance(node_ids=[1])
    started = worker(1)["异常开始时间"]
    spike(1, "88")
    at(400)
    dispatch_result.rebalance(node_ids=[1])
    # 窗口重新从 350 秒处计算，不会沿用旧的异常开始时间
    assert worker(1)["异常开始时间"] == started
    assert worker(1)["负载状态"] == nodestate.OBSERVING


def test_repeating_the_same_result_counts_once():
    spike(1)
    at(10)
    dispatch_result.rebalance(node_ids=[1])
    at(10 * 60 + 11)
    first = dispatch_result.rebalance(node_ids=[1])
    assert first["ok"] is True
    count_after_first = len(active_rows())
    second = dispatch_result.rebalance(node_ids=[1])
    assert second["skipped"] is True
    assert second["idempotent"] is True
    assert len(active_rows()) == count_after_first
    assert len(store.rows("dispatch_result")) == 4  # 3 条基线 + 1 条新版本


def test_unaffected_domain_never_moves():
    spike(1)
    at(10)
    dispatch_result.rebalance(node_ids=[1])
    at(10 * 60 + 11)
    untouched_before = {
        row["result_no"]: sorted(
            item["request_key"] for item in active_rows() if item["domain"] == "docs.example.com"
        )
        for row in active_rows()
        if row["domain"] == "docs.example.com"
    }
    outcome = dispatch_result.rebalance(node_ids=[1])
    assert outcome["untouched_domains"] == ["docs.example.com", "shop.example.com"]
    untouched_after = {
        row["result_no"]: sorted(
            item["request_key"] for item in active_rows() if item["domain"] == "docs.example.com"
        )
        for row in active_rows()
        if row["domain"] == "docs.example.com"
    }
    assert untouched_before == untouched_after


def test_version_conflict_keeps_both_versions():
    spike(1)
    at(10)
    dispatch_result.rebalance(node_ids=[1])
    at(10 * 60 + 11)

    def collide(result_no: str, version: int) -> None:
        clone = dict(dispatch_result._versions(result_no)[-1])
        clone.update({
            "id": 9000,
            "version": version,
            "revision": 1,
            "status": "待复核",
            "capacity_snapshot": {"fingerprint": "snap-other", "taken_at": "x", "nodes": []},
            "assignments": [],
        })
        store.rows("dispatch_result").append(clone)

    dispatch_result.concurrency_hook = collide
    outcome = dispatch_result.rebalance(node_ids=[1])
    entry = outcome["entries"][0]
    assert entry["version_conflicts"]
    conflict = entry["version_conflicts"][entry["result_no"]]
    assert set(conflict["kept_versions"]) == {1, entry["version"]}
    assert len(conflict["capacity_fingerprints"]) == 2
    kept = [
        row for row in store.rows("dispatch_result")
        if row["result_no"] == entry["result_no"]
        and row["version"] == entry["version"]
    ]
    assert len(kept) == 2
    assert {row["revision"] for row in kept} == {1, 2}


def test_concurrent_rebalance_does_not_fork_results():
    import threading

    spike(1)
    spike(2)
    at(10)
    dispatch_result.rebalance()
    at(10 * 60 + 11)
    outcomes = []
    threads = [
        threading.Thread(target=lambda: outcomes.append(dispatch_result.rebalance()))
        for _ in range(6)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    generated = sum(len(outcome["entries"]) for outcome in outcomes)
    assert generated == 2  # 两个受影响域名各生成一次
    assert dispatch_result.sink_service.integrity_problems() == []
    keys = [row["request_key"] for row in active_rows()]
    assert len(keys) == len(set(keys))


def test_review_payload_contains_policy_domain_affinity_and_versions():
    spike(1)
    at(10)
    dispatch_result.rebalance(node_ids=[1])
    at(10 * 60 + 11)
    entry = dispatch_result.rebalance(node_ids=[1])["entries"][0]
    payload = dispatch_result.review_payload(entry["id"])
    assert payload["target_domains"] == ["news.example.com"]
    assert payload["affinities"]["news.example.com"] == "华东1区"
    assert payload["rules"][0]["分配算法"] == "加权轮询"
    assert {item["version"] for item in payload["versions"]} >= {1, 2}
    assert payload["traceability"]["generated_by"]
    assert payload["sink_manifest"]


def test_reject_restores_previous_result_and_slot_counts():
    spike(1)
    at(10)
    dispatch_result.rebalance(node_ids=[1])
    at(10 * 60 + 11)
    entry = dispatch_result.rebalance(node_ids=[1])["entries"][0]
    before = {(row["node_id"], row["slot"], row["request_key"]) for row in active_rows()
              if row["domain"] == "news.example.com"}
    restored, message = dispatch_result.reject(entry["id"], reviewer="李复核")
    assert restored is not None
    assert "回到上一份完整结果" in message
    after = {(row["node_id"], row["slot"], row["request_key"]) for row in active_rows()
             if row["domain"] == "news.example.com"}
    assert before != after
    assert worker(1)["已占用槽位"] == 6
    assert dispatch_result.sink_service.integrity_problems() == []


def test_failed_migration_keeps_previous_allocation():
    # 三个承载域名的节点同时退出，容量不足以重排全部请求
    for node_id in (1, 2, 4):
        spike(node_id)
    at(10)
    dispatch_result.rebalance()
    at(10 * 60 + 11)
    before = {(row["node_id"], row["slot"], row["request_key"]) for row in active_rows()}
    outcome = dispatch_result.rebalance()
    assert outcome["ok"] is False
    assert outcome["failures"]
    after = {(row["node_id"], row["slot"], row["request_key"]) for row in active_rows()
             if row["domain"] in (outcome["failures"][0]["domain"], "shop.example.com")}
    assert after <= before
    assert dispatch_result.sink_service.integrity_problems() == []


def test_recovering_node_does_not_take_new_requests_immediately():
    spike(1)
    at(10)
    dispatch_result.rebalance(node_ids=[1])
    at(10 * 60 + 11)
    entry = dispatch_result.rebalance(node_ids=[1])["entries"][0]
    dispatch_result.approve(entry["id"], reviewer="张复核")
    worker(1)["CPU使用率"] = "30"
    at(700)
    dispatch_result.rebalance(node_ids=[1])
    recovering = nodestate.recovering_node_ids(store.rows("worker"), now=BASE + timedelta(seconds=800))
    assert 1 in recovering
    assert worker(1)["负载状态"] == nodestate.NORMAL


def test_sink_manifest_reflects_actual_free_capacity():
    spike(1)
    at(10)
    dispatch_result.rebalance(node_ids=[1])
    at(10 * 60 + 11)
    dispatch_result.rebalance(node_ids=[1])
    manifest = dispatch_result.sink_service.manifest_rows()
    by_node: dict[int, dict] = {}
    for row in manifest:
        by_node.setdefault(row["node_id"], row)
    for node_id, row in by_node.items():
        node = worker(node_id)
        assert row["node_free_slots"] == node["剩余槽位"]
        assert set(row["node_occupied_slots"]) == set(
            dispatch_result.sink_service.occupied_by_node().get(node_id, set())
        )


def test_capacity_snapshot_and_plan_share_one_entry():
    workers = store.rows("worker")
    occupied = dispatch_result.sink_service.occupied_by_node()
    snapshot = build_capacity_snapshot(workers, occupied=occupied)
    assert len(snapshot.nodes) == len(workers)
    first = snapshot.nodes[0]
    manual = make_node_capacity(
        workers[0], occupied_slots=occupied.get(workers[0]["id"], ())
    )
    assert first.as_dict() == manual.as_dict()
    assignments, _ = plan_domain(
        domain="probe.example.com",
        request_count=2,
        snapshot=snapshot,
    )
    assert check_slots(assignments, snapshot, occupied=occupied) == []


def test_plan_never_double_books_a_request_or_a_slot():
    snapshot = build_capacity_snapshot(store.rows("worker"))
    assignments = [
        Assignment("a", "x", 1, "WORK-0001", 1, 1),
        Assignment("a", "x", 2, "WORK-0002", 1, 1),
    ]
    problems = check_slots(assignments, snapshot)
    assert any("不能同时占新旧槽位" in problem for problem in problems)


def test_api_rebalance_and_review_flow():
    spike(1)
    with TestClient(app) as client:
        client.post(
            "/api/dispatch-result/rebalance",
            json={"values": {"node_ids": [1], "at": "2026-11-17T03:00:10+00:00"}},
        )
        response = client.post(
            "/api/dispatch-result/rebalance",
            json={"values": {"node_ids": [1], "at": "2026-11-17T03:10:11+00:00"}},
        )
        payload = response.json()
        assert payload["ok"] is True
        entry = payload["extra"]["entries"][0]
        review = client.get(f"/api/dispatch-result/{entry['id']}/review").json()
        assert review["target_domains"] == ["news.example.com"]
        approved = client.post(
            f"/api/dispatch-result/{entry['id']}/actions",
            json={"values": {"action": "复核通过", "reviewer": "张复核"}},
        ).json()
        assert approved["ok"] is True
        manifest = client.get("/api/dispatch-result/manifest").json()
        assert manifest["integrity"] == []
        capacity = client.get("/api/worker/capacity").json()
        assert capacity["fingerprint"]

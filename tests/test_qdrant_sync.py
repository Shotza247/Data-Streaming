"""Tests for the incremental Qdrant sync (no Qdrant, MinIO or embedding model needed)."""
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from agents import embed_tax_records as et   # noqa: E402
from scripts import sync_qdrant as sq        # noqa: E402


@pytest.fixture(autouse=True)
def fake_qdrant_models(monkeypatch):
    """qdrant-client is only installed in the dashboard image; stub the bits we use."""
    if "qdrant_client.models" in sys.modules:
        return
    models = types.ModuleType("qdrant_client.models")
    models.PointStruct = lambda id, vector, payload: types.SimpleNamespace(
        id=id, vector=vector, payload=payload)
    models.Distance = types.SimpleNamespace(COSINE="Cosine")
    models.VectorParams = lambda size, distance: (size, distance)
    pkg = types.ModuleType("qdrant_client")
    pkg.models = models
    monkeypatch.setitem(sys.modules, "qdrant_client", pkg)
    monkeypatch.setitem(sys.modules, "qdrant_client.models", models)


class FakeClient:
    """Minimal stand-in for QdrantClient: retrieve() + upsert() over a dict."""

    def __init__(self, existing_ids=()):
        self.points = {pid: {} for pid in existing_ids}
        self.retrieve_calls = 0

    def retrieve(self, collection_name, ids, with_payload=False, with_vectors=False):
        self.retrieve_calls += 1
        return [types.SimpleNamespace(id=i) for i in ids if i in self.points]

    def upsert(self, collection_name, points, wait=True):
        for p in points:
            self.points[p.id] = p.payload

    def get_collections(self):
        return types.SimpleNamespace(collections=[types.SimpleNamespace(name=et.COLLECTION_NAME)])


def rec(app_id, **kw):
    base = {"application_id": app_id, "customer_id": f"c-{app_id}", "taxable_income": 100000.0,
            "employment_type": "Salaried", "employment_status": "Employed",
            "filing_status": "Single", "province": "Gauteng", "is_fraud": False,
            "flink_processed_at": pd.Timestamp("2026-01-01 10:00:00")}
    base.update(kw)
    return base


def test_point_id_is_stable_and_distinct():
    assert et.point_id("a") == et.point_id("a")
    assert et.point_id("a") != et.point_id("b")


def test_find_new_skips_already_embedded():
    client = FakeClient(existing_ids=[et.point_id("a"), et.point_id("c")])
    new = et.find_new(client, [rec("a"), rec("b"), rec("c"), rec("d")])
    assert [r["application_id"] for r in new] == ["b", "d"]


def test_find_new_chunks_lookups():
    client = FakeClient()
    records = [rec(str(i)) for i in range(10)]
    assert len(et.find_new(client, records, chunk=4)) == 10
    assert client.retrieve_calls == 3


def test_payload_keeps_old_fields_and_adds_new_ones():
    p = et.build_payload(rec("a", is_fraud=True))
    for old in ("application_id", "customer_id", "taxable_income", "employment_type",
                "province", "risk_score", "is_fraud"):
        assert old in p
    assert p["employment_status"] == "Employed"
    assert p["filing_status"] == "Single"
    assert p["is_fraud"] is True
    assert p["processed_at"].startswith("2026-01-01")


def test_payload_without_timestamp():
    r = rec("a")
    r.pop("flink_processed_at")
    assert et.build_payload(r)["processed_at"] is None


@pytest.fixture
def patched(monkeypatch):
    """Patch the data source and embedder used by sync_new."""
    import storage.duckdb_queries as dq
    import agents.embeddings as emb

    NEWEST = pd.Timestamp("2026-01-01 10:00:00", tz="UTC")
    state = {"df": pd.DataFrame([rec("a"), rec("b"), rec("b"), rec("c")]), "embedded": [], "calls": []}

    def fake_arrivals(since=None, max_files=5000):
        state["calls"].append((since, max_files))
        return state["df"], NEWEST

    monkeypatch.setattr(dq, "get_recent_arrivals", fake_arrivals)
    monkeypatch.setitem(et._watermark, "since", None)

    def fake_embed(texts, batch_size=64, show_progress=False):
        state["embedded"] += texts
        return [[0.0] * et.VECTOR_DIM for _ in texts]

    monkeypatch.setattr(emb, "embed_batch", fake_embed)
    return state


def test_sync_new_adds_only_missing_and_dedupes(patched):
    client = FakeClient(existing_ids=[et.point_id("a")])
    assert et.sync_new(client=client, quiet=True) == 2        # b and c; duplicate b once
    assert set(client.points) == {et.point_id(x) for x in "abc"}
    assert len(patched["embedded"]) == 2


def test_sync_new_is_idempotent_and_skips_model_when_nothing_new(patched):
    client = FakeClient()
    assert et.sync_new(client=client, quiet=True) == 3
    patched["embedded"].clear()
    assert et.sync_new(client=client, quiet=True) == 0
    assert patched["embedded"] == []


def test_sync_new_picks_up_records_that_arrive_later(patched):
    client = FakeClient()
    et.sync_new(client=client, quiet=True)
    patched["df"] = pd.DataFrame([rec("a"), rec("z")])
    assert et.sync_new(client=client, quiet=True) == 1
    assert et.point_id("z") in client.points


def test_sync_new_empty_source(patched):
    patched["df"] = pd.DataFrame()
    assert et.sync_new(client=FakeClient(), quiet=True) == 0


def test_run_loop_once_returns_total():
    assert sq.run_loop(1, 10, once=True, sync=lambda limit, quiet: 7) == 7


def test_run_loop_retries_after_failure_then_stops():
    calls, sleeps = [], []

    def flaky(limit, quiet):
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("minio down")
        if len(calls) == 4:
            raise KeyboardInterrupt        # escape the infinite loop
        return 2

    with pytest.raises(KeyboardInterrupt):
        sq.run_loop(5, 10, once=False, sync=flaky, sleep=sleeps.append)
    assert sleeps[0] == 10          # backed off after the failure (5 * 2)
    assert sleeps[1:] == [5, 5]     # back to the normal interval after success


def test_run_loop_once_propagates_failure():
    def boom(limit, quiet):
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        sq.run_loop(1, 10, once=True, sync=boom)


def test_sync_reads_only_new_files_after_first_pass(patched):
    client = FakeClient()
    et.sync_new(limit=123, client=client, quiet=True)
    et.sync_new(limit=123, client=client, quiet=True)
    (first_since, first_max), (second_since, _) = patched["calls"]
    assert first_since is None and first_max == 123
    assert second_since is not None
    assert second_since < pd.Timestamp("2026-01-01 10:00:00", tz="UTC")   # overlap is applied


def test_watermark_not_advanced_when_embedding_fails(patched, monkeypatch):
    import agents.embeddings as emb

    def boom(*a, **k):
        raise RuntimeError("model unavailable")

    monkeypatch.setattr(emb, "embed_batch", boom)
    with pytest.raises(RuntimeError):
        et.sync_new(client=FakeClient(), quiet=True)
    assert et._watermark["since"] is None


def test_select_files_newest_first_with_since_and_cap():
    import storage.duckdb_queries as dq
    t = pd.Timestamp("2026-01-01", tz="UTC")
    files = [(f"f{i}", t + pd.Timedelta(minutes=i)) for i in range(10, 0, -1)]   # newest first
    assert [k for k, _ in dq._select_files(files, None, 3)] == ["f10", "f9", "f8"]
    assert [k for k, _ in dq._select_files(files, t + pd.Timedelta(minutes=8), 100)] == ["f10", "f9"]
    assert dq._select_files(files, t + pd.Timedelta(minutes=99)) == []


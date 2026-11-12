"""The vendored transport's wider Cloud API surface and typed errors, against a mocked httpx transport."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import httpx
import pytest

from langchain_kosa8 import _kosa8 as k

Answer = Callable[[httpx.Request], httpx.Response]


def sbx(**over: Any) -> dict[str, Any]:
    d = {"id": "sbx_1", "state": "running", "template": "base"}
    d.update(over)
    return d


def clients(answer: Answer) -> tuple[k.Kosa8Client, k.AsyncKosa8Client, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return answer(req)

    t = httpx.MockTransport(handler)
    sync = k.Kosa8Client(api_key="k8c_live_x", base_url="https://cp.test", http_client=httpx.Client(transport=t), stream=False)
    asyn = k.AsyncKosa8Client(api_key="k8c_live_x", base_url="https://cp.test", http_client=httpx.AsyncClient(transport=t), stream=False)
    return sync, asyn, seen


def q(req: httpx.Request) -> tuple[str, dict[str, str]]:
    u = urlsplit(str(req.url))
    return u.path, dict(parse_qsl(u.query))


def test_default_base_url_is_api_kosa8_com(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KOSA8_CLOUD_URL", raising=False)
    assert k.DEFAULT_BASE_URL == "https://api.kosa8.com"
    assert k.Kosa8Client(api_key="k8c_live_x").base_url == "https://api.kosa8.com"
    assert k.AsyncKosa8Client(api_key="k8c_live_x").base_url == "https://api.kosa8.com"
    monkeypatch.setenv("KOSA8_CLOUD_URL", "https://cp.example.com/")
    assert k.Kosa8Client(api_key="k8c_live_x").base_url == "https://cp.example.com"


def test_create_validates_lifetime() -> None:
    c, _, seen = clients(lambda r: httpx.Response(201, json={"sandbox": sbx()}))
    c.create_sandbox(delete_after=1_900_000_000_000)
    assert json.loads(seen[-1].content) == {"delete_after": 1_900_000_000_000}
    for bad in ({"ttl_s": 59}, {"ttl_s": 7_776_001}, {"ttl_s": 60, "delete_after": 1}):
        with pytest.raises(ValueError):
            c.create_sandbox(**bad)
    assert len(seen) == 1


def test_list_pages_update_and_dataplane() -> None:
    def answer(r: httpx.Request) -> httpx.Response:
        path, query = q(r)
        if path == "/v1/sandboxes" and r.method == "GET":
            if query.get("cursor") == "c2":
                return httpx.Response(200, json={"sandboxes": [sbx(id="sbx_3")]})
            return httpx.Response(200, json={"sandboxes": [sbx(id="sbx_1"), sbx(id="sbx_2")], "next_cursor": "c2"})
        if r.method == "PATCH":
            return httpx.Response(200, json={"sandbox": sbx(delete_after=5)})
        if path.endswith("/files") and r.method == "GET":
            return httpx.Response(200, content=b"\x00\xff", headers={"content-type": "application/octet-stream"})
        if path.endswith("/files") and r.method == "PUT":
            return httpx.Response(200, json={"file": {"path": query["path"], "size": len(r.content)}})
        if path.endswith("/files/list"):
            return httpx.Response(200, json={"path": query["path"], "files": [{"name": "a"}]})
        if path.endswith("/checkpoints") and r.method == "POST":
            return httpx.Response(201, json={"checkpoint": {"id": "ckp_1"}})
        if path.endswith("/checkpoints"):
            return httpx.Response(200, json={"checkpoints": [{"id": "ckp_1"}], "max": 10})
        if path.endswith("/rewind"):
            return httpx.Response(200, json={"sandbox": sbx(), "checkpoint": {"id": "ckp_1"}, "backup": {"id": "ckp_2"}})
        if path.endswith("/fork"):
            return httpx.Response(201, json={"parent": "sbx_1", "sandboxes": [sbx(id="sbx_f", parent_id="sbx_1")]})
        return httpx.Response(404, json={"error": "not_found", "message": "no route"})

    c, a, seen = clients(answer)
    page = c.list_sandboxes(limit=2, all=True)
    assert [s["id"] for s in page.sandboxes] == ["sbx_1", "sbx_2"] and page.next_cursor == "c2"
    assert q(seen[-1])[1] == {"all": "1", "limit": "2"}
    assert [s["id"] for s in c.iter_sandboxes(page_size=2)] == ["sbx_1", "sbx_2", "sbx_3"]
    assert c.update_sandbox("sbx_1", ttl_s=600)["delete_after"] == 5
    assert json.loads(seen[-1].content) == {"ttl_s": 600}
    c.update_sandbox("sbx_1", clear_lifetime=True)
    assert json.loads(seen[-1].content) == {"ttl_s": None}
    with pytest.raises(ValueError):
        c.update_sandbox("sbx_1")
    assert c.read_file("sbx_1", "/workspace/a b") == b"\x00\xff"
    assert q(seen[-1]) == ("/v1/sandboxes/sbx_1/files", {"path": "/workspace/a b"})
    assert c.write_file("sbx_1", "/workspace/x", b"abc")["size"] == 3
    assert seen[-1].headers["content-type"] == "application/octet-stream" and seen[-1].content == b"abc"
    assert c.list_files("sbx_1")[0]["name"] == "a"
    assert c.create_checkpoint("sbx_1", "before")["id"] == "ckp_1"
    assert json.loads(seen[-1].content) == {"label": "before"}
    assert c.list_checkpoints("sbx_1")[0]["id"] == "ckp_1"
    assert c.rewind_sandbox("sbx_1", "ckp_1")["backup"]["id"] == "ckp_2"
    assert c.fork_sandbox("sbx_1", 1)[0]["parent_id"] == "sbx_1"
    with pytest.raises(ValueError):
        c.fork_sandbox("sbx_1", 101)

    async def go() -> None:
        assert [s["id"] async for s in a.iter_sandboxes()] == ["sbx_1", "sbx_2", "sbx_3"]
        assert (await a.update_sandbox("sbx_1", sleep_after_s=0))["delete_after"] == 5
        assert await a.read_file("sbx_1", "/workspace/a") == b"\x00\xff"
        assert (await a.write_file("sbx_1", "/workspace/x", b"ab"))["size"] == 2
        assert (await a.list_files("sbx_1"))[0]["name"] == "a"
        assert (await a.create_checkpoint("sbx_1"))["id"] == "ckp_1"
        assert (await a.list_checkpoints("sbx_1"))[0]["id"] == "ckp_1"
        assert (await a.rewind_sandbox("sbx_1", "ckp_1"))["checkpoint"]["id"] == "ckp_1"
        assert len(await a.fork_sandbox("sbx_1")) == 1
        page = await a.list_sandboxes(cursor="c2")
        assert page.next_cursor is None

    asyncio.run(go())


def test_iter_stops_on_a_repeated_cursor() -> None:
    c, _, seen = clients(lambda r: httpx.Response(200, json={"sandboxes": [sbx()], "next_cursor": "same"}))
    assert len(list(c.iter_sandboxes())) == 2
    assert len(seen) == 2


@pytest.mark.parametrize(
    ("status", "body", "headers", "cls"),
    [
        (402, {"error": "spending_cap_reached", "message": "cap", "upgrade_url": "https://app.kosa8.com/#billing",
               "agent_message": "Tell your person."}, {}, k.Kosa8PaymentRequiredError),
        (423, {"error": "org_frozen", "message": "frozen"}, {}, k.Kosa8OrgFrozenError),
        (429, {"error": "quota_exceeded", "message": "limit"}, {"retry-after": "3"}, k.Kosa8RateLimitError),
        (501, {"error": "not_available", "message": "checkpoints is not available on this host yet"}, {}, k.Kosa8NotSupportedError),
    ],
)
def test_typed_errors(status: int, body: dict[str, Any], headers: dict[str, str], cls: type[k.Kosa8Error]) -> None:
    c, _, _ = clients(lambda r: httpx.Response(status, json=body, headers=headers))
    with pytest.raises(cls) as e:
        c.create_checkpoint("sbx_1")
    assert e.value.status == status and e.value.code == body["error"]
    if cls is k.Kosa8PaymentRequiredError:
        assert e.value.upgrade_url == "https://app.kosa8.com/#billing"
        assert e.value.agent_message == "Tell your person."
    if cls is k.Kosa8RateLimitError:
        assert e.value.retry_after == 3.0


def test_payment_required_without_extras() -> None:
    c, _, _ = clients(lambda r: httpx.Response(402, json={"error": "payment_required", "message": "pay"}))
    with pytest.raises(k.Kosa8PaymentRequiredError) as e:
        c.create_sandbox()
    assert e.value.upgrade_url is None and e.value.agent_message is None


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (502, {"error": "host_failed", "message": "exec failed on the host: host unreachable: timed out"}),
        (504, {"error": "exec_timeout", "message": "the command ran past 30 s"}),
    ],
)
def test_exec_timeout(status: int, body: dict[str, Any]) -> None:
    c, _, _ = clients(lambda r: httpx.Response(status, json=body))
    with pytest.raises(k.Kosa8ExecTimeoutError) as e:
        c.exec("sbx_1", k.ExecRequest(argv=["sleep", "40"]))
    assert isinstance(e.value, k.Kosa8TimeoutError)
    assert "30 s" in str(e.value)


def test_non_exec_502_is_not_an_exec_timeout() -> None:
    c, _, _ = clients(lambda r: httpx.Response(502, json={"error": "host_failed", "message": "timed out"}))
    with pytest.raises(k.Kosa8TimeoutError) as e:
        c.sleep_sandbox("sbx_1")
    assert not isinstance(e.value, k.Kosa8ExecTimeoutError)

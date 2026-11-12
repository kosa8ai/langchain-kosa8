"""Tests for the vendored kosa8 Cloud transport, against the fake API."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import httpx
import pytest

from langchain_kosa8 import _kosa8 as k
from tests.fake_cloud import FakeCloud, container_runner


@pytest.fixture
def cloud() -> FakeCloud:
    return FakeCloud()


@pytest.fixture
def client(cloud: FakeCloud) -> k.Kosa8Client:
    return k.Kosa8Client(api_key=cloud.api_key, http_client=cloud.client())


@pytest.fixture
def session(tmp_path: Path) -> k.SandboxSession:
    ws = tmp_path / "workspace"
    ws.mkdir()
    return k.SandboxSession(workspace=str(ws), persist_dirs=())


def run(client: k.Kosa8Client, sid: str, op):  # noqa: ANN001, ANN202
    return k.drive(op, lambda req: client.exec(sid, req))


def test_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KOSA8_API_KEY", raising=False)
    with pytest.raises(k.Kosa8Error, match="KOSA8_API_KEY"):
        k.Kosa8Client()


def test_rejects_plain_http_for_remote_hosts() -> None:
    with pytest.raises(k.Kosa8Error, match="https"):
        k.Kosa8Client(api_key="x", base_url="http://api.example.com")
    k.Kosa8Client(api_key="x", base_url="http://localhost:8787")


def test_repr_never_shows_key() -> None:
    c = k.Kosa8Client(api_key="k8c_live_secret")
    assert "secret" not in repr(c)


def test_create_get_sleep_wake_delete(cloud: FakeCloud, client: k.Kosa8Client) -> None:
    box = client.create_sandbox(template="python", ttl_s=600, sleep_after_s=300)
    assert box["state"] == "running"
    assert cloud.requests[0] == ("POST", "/v1/sandboxes", {"template": "python", "ttl_s": 600, "sleep_after_s": 300})
    assert client.get_sandbox(box["id"])["id"] == box["id"]
    assert client.sleep_sandbox(box["id"])["state"] == "sleeping"
    assert client.wake_sandbox(box["id"])["state"] == "running"
    client.delete_sandbox(box["id"])
    client.delete_sandbox(box["id"])  # missing is fine by default
    with pytest.raises(k.Kosa8Error) as e:
        client.delete_sandbox(box["id"], missing_ok=False)
    assert e.value.status == 404
    assert e.value.code == "not_found"


def test_error_carries_code_and_message(cloud: FakeCloud) -> None:
    c = k.Kosa8Client(api_key="wrong", http_client=cloud.client())
    with pytest.raises(k.Kosa8Error) as e:
        c.list_templates()
    assert e.value.status == 401
    assert e.value.code == "unauthorized"


def test_falls_back_to_buffered_exec_when_stream_is_unavailable(cloud: FakeCloud, client: k.Kosa8Client, session: k.SandboxSession) -> None:
    sid = client.create_sandbox()["id"]
    out = run(client, sid, session.command_op("echo hi"))
    assert out.exit_code == 0
    assert out.stdout == "hi\n"
    streamed = [b for (_, p, b) in cloud.requests if p.endswith("/exec") and b.get("stream")]
    assert len(streamed) == 1  # asked once, then remembered
    run(client, sid, session.command_op("true"))
    streamed = [b for (_, p, b) in cloud.requests if p.endswith("/exec") and b.get("stream")]
    assert len(streamed) == 1


def test_uses_streamed_exec_when_offered(session: k.SandboxSession) -> None:
    cloud = FakeCloud(stream=True)
    client = k.Kosa8Client(api_key=cloud.api_key, http_client=cloud.client())
    sid = client.create_sandbox()["id"]
    out = run(client, sid, session.command_op("echo out; echo err >&2; exit 3"))
    assert (out.exit_code, out.stdout, out.stderr) == (3, "out\n", "err\n")
    assert all(b.get("stream") for (_, p, b) in cloud.requests if p.endswith("/exec"))


def test_stream_required_raises_when_unavailable(cloud: FakeCloud, session: k.SandboxSession) -> None:
    client = k.Kosa8Client(api_key=cloud.api_key, http_client=cloud.client(), stream=True)
    sid = client.create_sandbox()["id"]
    with pytest.raises(k.Kosa8Error) as e:
        run(client, sid, session.command_op("true"))
    assert e.value.status == 501


def test_command_cwd_env_stdin(client: k.Kosa8Client, session: k.SandboxSession, tmp_path: Path) -> None:
    sid = client.create_sandbox()["id"]
    (tmp_path / "workspace" / "sub").mkdir()
    out = run(client, sid, session.command_op("pwd; echo $GREETING; cat", cwd="sub", env={"GREETING": "hello"}, stdin=b"from stdin"))
    lines = out.stdout.splitlines()
    assert Path(lines[0]).resolve() == (tmp_path / "workspace" / "sub").resolve()
    assert lines[1:] == ["hello", "from stdin"]


def test_internal_variables_are_hidden_from_the_command(client: k.Kosa8Client, session: k.SandboxSession) -> None:
    sid = client.create_sandbox()["id"]
    out = run(client, sid, session.command_op("env | grep '^K8_' || true"))
    assert out.stdout == ""


def test_reserved_and_invalid_env_names_are_rejected(session: k.SandboxSession) -> None:
    with pytest.raises(ValueError):
        session.command_op("true", env={"K8_X": "1"})
    with pytest.raises(ValueError):
        session.command_op("true", env={"1BAD": "1"})


def test_large_command_and_stdin_spill_to_files(cloud: FakeCloud, client: k.Kosa8Client, session: k.SandboxSession) -> None:
    sid = client.create_sandbox()["id"]
    big = "x" * 100_000
    out = run(client, sid, session.command_op(f"printf %s '{big}' | wc -c"))
    assert out.stdout.strip() == "100000"
    data = os.urandom(150_000)
    out = run(client, sid, session.command_op("wc -c", stdin=data))
    assert out.stdout.strip() == "150000"
    # every request stayed under the API's body limit
    assert all(len(str(b)) < 64 * 1024 for (_, _, b) in cloud.requests if b)
    # spill files are cleaned up
    assert run(client, sid, session.command_op("ls -A .kosa8-run 2>/dev/null | wc -l")).stdout.strip() == "0"


def test_timeout_kills_the_command(client: k.Kosa8Client, session: k.SandboxSession) -> None:
    if not (Path("/usr/bin/timeout").exists() or Path("/bin/timeout").exists()):
        pytest.skip("no timeout binary on this machine")
    sid = client.create_sandbox()["id"]
    out = run(client, sid, session.command_op("sleep 5", timeout=1))
    assert out.timed_out
    assert out.exit_code in (124, 137)


def test_upload_download_roundtrip_in_chunks(cloud: FakeCloud, client: k.Kosa8Client, tmp_path: Path) -> None:
    sess = k.SandboxSession(workspace=str(tmp_path), persist_dirs=(), upload_chunk_bytes=1000, download_chunk_bytes=4096)
    sid = client.create_sandbox()["id"]
    target = tmp_path / "a" / "b" / "data.bin"
    data = os.urandom(10_000)
    assert run(client, sid, sess.upload_op(str(target), data)) is None
    assert target.read_bytes() == data
    before = cloud.exec_count
    got = run(client, sid, sess.download_op(str(target)))
    assert got.error is None
    assert got.content == data
    assert cloud.exec_count - before == 3  # 10,000 bytes in 4,096-byte chunks


def test_upload_empty_file_and_overwrite(client: k.Kosa8Client, session: k.SandboxSession, tmp_path: Path) -> None:
    sid = client.create_sandbox()["id"]
    target = tmp_path / "workspace" / "f.txt"
    assert run(client, sid, session.upload_op(str(target), b"first version")) is None
    assert run(client, sid, session.upload_op(str(target), b"")) is None
    assert target.read_bytes() == b""


def test_file_errors(client: k.Kosa8Client, session: k.SandboxSession, tmp_path: Path) -> None:
    sid = client.create_sandbox()["id"]
    ws = tmp_path / "workspace"
    assert run(client, sid, session.download_op("relative.txt")).error == "invalid_path"
    assert run(client, sid, session.upload_op("relative.txt", b"x")) == "invalid_path"
    assert run(client, sid, session.download_op(str(ws / "missing"))).error == "file_not_found"
    assert run(client, sid, session.download_op(str(ws))).error == "is_directory"
    assert run(client, sid, session.upload_op(str(ws), b"x")) == "is_directory"
    (ws / "plain").write_text("x")
    assert run(client, sid, session.upload_op(str(ws / "plain" / "child"), b"x")) == "invalid_path"
    if os.geteuid() != 0:
        locked = ws / "locked"
        locked.write_text("secret")
        locked.chmod(0)
        try:
            assert run(client, sid, session.download_op(str(locked))).error == "permission_denied"
        finally:
            locked.chmod(0o644)


def test_persist_dirs_link_onto_the_workspace(client: k.Kosa8Client, tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    fake_tmp = tmp_path / "scratch"
    fake_tmp.mkdir()
    (fake_tmp / "from-image").write_text("baked in")
    sess = k.SandboxSession(workspace=str(ws), persist_dirs=(str(fake_tmp),))
    sid = client.create_sandbox()["id"]
    out = run(client, sid, sess.command_op(f"echo kept > {fake_tmp}/note; cat {fake_tmp}/from-image"))
    assert out.stdout == "baked in"
    target = ws / ".kosa8-persist" / str(fake_tmp).strip("/").replace("/", "__")
    assert (target / "note").read_text() == "kept\n"
    assert fake_tmp.is_symlink()


def test_persist_dirs_must_not_overlap_workspace() -> None:
    with pytest.raises(ValueError):
        k.SandboxSession(workspace="/workspace", persist_dirs=("/workspace/x",))
    with pytest.raises(ValueError):
        k.SandboxSession(workspace="/workspace", persist_dirs=("relative",))
    for unsafe in ("/", "/usr", "/etc/", "/tmp/../etc"):
        with pytest.raises(ValueError):
            k.SandboxSession(workspace="/workspace", persist_dirs=(unsafe,))


def test_truncation_marker_is_reported() -> None:
    out = k._buffered_output({"exit": 0, "stdout": "a\n[k8agent: 12 bytes of output dropped]\nb", "stderr": ""})
    assert out.truncated


def test_host_timeout_maps_to_timeout_error(cloud: FakeCloud, client: k.Kosa8Client, session: k.SandboxSession) -> None:
    sid = client.create_sandbox()["id"]
    cloud.fail_exec_with = (502, {"error": "host_failed", "message": "exec failed on the host: The operation was aborted due to timeout"})
    with pytest.raises(k.Kosa8TimeoutError):
        run(client, sid, session.command_op("sleep 100"))


def test_async_client_roundtrip(tmp_path: Path) -> None:
    cloud = FakeCloud()
    sess = k.SandboxSession(workspace=str(tmp_path), persist_dirs=())

    async def main() -> None:
        async with k.AsyncKosa8Client(api_key=cloud.api_key, http_client=cloud.async_client()) as c:
            sid = (await c.create_sandbox(template="python"))["id"]
            assert await k.adrive(sess.upload_op(str(tmp_path / "x.txt"), b"async data"), lambda r: c.exec(sid, r)) is None
            got = await k.adrive(sess.download_op(str(tmp_path / "x.txt")), lambda r: c.exec(sid, r))
            assert got.content == b"async data"
            out = await k.adrive(sess.command_op("echo ok"), lambda r: c.exec(sid, r))
            assert out.stdout == "ok\n"
            await c.delete_sandbox(sid)

    asyncio.run(main())


def test_transport_errors_are_wrapped() -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    c = k.Kosa8Client(api_key="x", http_client=httpx.Client(transport=httpx.MockTransport(boom)))
    with pytest.raises(k.Kosa8Error, match="could not reach"):
        c.list_templates()


def test_download_max_bytes(client: k.Kosa8Client, session: k.SandboxSession, tmp_path: Path) -> None:
    sid = client.create_sandbox()["id"]
    f = tmp_path / "workspace" / "big"
    f.write_bytes(b"x" * 2000)
    assert run(client, sid, session.download_op(str(f), max_bytes=1000)).error == "too_large"
    assert run(client, sid, session.download_op(str(f), max_bytes=2000)).content == b"x" * 2000


@pytest.mark.skipif(not os.environ.get("KOSA8_TEST_CONTAINER"), reason="set KOSA8_TEST_CONTAINER to a running Linux container")
def test_in_a_linux_container_with_default_settings() -> None:
    """Default session (/workspace, /tmp kept) in a real Linux userland, e.g. python:3.12-alpine."""
    run_in = container_runner(os.environ["KOSA8_TEST_CONTAINER"], os.environ.get("KOSA8_TEST_ENGINE", "kosa8"))
    run_in(["/bin/sh", "-c", "mkdir -p /workspace"], {}, 60)
    cloud = FakeCloud(runner=run_in)
    client = k.Kosa8Client(api_key=cloud.api_key, http_client=cloud.client())
    sess = k.SandboxSession()
    sid = client.create_sandbox()["id"]
    out = run(client, sid, sess.command_op("sleep 5", timeout=1))
    assert out.timed_out and out.exit_code in (124, 137)
    data = os.urandom(120_000)
    assert run(client, sid, sess.upload_op("/tmp/k8-check/data.bin", data)) is None
    assert run(client, sid, sess.download_op("/tmp/k8-check/data.bin")).content == data
    kept = run(client, sid, sess.command_op("ls /workspace/.kosa8-persist/tmp/k8-check"))
    assert kept.stdout.strip() == "data.bin"
    out = run(client, sid, sess.command_op("pwd; cat", stdin=b"in"))
    assert out.stdout == "/workspace\nin"

"""Unit tests for Kosa8Sandbox against the fake kosa8 Cloud API (no network)."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest
from deepagents.backends.protocol import FileDownloadResponse, FileUploadResponse, SandboxBackendProtocol

import langchain_kosa8
from langchain_kosa8 import AsyncKosa8Client, Kosa8Client, Kosa8Sandbox
from tests.fake_cloud import FakeCloud


@pytest.fixture
def cloud() -> FakeCloud:
    return FakeCloud()


@pytest.fixture
def backend(cloud: FakeCloud, tmp_path: Path) -> Kosa8Sandbox:
    client = Kosa8Client(api_key=cloud.api_key, http_client=cloud.client())
    aclient = AsyncKosa8Client(api_key=cloud.api_key, http_client=cloud.async_client())
    sb = Kosa8Sandbox.create(client=client, workspace=str(tmp_path), persist_dirs=())
    sb._async_client = aclient  # noqa: SLF001
    return sb


def test_exports() -> None:
    assert langchain_kosa8.Kosa8Sandbox is Kosa8Sandbox
    assert langchain_kosa8.__version__


def test_is_a_sandbox_backend(backend: Kosa8Sandbox) -> None:
    assert isinstance(backend, SandboxBackendProtocol)
    assert backend.id.startswith("sbx_")


def test_create_sends_template_and_lifetime(cloud: FakeCloud) -> None:
    client = Kosa8Client(api_key=cloud.api_key, http_client=cloud.client())
    sb = Kosa8Sandbox.create(client=client, template="python", region="eu-hel1", ttl_s=3600, sleep_after_s=600)
    assert cloud.requests[0] == ("POST", "/v1/sandboxes", {"template": "python", "region": "eu-hel1", "sleep_after_s": 600, "ttl_s": 3600})
    assert sb.id in cloud.sandboxes


def test_default_template_is_python(cloud: FakeCloud) -> None:
    client = Kosa8Client(api_key=cloud.api_key, http_client=cloud.client())
    Kosa8Sandbox.create(client=client)
    assert cloud.requests[0][2] == {"template": "python"}


def test_execute_stdout_stderr_exit(backend: Kosa8Sandbox) -> None:
    r = backend.execute("echo hello; echo oops >&2; exit 2")
    assert r.exit_code == 2
    assert r.output == "hello\n\n<stderr>oops</stderr>"
    assert r.truncated is False


def test_execute_starts_in_workspace(backend: Kosa8Sandbox, tmp_path: Path) -> None:
    assert Path(backend.execute("pwd").output.strip()).resolve() == tmp_path.resolve()


def test_execute_sends_timeout_into_the_sandbox(cloud: FakeCloud, backend: Kosa8Sandbox) -> None:
    backend.execute("true", timeout=7)
    env = cloud.requests[-1][2]["env"]
    assert "K8_TIMEOUT=7" in env
    backend.execute("true", timeout=0)
    assert not any(e.startswith("K8_TIMEOUT=") for e in cloud.requests[-1][2]["env"])


def test_execute_timeout_returns_124(backend: Kosa8Sandbox) -> None:
    if not (Path("/usr/bin/timeout").exists() or Path("/bin/timeout").exists()):
        pytest.skip("no timeout binary on this machine")
    r = backend.execute("echo started; sleep 5", timeout=1)
    assert r.exit_code == 124
    assert r.output.startswith("Command timed out after 1 seconds")
    assert "started" in r.output


def test_host_request_limit_returns_124(cloud: FakeCloud, backend: Kosa8Sandbox) -> None:
    cloud.fail_exec_with = (502, {"error": "host_failed", "message": "exec failed on the host: The operation was aborted due to timeout"})
    r = backend.execute("sleep 100")
    assert r.exit_code == 124
    assert "30 s" in r.output


def test_upload_download_and_errors(backend: Kosa8Sandbox, tmp_path: Path) -> None:
    data = os.urandom(100_000)
    f = str(tmp_path / "dir" / "blob.bin")
    assert backend.upload_files([(f, data), ("rel.txt", b"x")]) == [
        FileUploadResponse(path=f, error=None),
        FileUploadResponse(path="rel.txt", error="invalid_path"),
    ]
    missing = str(tmp_path / "missing")
    assert backend.download_files([f, missing, "rel.txt"]) == [
        FileDownloadResponse(path=f, content=data, error=None),
        FileDownloadResponse(path=missing, content=None, error="file_not_found"),
        FileDownloadResponse(path="rel.txt", content=None, error="invalid_path"),
    ]


def test_file_tools_from_base_sandbox(backend: Kosa8Sandbox, tmp_path: Path) -> None:
    p = str(tmp_path / "notes" / "a.txt")
    assert backend.write(p, "alpha\nbeta\n").error is None
    assert backend.edit(p, "beta", "gamma").error is None
    assert "gamma" in str(backend.read(p))
    assert backend.grep("gamma", path=str(tmp_path)).error is None


def test_async_variants(backend: Kosa8Sandbox, tmp_path: Path) -> None:
    async def main() -> None:
        r = await backend.aexecute("echo async")
        assert r.output == "async\n"
        p = str(tmp_path / "async.txt")
        assert await backend.aupload_files([(p, b"payload")]) == [FileUploadResponse(path=p, error=None)]
        assert await backend.adownload_files([p]) == [FileDownloadResponse(path=p, content=b"payload", error=None)]

    asyncio.run(main())


def test_api_errors_are_reported_per_file(cloud: FakeCloud, backend: Kosa8Sandbox, tmp_path: Path) -> None:
    cloud.fail_exec_with = (409, {"error": "not_running", "message": "the sandbox is stopped, not running"})
    p = str(tmp_path / "x")
    [up] = backend.upload_files([(p, b"x")])
    assert up.error and "not running" in up.error
    [down] = backend.download_files([p])
    assert down.content is None and down.error and "not running" in down.error


def test_sleep_wake_delete(cloud: FakeCloud, backend: Kosa8Sandbox) -> None:
    backend.sleep()
    assert cloud.sandboxes[backend.id]["state"] == "sleeping"
    backend.execute("true")  # a command wakes it
    assert cloud.sandboxes[backend.id]["state"] == "running"
    backend.sleep()
    backend.wake()
    assert cloud.sandboxes[backend.id]["state"] == "running"
    backend.delete()
    assert cloud.sandboxes[backend.id]["state"] == "deleted"

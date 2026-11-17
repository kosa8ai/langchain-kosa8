"""A stand-in for the kosa8 Cloud API, for tests.

It answers the sandbox endpoints the integrations use through an
``httpx.MockTransport``. Exec requests really run, as local subprocesses, so
the integration's shell wrappers are exercised end to end; the "sandbox" is a
temporary directory used as the workspace. It also enforces the API limits
that matter to the transport: a 64 KiB JSON body, 8192 characters per
argument, 128 environment entries, a buffered ``timeout_s`` of at most 90 s,
and the files API's 8 MiB per file under ``files_root``.

Nothing here talks to the network.
"""

from __future__ import annotations

import base64
import contextlib
import json
import os
import signal
import subprocess
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import httpx

# runner(argv, env, timeout) -> (exit code, stdout bytes, stderr bytes)
Runner = Callable[[list[str], dict[str, str], float], "tuple[int, bytes, bytes]"]


def local_runner(argv: list[str], env: dict[str, str], timeout: float) -> tuple[int, bytes, bytes]:
    """Run argv as a local subprocess (the default)."""
    base = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": os.environ.get("HOME", "/")}
    # Like the host at a timeout_s, end the command and everything it started.
    with subprocess.Popen(
        argv, env={**base, **env}, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True
    ) as p:
        try:
            out, err = p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as e:
            os.killpg(p.pid, signal.SIGKILL)
            out, err = p.communicate()
            raise subprocess.TimeoutExpired(argv, timeout, output=out, stderr=err) from e
    return (p.returncode if p.returncode >= 0 else 128 - p.returncode), out, err


def container_runner(container: str, engine: str = "kosa8") -> Runner:
    """Run argv in a running local container (``kosa8 exec`` or ``docker exec``)."""

    def run(argv: list[str], env: dict[str, str], timeout: float) -> tuple[int, bytes, bytes]:
        envargs = [f"{k}={v}" for k, v in env.items()]
        p = subprocess.run([engine, "exec", container, "/usr/bin/env", *envargs, *argv], capture_output=True, timeout=timeout, check=False)
        return (p.returncode if p.returncode >= 0 else 128 - p.returncode), p.stdout, p.stderr

    return run


BODY_LIMIT = 64 * 1024


@dataclass
class FakeCloud:
    """State of the fake API. ``stream`` controls whether streamed exec is offered."""

    stream: bool = False
    api_key: str = "k8c_live_test"
    sandboxes: dict[str, dict[str, Any]] = field(default_factory=dict)
    requests: list[tuple[str, str, Any]] = field(default_factory=list)
    exec_count: int = 0
    fail_exec_with: tuple[int, dict[str, Any]] | None = None
    host_timeout_s: float = 60.0
    runner: Runner | None = None
    #: Whether the files API is offered (False answers 501), and the folder it reaches.
    files_api: bool = True
    files_root: str = "/workspace"
    #: The real files API acts with host privileges, so it ignores the
    #: sandbox user's permissions: the fake does the same.
    files_api_ignores_permissions: bool = True
    #: Whether a streamed exec takes timeout_s (False answers 501, as older hosts do).
    stream_timeout: bool = True
    #: Polls of GET before a create that answered ``creating`` runs.
    boot_polls: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _n: int = 0

    # httpx plumbing --------------------------------------------------------

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def async_transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def client(self) -> httpx.Client:
        return httpx.Client(transport=self.transport())

    def async_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self.async_transport())

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.headers.get("authorization") != f"Bearer {self.api_key}":
            return _err(401, "unauthorized", "bad key")
        raw = request.read()
        path = request.url.path
        method = request.method
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 4 and parts[3] == "files" and method == "PUT":
            with self._lock:
                self.requests.append((method, path, {"path": request.url.params.get("path"), "bytes": len(raw)}))
            return self._files(parts, method, request, raw)
        if len(raw) > BODY_LIMIT:
            return _err(413, "too_large", f"body over {BODY_LIMIT} bytes")
        body = json.loads(raw) if raw else None
        with self._lock:
            self.requests.append((method, path, body if body is not None else dict(request.url.params) or None))
        if parts[:2] == ["v1", "templates"] and method == "GET":
            return _json(200, {"templates": [{"key": "python", "cpus": 1, "mem_mb": 256}], "default_template": "base"})
        if parts[:2] != ["v1", "sandboxes"]:
            return _err(404, "not_found", "no such route")
        if len(parts) == 2 and method == "POST":
            return self._create(body or {})
        if len(parts) < 3:
            return _err(404, "not_found", "no such route")
        sid = parts[2]
        box = self.sandboxes.get(sid)
        if box is None or box["state"] == "deleted":
            return _err(404, "not_found", "no such sandbox")
        if len(parts) == 3:
            if method == "GET":
                if box["state"] == "creating":
                    box["_polls"] = box.get("_polls", 0) + 1
                    if box["_polls"] >= self.boot_polls:
                        box["state"] = "running"
                return _json(200, {"sandbox": box})
            if method == "DELETE":
                box["state"] = "deleted"
                return _json(200, {"ok": True, "id": sid})
        action = parts[3] if len(parts) > 3 else ""
        if action == "sleep" and method == "POST":
            box["state"] = "sleeping"
            return _json(200, {"sandbox": box})
        if action == "wake" and method == "POST":
            box["state"] = "running"
            return _json(200, {"sandbox": box, "wake_ms": 5})
        if action == "exec" and method == "POST":
            return self._exec(box, body or {})
        if action == "files":
            return self._files(parts, method, request, body)
        return _err(404, "not_found", "no such route")

    def _files(self, parts: list[str], method: str, request: httpx.Request, body: Any) -> httpx.Response:
        box = self.sandboxes.get(parts[2])
        if box is None or box["state"] == "deleted":
            return _err(404, "not_found", "no such sandbox")
        if not self.files_api:
            return _err(501, "not_available", "Files is not available on this host yet")
        if parts[4:] == ["mkdir"] and method == "POST":
            p = (body or {}).get("path", "")
        else:
            p = request.url.params.get("path", "")
        root = self.files_root.rstrip("/")
        if not p.startswith(root + "/") or "/../" in p + "/":
            return _err(400, "invalid_path", "path must be a workspace path such as /workspace/app.py")
        try:
            if parts[4:] == ["mkdir"] and method == "POST":
                os.makedirs(p, exist_ok=True)
                return _json(200, {"file": {"path": p, "dir": True}})
            if len(parts) == 4 and method == "PUT":
                if len(body) > 8 * 1024 * 1024:
                    return _err(413, "too_large", "one file is at most 8388608 bytes through the API")
                with self._privileged(p, os.W_OK):
                    with open(p, "wb") as f:
                        f.write(body)
                return _json(200, {"file": {"path": p, "size": len(body)}})
            if len(parts) == 4 and method == "GET":
                with self._privileged(p, os.R_OK):
                    with open(p, "rb") as f:
                        data = f.read()
                if len(data) > 8 * 1024 * 1024:
                    return _err(413, "too_large", "one file is at most 8388608 bytes through the API")
                return httpx.Response(200, content=data, headers={"content-type": "application/octet-stream"})
        except FileNotFoundError:
            return _err(404, "not_found", f"could not read {p}: no such file")
        except IsADirectoryError:
            return _err(400, "bad_request", f"could not use {p}: is a directory")
        except NotADirectoryError:
            return _err(400, "bad_request", f"could not use {p}: not a directory")
        except PermissionError:
            return _err(502, "host_failed", f"could not use {p}: permission denied")
        return _err(404, "not_found", "no such route")

    @contextlib.contextmanager
    def _privileged(self, path: str, want: int) -> Iterator[None]:
        """Lift the file's (and its folder's) permission bits for one call, as
        a host with privileges would not need to, then put them back."""
        saved: list[tuple[str, int]] = []
        if self.files_api_ignores_permissions:
            for target, need in ((os.path.dirname(path), os.W_OK | os.X_OK if want == os.W_OK else os.X_OK), (path, want)):
                if os.path.exists(target) and not os.access(target, need):
                    mode = os.stat(target).st_mode & 0o7777
                    saved.append((target, mode))
                    os.chmod(target, mode | (0o700 if os.path.isdir(target) else 0o600))
        try:
            yield
        finally:
            for target, mode in reversed(saved):
                os.chmod(target, mode)

    # Endpoints ---------------------------------------------------------------

    def _create(self, body: dict[str, Any]) -> httpx.Response:
        with self._lock:
            self._n += 1
            sid = f"sbx_test{self._n}"
        box = {
            "id": sid,
            "state": "creating" if self.boot_polls else "running",
            "template": body.get("template", "base"),
            "region": body.get("region"),
            "sleep_after_s": body.get("sleep_after_s"),
            "ttl_s": body.get("ttl_s"),
        }
        self.sandboxes[sid] = box
        return _json(201, {"sandbox": box})

    def _exec(self, box: dict[str, Any], body: dict[str, Any]) -> httpx.Response:
        cmd = body.get("cmd")
        if not isinstance(cmd, list) or not cmd or len(cmd) > 256 or not all(isinstance(a, str) and len(a) <= 8192 for a in cmd):
            return _err(400, "invalid_cmd", "cmd must be a non-empty list of strings")
        envl = body.get("env", [])
        if len(envl) > 128:
            return _err(400, "invalid_env", "too many env entries")
        if body.get("stream") and not self.stream:
            return _err(501, "not_available", "Streamed exec is not available on this host yet")
        timeout_s = body.get("timeout_s")
        if timeout_s is not None:
            if not isinstance(timeout_s, int) or timeout_s < 1:
                return _err(400, "invalid_timeout", "timeout_s must be a whole number of seconds")
            if not body.get("stream") and timeout_s > 90:
                return _err(400, "invalid_timeout", "a buffered exec is held to 90 s, so timeout_s is at most 90 there")
            if body.get("stream") and not self.stream_timeout:
                return _err(501, "not_available", "timeout_s on a streamed exec is not available on this sandbox's host yet")
        if self.fail_exec_with is not None:
            status, payload = self.fail_exec_with
            return _json(status, payload)
        if box["state"] == "sleeping":
            box["state"] = "running"
        self.exec_count += 1
        env: dict[str, str] = {}
        for e in envl:
            k, _, v = e.partition("=")
            env[k] = v
        limit = self.host_timeout_s if timeout_s is None else min(self.host_timeout_s, timeout_s)
        try:
            exit_code, stdout, stderr = (self.runner or local_runner)(cmd, env, limit)
        except subprocess.TimeoutExpired as e:
            out = (e.stdout or b"").decode("utf-8", "replace")
            err = (e.stderr or b"").decode("utf-8", "replace")
            if body.get("stream"):
                frames = [{"stream": "stdout", "data": base64.b64encode(e.stdout or b"").decode()}]
                frames.append({"done": True, "error": "the command ran longer than its timeout_s", "code": "exec_timeout"})
                text = "".join(json.dumps(f) + "\n" for f in frames)
                return httpx.Response(200, content=text.encode(), headers={"content-type": "application/x-ndjson"})
            return _json(
                504,
                {
                    "error": "exec_timeout",
                    "message": f"the command ran longer than {limit:.0f} s and was stopped",
                    "limit_s": limit,
                    "stdout": out,
                    "stderr": err,
                },
            )
        if body.get("stream"):
            frames = []
            if stdout:
                frames.append({"stream": "stdout", "data": base64.b64encode(stdout).decode()})
            if stderr:
                frames.append({"stream": "stderr", "data": base64.b64encode(stderr).decode()})
            frames.append({"done": True, "exit": exit_code})
            text = "".join(json.dumps(f) + "\n" for f in frames)
            return httpx.Response(200, content=text.encode(), headers={"content-type": "application/x-ndjson"})
        return _json(
            200,
            {
                "exit": exit_code,
                "stdout": stdout.decode("utf-8", "replace"),
                "stderr": stderr.decode("utf-8", "replace"),
            },
        )


def _json(status: int, body: Any) -> httpx.Response:
    return httpx.Response(status, json=body)


def _err(status: int, code: str, message: str) -> httpx.Response:
    return httpx.Response(status, json={"error": code, "message": message})

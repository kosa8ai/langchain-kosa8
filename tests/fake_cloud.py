"""A stand-in for the kosa8 Cloud API, for tests.

It answers the sandbox endpoints the integrations use through an
``httpx.MockTransport``. Exec requests really run, as local subprocesses, so
the integration's shell wrappers are exercised end to end; the "sandbox" is a
temporary directory used as the workspace. It also enforces the API limits
that matter to the transport: a 64 KiB JSON body, 8192 characters per
argument, 128 environment entries.

Nothing here talks to the network.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import httpx

# runner(argv, env, timeout) -> (exit code, stdout bytes, stderr bytes)
Runner = Callable[[list[str], dict[str, str], float], "tuple[int, bytes, bytes]"]


def local_runner(argv: list[str], env: dict[str, str], timeout: float) -> tuple[int, bytes, bytes]:
    """Run argv as a local subprocess (the default)."""
    base = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": os.environ.get("HOME", "/")}
    p = subprocess.run(argv, env={**base, **env}, capture_output=True, timeout=timeout, check=False)
    return (p.returncode if p.returncode >= 0 else 128 - p.returncode), p.stdout, p.stderr


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
    runner: Optional[Runner] = None
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
        if len(raw) > BODY_LIMIT:
            return _err(413, "too_large", f"body over {BODY_LIMIT} bytes")
        body = json.loads(raw) if raw else None
        path = request.url.path
        method = request.method
        with self._lock:
            self.requests.append((method, path, body))
        parts = [p for p in path.split("/") if p]
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
        return _err(404, "not_found", "no such route")

    # Endpoints ---------------------------------------------------------------

    def _create(self, body: dict[str, Any]) -> httpx.Response:
        with self._lock:
            self._n += 1
            sid = f"sbx_test{self._n}"
        box = {
            "id": sid,
            "state": "running",
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
        try:
            exit_code, stdout, stderr = (self.runner or local_runner)(cmd, env, self.host_timeout_s)
        except subprocess.TimeoutExpired:
            return _err(502, "host_failed", "exec failed on the host: context deadline exceeded (timed out)")
        if body.get("stream"):
            frames = []
            if stdout:
                frames.append({"stream": "stdout", "data": base64.b64encode(stdout).decode()})
            if stderr:
                frames.append({"stream": "stderr", "data": base64.b64encode(stderr).decode()})
            frames.append({"done": True, "exit": exit_code})
            text = "".join(json.dumps(f) + "\n" for f in frames)
            return httpx.Response(200, content=text.encode(), headers={"content-type": "application/x-ndjson"})
        return _json(200, {
            "exit": exit_code,
            "stdout": stdout.decode("utf-8", "replace"),
            "stderr": stderr.decode("utf-8", "replace"),
        })


def _json(status: int, body: Any) -> httpx.Response:
    return httpx.Response(status, json=body)


def _err(status: int, code: str, message: str) -> httpx.Response:
    return httpx.Response(status, json={"error": code, "message": message})

"""kosa8 Cloud transport shared by the kosa8 framework integrations.

This module is vendored, unchanged, into ``langchain-kosa8``, ``inspect-kosa8``
and ``harbor-kosa8``. Keep the copies in sync.

It talks to the kosa8 Cloud API (``https://api.kosa8.com``) with an API key
and needs nothing but ``httpx``. What it relies on:

* ``POST /v1/sandboxes`` to create (polling ``GET /v1/sandboxes/{id}`` while a
  sized create answers ``creating``), ``GET`` / ``DELETE /v1/sandboxes/{id}``,
  ``POST /v1/sandboxes/{id}/sleep`` and ``/wake``, ``GET /v1/templates``.
* ``POST /v1/sandboxes/{id}/exec`` with ``{"cmd": [...], "env": [...],
  "timeout_s": n, "stream": bool}``.
* ``GET`` / ``PUT /v1/sandboxes/{id}/files?path=`` and ``POST .../files/mkdir``
  for files under ``/workspace`` (at most 8 MiB each).

How commands run on kosa8 Cloud, and what this module does about it:

* Commands run in the sandbox's own container, whose filesystem lasts from
  one command to the next. Hosts that start each command in a fresh
  container keep only ``/workspace``; for those, each command first links (or
  bind-mounts) the ``persist_dirs`` (``/tmp`` by default) onto
  ``/workspace/.kosa8-persist/``. On a long-lived container that is a no-op
  after the first command.
* A buffered exec is held to 90 s and takes ``timeout_s`` up to that; past it
  the answer is 504 ``exec_timeout``. A streamed exec (NDJSON frames) has no
  such limit and ends with a frame ``{done, error, code: exec_timeout}`` when
  its ``timeout_s`` runs out. Streamed exec is used when the host offers it.
* File bytes travel through the files API (``/workspace`` only, at most
  8 MiB per request), but the files API acts with host privileges and
  ignores the sandbox user's permissions. So a write is staged in the
  workspace's ``.kosa8-run`` and moved into place by one exec as the
  sandbox user (``cat parts > target``: permissions are checked, an
  existing file keeps its mode and owner, symlinks are written through);
  a read is checked by one exec as the sandbox user, which also splits a
  file over 8 MiB (or outside ``/workspace``) into staged parts, before the
  bytes are fetched. A command or stdin too large for one exec request is
  staged in ``.kosa8-run`` directly. Without the files API, and on errors
  that exec reports more precisely, the bytes go through exec instead,
  base64-encoded, in chunks sized to the API's request (64 KiB) and output
  (1 MiB) limits.
* There is no stdin, user or working-directory field in the exec API; this
  module provides stdin, working directory and timeouts with a small POSIX
  shell wrapper. Commands run as the image's default user. Each argv entry
  is at most 8 KiB, so data travels in environment variables instead.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import posixpath
import re
import shlex
import time
import uuid
from collections.abc import Callable, Generator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar
from urllib.parse import quote

import httpx

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_WORKSPACE",
    "AsyncKosa8Client",
    "DownloadResult",
    "ExecOutput",
    "ExecRequest",
    "Kosa8Client",
    "Kosa8Error",
    "Kosa8TimeoutError",
    "SandboxSession",
    "UploadRequest",
    "adownload",
    "aupload",
    "download",
    "upload",
]

DEFAULT_BASE_URL = "https://api.kosa8.com"
DEFAULT_WORKSPACE = "/workspace"
DEFAULT_PERSIST_DIRS: tuple[str, ...] = ("/tmp",)

# The API reads at most 64 KiB of JSON per exec request. Data chunks leave
# room for the wrapper script, the path and the JSON around them.
REQUEST_BODY_LIMIT = 60_000
UPLOAD_CHUNK_BYTES = 36_000  # 48,000 base64 characters per request
# Buffered exec keeps the first 1 MiB of each stream (and the last 1 MiB).
# 512 KiB of file is about 700 KB of base64.
DOWNLOAD_CHUNK_BYTES = 512 * 1024
MAX_ENV_ENTRIES = 128
MAX_ARG_CHARS = 8192
# A buffered exec is held to 90 s on the host; wait a little longer for its answer.
BUFFERED_EXEC_LIMIT_S = 90
BUFFERED_REQUEST_TIMEOUT_S = BUFFERED_EXEC_LIMIT_S + 30.0
# Seconds the API's own timeout_s waits past the in-sandbox timeout, as a backstop.
SERVER_TIMEOUT_GRACE_S = 1
# The files API reaches /workspace only, one file of at most 8 MiB.
FILES_API_ROOT = "/workspace"
MAX_FILES_API_BYTES = 8 * 1024 * 1024
# How long, and how often, a sandbox booting cold is polled until it runs.
CREATE_WAIT_S = 300.0
CREATE_POLL_S = 1.0

_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DROPPED_RE = re.compile(r"\n\[k8agent: \d+ bytes of output dropped\]\n")
_HOST_TIMEOUT_RE = re.compile(r"time(d)? ?out|deadline|context canceled|aborted", re.I)


class Kosa8Error(Exception):
    """An error answered by kosa8 Cloud, or a failure to reach it."""

    def __init__(self, message: str, *, status: int | None = None, code: str | None = None, body: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.body = body


class Kosa8TimeoutError(Kosa8Error, TimeoutError):
    """A command or request did not finish in time."""


@dataclass
class ExecOutput:
    """What one exec request answered."""

    exit_code: int | None
    stdout: str
    stderr: str
    truncated: bool = False
    timed_out: bool = False


@dataclass
class ExecRequest:
    """One exec request: argv plus internal environment variables."""

    argv: list[str]
    env: dict[str, str] = field(default_factory=dict)
    timeout: float | None = None
    image: str | None = None


@dataclass
class UploadRequest:
    """An operation's request to write ``data`` to ``path``.

    The driver answers with None or an error code when it wrote the file
    (through the files API), or with ``FALLBACK`` to have the operation
    write it through exec instead."""

    path: str
    data: bytes
    #: True for the transport's own staging files (written as they are);
    #: False for a caller's file, which is moved into place as the sandbox user.
    direct: bool = False


#: A driver's answer to an UploadRequest it did not carry.
FALLBACK: Any = object()


@dataclass
class DownloadResult:
    content: bytes | None
    error: str | None = None


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _check_env(env: Mapping[str, str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (env or {}).items():
        if not isinstance(k, str) or not _ENV_NAME_RE.match(k):
            raise ValueError(f"invalid environment variable name: {k!r}")
        if k.startswith("K8_"):
            raise ValueError(f"environment variable names starting with K8_ are reserved: {k!r}")
        out[k] = str(v)
    return out


def _resolve_api_key(api_key: str | None) -> str:
    key = (api_key if api_key is not None else os.environ.get("KOSA8_API_KEY", "")).strip()
    if not key:
        raise Kosa8Error("no kosa8 Cloud API key: pass api_key or set KOSA8_API_KEY")
    return key


def _resolve_base_url(base_url: str | None) -> str:
    url = (base_url or os.environ.get("KOSA8_CLOUD_URL") or DEFAULT_BASE_URL).rstrip("/")
    parsed = httpx.URL(url)
    local = parsed.host in ("localhost", "127.0.0.1", "::1")
    if parsed.scheme != "https" and not (parsed.scheme == "http" and local):
        # The API key travels in every request.
        raise Kosa8Error(f"kosa8 Cloud address must use https: {url}")
    return url


def _error_from_response(resp: httpx.Response) -> Kosa8Error:
    code = None
    message = f"kosa8 Cloud answered HTTP {resp.status_code}"
    try:
        body = resp.json()
    except ValueError:
        body = None
    if isinstance(body, dict):
        if isinstance(body.get("error"), str):
            code = body["error"]
        if isinstance(body.get("message"), str):
            message = body["message"]
        elif code:
            message = code
    elif resp.text:
        message = f"{message}: {resp.text[:300]}"
    cls = Kosa8Error
    if code == "exec_timeout" or (resp.status_code in (502, 504) and _HOST_TIMEOUT_RE.search(message)):
        cls = Kosa8TimeoutError
    return cls(message, status=resp.status_code, code=code, body=body)


def _exec_body(
    argv: Sequence[str],
    env: Mapping[str, str],
    stream: bool,
    image: str | None = None,
    timeout_s: int | None = None,
) -> dict[str, Any]:
    if not argv:
        raise ValueError("argv must not be empty")
    for a in argv:
        if len(a) > MAX_ARG_CHARS:
            raise ValueError(f"an exec argument is longer than {MAX_ARG_CHARS} characters")
    if len(env) > MAX_ENV_ENTRIES:
        raise ValueError(f"at most {MAX_ENV_ENTRIES} environment variables per command")
    body: dict[str, Any] = {"cmd": list(argv)}
    if image:
        body["image"] = image
    if env:
        body["env"] = [f"{k}={v}" for k, v in env.items()]
    if stream:
        body["stream"] = True
    if timeout_s is not None:
        body["timeout_s"] = timeout_s
    return body


def _server_timeout(request: ExecRequest, stream: bool) -> int | None:
    """The API's ``timeout_s`` for a request: a backstop a little past the
    in-sandbox timeout, or None (a buffered exec is then held to 90 s)."""
    if request.timeout is None or request.timeout <= 0:
        return None
    secs = max(1, int(-(-request.timeout // 1))) + SERVER_TIMEOUT_GRACE_S
    if not stream and secs > BUFFERED_EXEC_LIMIT_S:
        return BUFFERED_EXEC_LIMIT_S
    return secs


def _timed_out_output(err: Kosa8Error) -> ExecOutput:
    """The output so far of a command the API stopped at its timeout_s."""
    body = err.body if isinstance(err.body, dict) else {}
    return ExecOutput(
        exit_code=None,
        stdout=str(body.get("stdout") or ""),
        stderr=str(body.get("stderr") or ""),
        timed_out=True,
    )


def _buffered_output(data: Any) -> ExecOutput:
    if not isinstance(data, dict):
        raise Kosa8Error("kosa8 Cloud answered an exec with an unexpected body")
    stdout = data.get("stdout") or ""
    stderr = data.get("stderr") or ""
    exit_code = data.get("exit")
    truncated = bool(_DROPPED_RE.search(stdout) or _DROPPED_RE.search(stderr))
    return ExecOutput(
        exit_code=exit_code if isinstance(exit_code, int) else None,
        stdout=stdout,
        stderr=stderr,
        truncated=truncated,
    )


class _StreamCollector:
    """Collects NDJSON exec frames: {stream, data: base64}, then {done, exit|error}."""

    def __init__(self) -> None:
        self.out = bytearray()
        self.err = bytearray()
        self.exit: int | None = None
        self.error: str | None = None
        self.code: str | None = None
        self.done = False

    def feed(self, line: str) -> None:
        line = line.strip()
        if not line:
            return
        try:
            frame = json.loads(line)
        except ValueError:
            return
        if not isinstance(frame, dict):
            return
        if frame.get("done"):
            self.done = True
            if isinstance(frame.get("exit"), int):
                self.exit = frame["exit"]
            if frame.get("error"):
                self.error = str(frame["error"])
            if isinstance(frame.get("code"), str):
                self.code = frame["code"]
            return
        data = frame.get("data")
        if not isinstance(data, str):
            return
        try:
            raw = base64.b64decode(data)
        except ValueError:
            return
        if frame.get("stream") == "stderr":
            self.err.extend(raw)
        else:
            self.out.extend(raw)

    def result(self) -> ExecOutput:
        if not self.done:
            raise Kosa8Error("the exec stream ended before the command finished")
        if self.code == "exec_timeout":
            return ExecOutput(
                exit_code=None,
                stdout=self.out.decode("utf-8", "replace"),
                stderr=self.err.decode("utf-8", "replace"),
                timed_out=True,
            )
        if self.error is not None:
            if _HOST_TIMEOUT_RE.search(self.error):
                raise Kosa8TimeoutError(f"exec failed on the host: {self.error}")
            raise Kosa8Error(f"exec failed on the host: {self.error}")
        return ExecOutput(
            exit_code=self.exit,
            stdout=self.out.decode("utf-8", "replace"),
            stderr=self.err.decode("utf-8", "replace"),
        )


def _sandbox_path(sandbox_id: str, suffix: str = "") -> str:
    return f"/v1/sandboxes/{quote(sandbox_id, safe='')}{suffix}"


def _create_body(
    template: str | None,
    region: str | None,
    sleep_after_s: int | None,
    ttl_s: int | None,
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if template is not None:
        body["template"] = template
    if region is not None:
        body["region"] = region
    if sleep_after_s is not None:
        body["sleep_after_s"] = sleep_after_s
    if ttl_s is not None:
        body["ttl_s"] = ttl_s
    return body


class Kosa8Client:
    """A small synchronous client for the kosa8 Cloud sandbox API."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = 60.0,
        http_client: httpx.Client | None = None,
        stream: bool | None = None,
        files_api: bool | None = None,
        files_root: str = FILES_API_ROOT,
    ) -> None:
        """Create a client.

        Args:
            api_key: kosa8 Cloud API key. Defaults to ``KOSA8_API_KEY``.
            base_url: API address. Defaults to ``KOSA8_CLOUD_URL`` or
                ``https://api.kosa8.com``.
            timeout: Timeout in seconds for control-plane requests.
            http_client: An ``httpx.Client`` to use instead of a new one.
            stream: ``True`` to require streamed exec, ``False`` to always use
                buffered exec, ``None`` (default) to try streamed exec and fall
                back to buffered exec when the host does not offer it.
            files_api: ``False`` to move every file through exec; ``None``
                (default) to use the files API for files under ``files_root``
                until a host answers that it has none.
            files_root: The directory the files API reaches.
        """
        self._key = _resolve_api_key(api_key)
        self.base_url = _resolve_base_url(base_url)
        self.timeout = timeout
        self._http = http_client or httpx.Client(timeout=timeout)
        self._own_http = http_client is None
        self._stream = stream
        # Whether the host takes timeout_s on a streamed exec (older ones answer 501).
        self._stream_timeout = True
        self.files_api = files_api
        self.files_root = files_root.rstrip("/") or "/"

    def __repr__(self) -> str:  # never the key
        return f"Kosa8Client(base_url={self.base_url!r})"

    def close(self) -> None:
        if self._own_http:
            self._http.close()

    def __enter__(self) -> Kosa8Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _headers(self, content_type: str | None = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._key}",
            "Accept": "application/json",
            "User-Agent": "kosa8-integrations",
        }
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def _send(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        timeout: float | None = None,
        params: dict[str, str] | None = None,
        content: bytes | None = None,
    ) -> httpx.Response:
        try:
            resp = self._http.request(
                method,
                self.base_url + path,
                headers=self._headers("application/octet-stream" if content is not None else None),
                json=body if content is None else None,
                content=content,
                params=params,
                timeout=timeout if timeout is not None else self.timeout,
            )
        except httpx.TimeoutException as e:
            raise Kosa8TimeoutError(f"kosa8 Cloud did not answer {method} {path} in time") from e
        except httpx.HTTPError as e:
            raise Kosa8Error(f"could not reach kosa8 Cloud at {self.base_url}: {e}") from e
        if resp.status_code >= 400:
            raise _error_from_response(resp)
        return resp

    def _request(self, method: str, path: str, body: Any = None, timeout: float | None = None) -> Any:
        resp = self._send(method, path, body, timeout=timeout)
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError as e:
            raise Kosa8Error(f"kosa8 Cloud answered {method} {path} with a body that is not JSON") from e

    # Sandboxes ---------------------------------------------------------------

    def create_sandbox(
        self,
        *,
        template: str | None = None,
        region: str | None = None,
        sleep_after_s: int | None = None,
        ttl_s: int | None = None,
    ) -> dict[str, Any]:
        """Start a sandbox and return its record once it is running.

        A create that boots cold answers ``creating``; it is polled until it
        runs, and deleted if it does not."""
        r = self._request("POST", "/v1/sandboxes", _create_body(template, region, sleep_after_s, ttl_s), timeout=max(self.timeout, 90.0))
        box = r["sandbox"]
        deadline = time.monotonic() + CREATE_WAIT_S
        try:
            while box.get("state") == "creating":
                if time.monotonic() >= deadline:
                    raise Kosa8TimeoutError(f"kosa8 sandbox {box['id']} was still starting after {CREATE_WAIT_S:.0f} s")
                time.sleep(CREATE_POLL_S)
                box = self.get_sandbox(box["id"])
            if box.get("state") != "running":
                raise Kosa8Error(f"kosa8 sandbox {box['id']} did not start: {box.get('last_error') or box.get('state')}")
        except BaseException:
            try:
                self.delete_sandbox(box["id"])
            except Kosa8Error:
                pass
            raise
        return box

    def get_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return (self._request("GET", _sandbox_path(sandbox_id)))["sandbox"]

    def delete_sandbox(self, sandbox_id: str, *, missing_ok: bool = True) -> None:
        try:
            self._request("DELETE", _sandbox_path(sandbox_id))
        except Kosa8Error as e:
            if not (missing_ok and e.status in (404, 410)):
                raise

    def sleep_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return (self._request("POST", _sandbox_path(sandbox_id, "/sleep")))["sandbox"]

    def wake_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return (self._request("POST", _sandbox_path(sandbox_id, "/wake")))["sandbox"]

    def list_templates(self) -> list[dict[str, Any]]:
        return (self._request("GET", "/v1/templates"))["templates"]

    # Files (under files_root, at most 8 MiB) ---------------------------------

    def put_file(self, sandbox_id: str, path: str, data: bytes) -> None:
        """Write a file through the files API (its parent must exist)."""
        self._send("PUT", _sandbox_path(sandbox_id, "/files"), params={"path": path}, content=data, timeout=max(self.timeout, 120.0))

    def get_file(self, sandbox_id: str, path: str) -> bytes:
        """Read a file through the files API."""
        resp = self._send("GET", _sandbox_path(sandbox_id, "/files"), params={"path": path}, timeout=max(self.timeout, 120.0))
        return resp.content

    def make_dir(self, sandbox_id: str, path: str) -> None:
        """Make a folder and its parents through the files API."""
        self._send("POST", _sandbox_path(sandbox_id, "/files/mkdir"), {"path": path})

    # Exec --------------------------------------------------------------------

    def exec(self, sandbox_id: str, request: ExecRequest) -> ExecOutput:
        """Run argv in the sandbox and wait for it.

        A command the API stopped at its ``timeout_s`` answers with
        ``timed_out`` set and the output so far."""
        if self._stream is not False:
            try:
                return self._exec_stream(sandbox_id, request)
            except Kosa8Error as e:
                if e.status != 501:
                    raise
                if self._stream_timeout and "timeout_s" in e.message:
                    # The host streams but cannot time a stream; the in-sandbox timeout still applies.
                    self._stream_timeout = False
                    return self._exec_stream(sandbox_id, request)
                if self._stream is True:
                    raise
                self._stream = False  # this host has no streamed exec; stop asking
        timeout_s = _server_timeout(request, stream=False)
        body = _exec_body(request.argv, request.env, stream=False, image=request.image, timeout_s=timeout_s)
        try:
            data = self._request("POST", _sandbox_path(sandbox_id, "/exec"), body, timeout=BUFFERED_REQUEST_TIMEOUT_S)
        except Kosa8TimeoutError as e:
            if e.code == "exec_timeout" and timeout_s is not None and timeout_s < BUFFERED_EXEC_LIMIT_S:
                return _timed_out_output(e)
            raise
        return _buffered_output(data)

    def _exec_stream(self, sandbox_id: str, request: ExecRequest) -> ExecOutput:
        timeout_s = _server_timeout(request, stream=True) if self._stream_timeout else None
        body = _exec_body(request.argv, request.env, stream=True, image=request.image, timeout_s=timeout_s)
        read = None if request.timeout is None else request.timeout + 30.0
        col = _StreamCollector()
        try:
            with self._http.stream(
                "POST",
                self.base_url + _sandbox_path(sandbox_id, "/exec"),
                headers=self._headers(),
                json=body,
                timeout=httpx.Timeout(self.timeout, read=read),
            ) as resp:
                if resp.status_code >= 400:
                    resp.read()
                    raise _error_from_response(resp)
                if "ndjson" not in resp.headers.get("content-type", ""):
                    resp.read()
                    return _buffered_output(resp.json())
                for line in resp.iter_lines():
                    col.feed(line)
        except httpx.TimeoutException as e:
            raise Kosa8TimeoutError("kosa8 Cloud stopped sending command output in time") from e
        except httpx.HTTPError as e:
            raise Kosa8Error(f"could not reach kosa8 Cloud at {self.base_url}: {e}") from e
        return col.result()


class AsyncKosa8Client:
    """A small asynchronous client for the kosa8 Cloud sandbox API."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = 60.0,
        http_client: httpx.AsyncClient | None = None,
        stream: bool | None = None,
        files_api: bool | None = None,
        files_root: str = FILES_API_ROOT,
    ) -> None:
        """Create a client.

        Args:
            api_key: kosa8 Cloud API key. Defaults to ``KOSA8_API_KEY``.
            base_url: API address. Defaults to ``KOSA8_CLOUD_URL`` or
                ``https://api.kosa8.com``.
            timeout: Timeout in seconds for control-plane requests.
            http_client: An ``httpx.AsyncClient`` to use instead of a new one.
            stream: ``True`` to require streamed exec, ``False`` to always use
                buffered exec, ``None`` (default) to try streamed exec and fall
                back to buffered exec when the host does not offer it.
            files_api: ``False`` to move every file through exec; ``None``
                (default) to use the files API for files under ``files_root``
                until a host answers that it has none.
            files_root: The directory the files API reaches.
        """
        self._key = _resolve_api_key(api_key)
        self.base_url = _resolve_base_url(base_url)
        self.timeout = timeout
        self._http = http_client or httpx.AsyncClient(timeout=timeout)
        self._own_http = http_client is None
        self._stream = stream
        # Whether the host takes timeout_s on a streamed exec (older ones answer 501).
        self._stream_timeout = True
        self.files_api = files_api
        self.files_root = files_root.rstrip("/") or "/"

    def __repr__(self) -> str:  # never the key
        return f"AsyncKosa8Client(base_url={self.base_url!r})"

    async def aclose(self) -> None:
        if self._own_http:
            await self._http.aclose()

    async def __aenter__(self) -> AsyncKosa8Client:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def _headers(self, content_type: str | None = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._key}",
            "Accept": "application/json",
            "User-Agent": "kosa8-integrations",
        }
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    async def _send(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        timeout: float | None = None,
        params: dict[str, str] | None = None,
        content: bytes | None = None,
    ) -> httpx.Response:
        try:
            resp = await self._http.request(
                method,
                self.base_url + path,
                headers=self._headers("application/octet-stream" if content is not None else None),
                json=body if content is None else None,
                content=content,
                params=params,
                timeout=timeout if timeout is not None else self.timeout,
            )
        except httpx.TimeoutException as e:
            raise Kosa8TimeoutError(f"kosa8 Cloud did not answer {method} {path} in time") from e
        except httpx.HTTPError as e:
            raise Kosa8Error(f"could not reach kosa8 Cloud at {self.base_url}: {e}") from e
        if resp.status_code >= 400:
            raise _error_from_response(resp)
        return resp

    async def _request(self, method: str, path: str, body: Any = None, timeout: float | None = None) -> Any:
        resp = await self._send(method, path, body, timeout=timeout)
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError as e:
            raise Kosa8Error(f"kosa8 Cloud answered {method} {path} with a body that is not JSON") from e

    # Sandboxes ---------------------------------------------------------------

    async def create_sandbox(
        self,
        *,
        template: str | None = None,
        region: str | None = None,
        sleep_after_s: int | None = None,
        ttl_s: int | None = None,
    ) -> dict[str, Any]:
        """Start a sandbox and return its record once it is running.

        A create that boots cold answers ``creating``; it is polled until it
        runs, and deleted if it does not."""
        r = await self._request(
            "POST", "/v1/sandboxes", _create_body(template, region, sleep_after_s, ttl_s), timeout=max(self.timeout, 90.0)
        )
        box = r["sandbox"]
        deadline = time.monotonic() + CREATE_WAIT_S
        try:
            while box.get("state") == "creating":
                if time.monotonic() >= deadline:
                    raise Kosa8TimeoutError(f"kosa8 sandbox {box['id']} was still starting after {CREATE_WAIT_S:.0f} s")
                await asyncio.sleep(CREATE_POLL_S)
                box = await self.get_sandbox(box["id"])
            if box.get("state") != "running":
                raise Kosa8Error(f"kosa8 sandbox {box['id']} did not start: {box.get('last_error') or box.get('state')}")
        except BaseException:
            try:
                await self.delete_sandbox(box["id"])
            except Kosa8Error:
                pass
            raise
        return box

    async def get_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return (await self._request("GET", _sandbox_path(sandbox_id)))["sandbox"]

    async def delete_sandbox(self, sandbox_id: str, *, missing_ok: bool = True) -> None:
        try:
            await self._request("DELETE", _sandbox_path(sandbox_id))
        except Kosa8Error as e:
            if not (missing_ok and e.status in (404, 410)):
                raise

    async def sleep_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return (await self._request("POST", _sandbox_path(sandbox_id, "/sleep")))["sandbox"]

    async def wake_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return (await self._request("POST", _sandbox_path(sandbox_id, "/wake")))["sandbox"]

    async def list_templates(self) -> list[dict[str, Any]]:
        return (await self._request("GET", "/v1/templates"))["templates"]

    # Files (under files_root, at most 8 MiB) ---------------------------------

    async def put_file(self, sandbox_id: str, path: str, data: bytes) -> None:
        """Write a file through the files API (its parent must exist)."""
        await self._send("PUT", _sandbox_path(sandbox_id, "/files"), params={"path": path}, content=data, timeout=max(self.timeout, 120.0))

    async def get_file(self, sandbox_id: str, path: str) -> bytes:
        """Read a file through the files API."""
        resp = await self._send("GET", _sandbox_path(sandbox_id, "/files"), params={"path": path}, timeout=max(self.timeout, 120.0))
        return resp.content

    async def make_dir(self, sandbox_id: str, path: str) -> None:
        """Make a folder and its parents through the files API."""
        await self._send("POST", _sandbox_path(sandbox_id, "/files/mkdir"), {"path": path})

    # Exec --------------------------------------------------------------------

    async def exec(self, sandbox_id: str, request: ExecRequest) -> ExecOutput:
        """Run argv in the sandbox and wait for it.

        A command the API stopped at its ``timeout_s`` answers with
        ``timed_out`` set and the output so far."""
        if self._stream is not False:
            try:
                return await self._exec_stream(sandbox_id, request)
            except Kosa8Error as e:
                if e.status != 501:
                    raise
                if self._stream_timeout and "timeout_s" in e.message:
                    # The host streams but cannot time a stream; the in-sandbox timeout still applies.
                    self._stream_timeout = False
                    return await self._exec_stream(sandbox_id, request)
                if self._stream is True:
                    raise
                self._stream = False  # this host has no streamed exec; stop asking
        timeout_s = _server_timeout(request, stream=False)
        body = _exec_body(request.argv, request.env, stream=False, image=request.image, timeout_s=timeout_s)
        try:
            data = await self._request("POST", _sandbox_path(sandbox_id, "/exec"), body, timeout=BUFFERED_REQUEST_TIMEOUT_S)
        except Kosa8TimeoutError as e:
            if e.code == "exec_timeout" and timeout_s is not None and timeout_s < BUFFERED_EXEC_LIMIT_S:
                return _timed_out_output(e)
            raise
        return _buffered_output(data)

    async def _exec_stream(self, sandbox_id: str, request: ExecRequest) -> ExecOutput:
        timeout_s = _server_timeout(request, stream=True) if self._stream_timeout else None
        body = _exec_body(request.argv, request.env, stream=True, image=request.image, timeout_s=timeout_s)
        read = None if request.timeout is None else request.timeout + 30.0
        col = _StreamCollector()
        try:
            async with self._http.stream(
                "POST",
                self.base_url + _sandbox_path(sandbox_id, "/exec"),
                headers=self._headers(),
                json=body,
                timeout=httpx.Timeout(self.timeout, read=read),
            ) as resp:
                if resp.status_code >= 400:
                    await resp.aread()
                    raise _error_from_response(resp)
                if "ndjson" not in resp.headers.get("content-type", ""):
                    await resp.aread()
                    return _buffered_output(resp.json())
                async for line in resp.aiter_lines():
                    col.feed(line)
        except httpx.TimeoutException as e:
            raise Kosa8TimeoutError("kosa8 Cloud stopped sending command output in time") from e
        except httpx.HTTPError as e:
            raise Kosa8Error(f"could not reach kosa8 Cloud at {self.base_url}: {e}") from e
        return col.result()


# Shell scripts ----------------------------------------------------------------
#
# Each script is POSIX sh (busybox ash, dash and bash all run it). Data and
# commands travel in K8_* environment variables, not argv: the API takes at
# most 8192 characters per argument but a whole 64 KiB body.

_PERSIST_FN = (
    'k8p(){ t=$1; d=$2; [ -L "$d" ] && return 0; [ -d "$t" ] && [ "$d" -ef "$t" ] && return 0; '
    'if [ ! -d "$t" ]; then mkdir -p "${t%/*}" 2>/dev/null; '
    'if mkdir "$t" 2>/dev/null; then [ -d "$d" ] && cp -a "$d"/. "$t"/ 2>/dev/null; '
    'case "$d" in /tmp|/var/tmp) chmod 1777 "$t" 2>/dev/null;; esac; fi; fi; '
    'mount --bind "$t" "$d" 2>/dev/null && return 0; '
    'rm -rf "$d" 2>/dev/null; ln -s "$t" "$d" 2>/dev/null; return 0; }; '
)

# Runs K8_CMD (or the script file K8_CMDFILE, read by the shell itself so a
# command of any size works) with K8_SHELL in K8_CWD, stdin from K8_STDIN
# (base64) or K8_STDINFILE, under /usr/bin/timeout when K8_TIMEOUT is set.
# The command never sees the K8_* variables.
_RUN_SCRIPT = (
    "c=$K8_CMD; cf=$K8_CMDFILE; w=$K8_CWD; t=$K8_TIMEOUT; i=$K8_STDIN; inf=$K8_STDINFILE; s=$K8_SHELL; "
    "unset K8_CMD K8_CMDFILE K8_CWD K8_TIMEOUT K8_STDIN K8_STDINFILE K8_SHELL; "
    'if [ -z "$s" ] || [ "$s" = auto ]; then if [ -x /bin/bash ]; then s=/bin/bash; else s=/bin/sh; fi; fi; '
    'if [ -n "$w" ]; then cd "$w" 2>/dev/null || { echo "kosa8: cannot change to directory $w" >&2; exit 126; }; fi; '
    'T=; if [ -n "$t" ]; then for x in /usr/bin/timeout /bin/timeout; do if [ -x "$x" ]; then T=$x; break; fi; done; fi; '
    'k8r(){ if [ -n "$T" ]; then "$T" -s KILL "$t" "$@"; else "$@"; fi; }; '
    'k8c(){ if [ -n "$cf" ]; then k8r "$s" "$cf"; else k8r "$s" -c "$c"; fi; }; '
    'if [ -n "$inf" ]; then k8c < "$inf"; r=$?; rm -f "$inf"; '
    'elif [ -n "$i" ]; then printf %s "$i" | base64 -d | k8c; r=$?; '
    "else k8c < /dev/null; r=$?; fi; "
    '[ -n "$cf" ] && rm -f "$cf"; exit $r'
)

# $1 path, $2 w (truncate) or a (append), data in K8_DATA. Errors go to stderr
# as one word: K8ERR:<code>.
_UPLOAD_SCRIPT = (
    "p=$1; m=$2; d=$K8_DATA; unset K8_DATA; "
    'if [ "$m" = w ]; then '
    '[ -d "$p" ] && { echo K8ERR:is_directory >&2; exit 21; }; '
    'dir=${p%/*}; [ -z "$dir" ] && dir=/; '
    'if [ ! -d "$dir" ]; then mkdir -p "$dir" 2>/dev/null || { '
    'if [ -e "$dir" ]; then echo K8ERR:invalid_path >&2; else echo K8ERR:permission_denied >&2; fi; exit 22; }; fi; '
    '( : > "$p" ) 2>/dev/null || { echo K8ERR:permission_denied >&2; exit 23; }; fi; '
    '[ -z "$d" ] && exit 0; '
    "command -v base64 >/dev/null 2>&1 || { echo K8ERR:base64_missing >&2; exit 24; }; "
    'printf %s "$d" | base64 -d >> "$p" || { echo K8ERR:write_failed >&2; exit 25; }'
)

# $1 path, $2 chunk size, $3 chunk index. Chunk 0 starts with a status line.
_DOWNLOAD_SCRIPT = (
    "p=$1; c=$2; n=$3; "
    'if [ "$n" = 0 ]; then '
    'if [ -d "$p" ]; then echo K8:is_directory; exit 0; fi; '
    'if [ ! -e "$p" ]; then echo K8:file_not_found; exit 0; fi; '
    'if [ ! -r "$p" ]; then echo K8:permission_denied; exit 0; fi; '
    'z=$(wc -c < "$p" 2>/dev/null) || { echo K8:permission_denied; exit 0; }; '
    "command -v base64 >/dev/null 2>&1 || { echo K8:base64_missing; exit 0; }; "
    "echo K8:ok $z; fi; "
    'dd if="$p" bs="$c" skip="$n" count=1 2>/dev/null | base64'
)


# System directories that must never be replaced by a link.
UNSAFE_PERSIST_DIRS = frozenset(
    {"/", "/bin", "/sbin", "/usr", "/lib", "/lib32", "/lib64", "/etc", "/proc", "/sys", "/dev", "/var", "/boot", "/run"}
)


def persist_prelude(persist_dirs: Sequence[str], workspace: str) -> str:
    """Shell that makes each dir in persist_dirs live on the workspace disk."""
    dirs = [d.rstrip("/") or "/" for d in persist_dirs if d]
    if not dirs:
        return ""
    ws = workspace.rstrip("/")
    calls = []
    for d in dirs:
        if not d.startswith("/"):
            raise ValueError(f"persist_dirs entries must be absolute paths: {d!r}")
        if d in UNSAFE_PERSIST_DIRS or "/.." in d or "/./" in d:
            raise ValueError(f"persist_dirs entry {d!r} is a system directory or not a plain path")
        if d == ws or d.startswith(ws + "/") or ws.startswith(d + "/"):
            raise ValueError(f"persist_dirs entry {d!r} overlaps the workspace {ws!r}")
        target = f"{ws}/.kosa8-persist/{d.strip('/').replace('/', '__')}"
        calls.append(f"k8p {shlex.quote(target)} {shlex.quote(d)}; ")
    return _PERSIST_FN + "".join(calls)


T = TypeVar("T")
Op = Generator["ExecRequest | UploadRequest", Any, T]

# $1 target, then the staged part files: write them, in order, into the
# target as the sandbox user and remove them. With no parts the target is
# made empty. Errors go to stderr as K8ERR:<code>.
_INSTALL_SCRIPT = (
    "t=$1; shift; "
    '[ -d "$t" ] && { rm -f "$@"; echo K8ERR:is_directory >&2; exit 21; }; '
    'dir=${t%/*}; [ -z "$dir" ] && dir=/; '
    'if [ ! -d "$dir" ]; then mkdir -p "$dir" 2>/dev/null || { rm -f "$@"; '
    'if [ -e "$dir" ]; then echo K8ERR:invalid_path >&2; else echo K8ERR:permission_denied >&2; fi; exit 22; }; fi; '
    '( : >> "$t" ) 2>/dev/null || { rm -f "$@"; echo K8ERR:permission_denied >&2; exit 23; }; '
    'if [ "$#" = 0 ]; then : > "$t"; exit 0; fi; '
    'cat "$@" > "$t" 2>/dev/null; r=$?; rm -f "$@"; '
    '[ "$r" = 0 ] || { echo K8ERR:write_failed >&2; exit 25; }'
)

# $1 file, $2 part size, $3 part prefix, $4 max bytes (or empty), $5 1 to
# always split. Checks the file as the sandbox user and prints
# "K8:<status> <size> <parts>"; a file over $2 bytes (or any, with $5) is
# split into $3.0, $3.1, ... for the files API to read.
_READ_SCRIPT = (
    "p=$1; b=$2; o=$3; mx=$4; "
    'if [ -d "$p" ]; then echo K8:is_directory; exit 0; fi; '
    'if [ ! -e "$p" ]; then echo K8:file_not_found; exit 0; fi; '
    'if [ ! -r "$p" ]; then echo K8:permission_denied; exit 0; fi; '
    'z=$(wc -c < "$p" 2>/dev/null) || { echo K8:permission_denied; exit 0; }; z=$((z + 0)); '
    'if [ -n "$mx" ] && [ "$z" -gt "$mx" ]; then echo K8:too_large; exit 0; fi; '
    'n=0; if [ "$z" -gt "$b" ] || [ "$5" = 1 ]; then mkdir -p "${o%/*}" || exit 1; '
    'while [ $((n * b)) -lt "$z" ]; do '
    'dd if="$p" of="$o.$n" bs="$b" skip="$n" count=1 2>/dev/null || { rm -f "$o".*; exit 1; }; n=$((n + 1)); done; fi; '
    'echo "K8:ok $z $n"'
)


def drive(op: Op[T], send: Callable[[ExecRequest], ExecOutput], put: Callable[[UploadRequest], Any] | None = None) -> T:
    """Run an operation, sending each exec request it yields synchronously.

    ``put(request)`` carries its file writes (see :func:`files_put`);
    without it they go through exec."""
    try:
        req = next(op)
        while True:
            if isinstance(req, UploadRequest):
                req = op.send(put(req) if put is not None else FALLBACK)
            else:
                req = op.send(send(req))
    except StopIteration as stop:
        return stop.value


async def adrive(op: Op[T], send: Callable[[ExecRequest], Any], put: Callable[[UploadRequest], Any] | None = None) -> T:
    """Run an operation, awaiting each exec request it yields (and ``put``, as :func:`drive`)."""
    try:
        req = next(op)
        while True:
            if isinstance(req, UploadRequest):
                req = op.send(await put(req) if put is not None else FALLBACK)
            else:
                req = op.send(await send(req))
    except StopIteration as stop:
        return stop.value


def _files_api_path(client: Kosa8Client | AsyncKosa8Client, path: str) -> bool:
    """Whether the files API should carry this path (a plain path under the
    client's files_root; larger files travel as parts)."""
    if client.files_api is False:
        return False
    norm = posixpath.normpath(path)
    root = client.files_root
    return norm == path and norm != root and norm.startswith(root.rstrip("/") + "/")


def _files_api_fallback(client: Kosa8Client | AsyncKosa8Client, err: Kosa8Error) -> bool:
    """Whether an error from the files API sends the operation through exec
    instead. Exec reports file errors precisely, so most answers do; those
    exec would meet too (no key, no plan, rate limits, an unreachable API) do not."""
    if err.status == 501:
        client.files_api = False  # this host has no files API; stop asking
        return True
    return err.status is not None and err.status not in (401, 402, 403, 429)


def _parts(base: str, data: bytes) -> list[tuple[str, bytes]]:
    step = MAX_FILES_API_BYTES
    return [(f"{base}.k8part{i}", data[off : off + step]) for i, off in enumerate(range(0, len(data), step))]


def _put_plan(
    client: Kosa8Client | AsyncKosa8Client, session: SandboxSession, req: UploadRequest
) -> tuple[str, list[tuple[str, bytes]], bool] | None:
    """How the files API carries one write: (directory to make, files to PUT,
    whether one exec then installs them into ``req.path``), or None for exec.

    The transport's own staging files under files_root are written as they
    are (joined by an exec only past 8 MiB). A caller's file is always staged
    in the spill directory and installed as the sandbox user, because the
    files API ignores permissions."""
    if req.direct:
        if not _files_api_path(client, req.path):
            return None
        if len(req.data) <= MAX_FILES_API_BYTES:
            return posixpath.dirname(req.path), [(req.path, req.data)], False
        return posixpath.dirname(req.path), _parts(req.path, req.data), True
    spill = session._spill_dir()
    if not _files_api_path(client, spill):
        return None
    return spill, _parts(f"{spill}/up-{uuid.uuid4().hex}", req.data), True


def files_put(client: Kosa8Client, session: SandboxSession, sandbox_id: str) -> Callable[[UploadRequest], Any]:
    """A ``put`` for :func:`drive` that carries writes through the files API
    (see :func:`_put_plan`). It answers FALLBACK when exec should write the
    file instead: no files API, a spill directory outside it, or an error
    that exec reports better."""

    def put(req: UploadRequest) -> Any:
        plan = _put_plan(client, session, req)
        if plan is None:
            return FALLBACK
        folder, files, install = plan
        sent: list[str] = []
        try:
            if files and folder != client.files_root:
                client.make_dir(sandbox_id, folder)
            for name, part in files:
                sent.append(name)
                client.put_file(sandbox_id, name, part)
            if not install:
                return None
            names = [n for n, _ in files]
            sent = []
            out = client.exec(sandbox_id, session._sh(_INSTALL_SCRIPT, req.path, *names))
            return None if out.exit_code == 0 else _upload_error(out)
        except Kosa8Error as e:
            if sent and install:  # leave no staged parts behind
                try:
                    client.exec(sandbox_id, session._sh('rm -f "$@"', *sent))
                except Kosa8Error:
                    pass
            if not _files_api_fallback(client, e):
                raise
            return FALLBACK

    return put


def afiles_put(client: AsyncKosa8Client, session: SandboxSession, sandbox_id: str) -> Callable[[UploadRequest], Any]:
    """The asynchronous twin of :func:`files_put`."""

    async def put(req: UploadRequest) -> Any:
        plan = _put_plan(client, session, req)
        if plan is None:
            return FALLBACK
        folder, files, install = plan
        sent: list[str] = []
        try:
            if files and folder != client.files_root:
                await client.make_dir(sandbox_id, folder)
            for name, part in files:
                sent.append(name)
                await client.put_file(sandbox_id, name, part)
            if not install:
                return None
            names = [n for n, _ in files]
            sent = []
            out = await client.exec(sandbox_id, session._sh(_INSTALL_SCRIPT, req.path, *names))
            return None if out.exit_code == 0 else _upload_error(out)
        except Kosa8Error as e:
            if sent and install:
                try:
                    await client.exec(sandbox_id, session._sh('rm -f "$@"', *sent))
                except Kosa8Error:
                    pass
            if not _files_api_fallback(client, e):
                raise
            return FALLBACK

    return put


def upload(client: Kosa8Client, session: SandboxSession, sandbox_id: str, path: str, data: bytes) -> str | None:
    """Write ``data`` to the absolute ``path`` in the sandbox as the sandbox
    user, creating parent directories. Returns None, or an error code as
    ``SandboxSession.upload_op``."""
    return drive(session.upload_op(path, data), lambda req: client.exec(sandbox_id, req), files_put(client, session, sandbox_id))


async def aupload(client: AsyncKosa8Client, session: SandboxSession, sandbox_id: str, path: str, data: bytes) -> str | None:
    """The asynchronous twin of :func:`upload`."""
    return await adrive(session.upload_op(path, data), lambda req: client.exec(sandbox_id, req), afiles_put(client, session, sandbox_id))


def _read_request(
    client: Kosa8Client | AsyncKosa8Client, session: SandboxSession, path: str, max_bytes: int | None
) -> tuple[ExecRequest, str] | None:
    """The exec that checks (and if needed splits) a file before the files API
    reads it, and the prefix of its parts; None when exec must read it."""
    spill = session._spill_dir()
    if not path.startswith("/") or not _files_api_path(client, spill):
        return None
    prefix = f"{spill}/dl-{uuid.uuid4().hex}"
    always_split = "0" if _files_api_path(client, path) else "1"
    mx = "" if max_bytes is None else str(max_bytes)
    return session._sh(_READ_SCRIPT, path, str(MAX_FILES_API_BYTES), prefix, mx, always_split), prefix


def _read_status(out: ExecOutput) -> tuple[str, int]:
    """(status, parts) from the read check: status is ok or an error code."""
    head = (out.stdout or "").strip().split()
    if out.exit_code != 0 or not head or not head[0].startswith("K8:"):
        return "error", 0
    status = head[0][3:]
    if status != "ok":
        return status, 0
    try:
        return "ok", int(head[2])
    except (IndexError, ValueError):
        return "error", 0


def download(client: Kosa8Client, session: SandboxSession, sandbox_id: str, path: str, max_bytes: int | None = None) -> DownloadResult:
    """Read the absolute ``path`` as the sandbox user; errors as
    ``SandboxSession.download_op``. One exec checks the file (and splits one
    over 8 MiB, or outside files_root, into staged parts); the files API
    carries the bytes."""
    plan = _read_request(client, session, path, max_bytes) if client.files_api is not False else None
    if plan is not None:
        check, prefix = plan
        status, n = _read_status(client.exec(sandbox_id, check))
        if status not in ("ok", "error"):
            return DownloadResult(None, status)
        if status == "ok":
            names = [f"{prefix}.{i}" for i in range(n)]
            try:
                data = b"".join(client.get_file(sandbox_id, name) for name in names) if names else client.get_file(sandbox_id, path)
                return DownloadResult(data)
            except Kosa8Error as e:
                if not _files_api_fallback(client, e):
                    raise
            finally:
                if names:
                    client.exec(sandbox_id, session._sh('rm -f "$@"', *names))
    return drive(session.download_op(path, max_bytes), lambda req: client.exec(sandbox_id, req))


async def adownload(
    client: AsyncKosa8Client, session: SandboxSession, sandbox_id: str, path: str, max_bytes: int | None = None
) -> DownloadResult:
    """The asynchronous twin of :func:`download`."""
    plan = _read_request(client, session, path, max_bytes) if client.files_api is not False else None
    if plan is not None:
        check, prefix = plan
        status, n = _read_status(await client.exec(sandbox_id, check))
        if status not in ("ok", "error"):
            return DownloadResult(None, status)
        if status == "ok":
            names = [f"{prefix}.{i}" for i in range(n)]
            try:
                if names:
                    data = b"".join([await client.get_file(sandbox_id, name) for name in names])
                else:
                    data = await client.get_file(sandbox_id, path)
                return DownloadResult(data)
            except Kosa8Error as e:
                if not _files_api_fallback(client, e):
                    raise
            finally:
                if names:
                    await client.exec(sandbox_id, session._sh('rm -f "$@"', *names))
    return await adrive(session.download_op(path, max_bytes), lambda req: client.exec(sandbox_id, req))


def _upload_error(out: ExecOutput) -> str:
    m = re.search(r"K8ERR:([a-z_0-9]+)", out.stderr or "")
    if m:
        code = m.group(1)
        if code == "base64_missing":
            raise Kosa8Error("the sandbox image has no base64 command, which file transfer needs")
        if code == "write_failed":
            return _classify_write_failure(out.stderr)
        return code
    return _classify_write_failure(out.stderr or out.stdout or f"exit {out.exit_code}")


def _classify_write_failure(text: str) -> str:
    low = text.lower()
    if "permission denied" in low or "read-only" in low:
        return "permission_denied"
    if "is a directory" in low:
        return "is_directory"
    if "not a directory" in low:
        return "invalid_path"
    if "no space" in low:
        return "no_space_left"
    return f"upload failed: {text.strip()[:200]}"


@dataclass
class SandboxSession:
    """How commands are wrapped for one kosa8 sandbox (shared by the integrations).

    Nothing here talks to the network; it builds exec requests and reads their
    answers, so the integration can drive it with a sync or an async client.
    """

    workspace: str = DEFAULT_WORKSPACE
    persist_dirs: Sequence[str] = DEFAULT_PERSIST_DIRS
    shell: str = "auto"
    #: Image for every command (the exec API's ``image``); None = the template's.
    image: str | None = None
    #: Commands without a cwd start in the workspace; False keeps the image's WORKDIR.
    start_in_workspace: bool = True
    upload_chunk_bytes: int = UPLOAD_CHUNK_BYTES
    download_chunk_bytes: int = DOWNLOAD_CHUNK_BYTES
    body_limit: int = REQUEST_BODY_LIMIT

    def __post_init__(self) -> None:
        self._prelude = persist_prelude(self.persist_dirs, self.workspace)

    def _sh(self, script: str, *args: str, env: dict[str, str] | None = None, timeout: float | None = None) -> ExecRequest:
        return ExecRequest(argv=["/bin/sh", "-c", self._prelude + script, "k8", *args], env=env or {}, timeout=timeout, image=self.image)

    def _spill_dir(self) -> str:
        return f"{self.workspace.rstrip('/')}/.kosa8-run"

    # Upload ------------------------------------------------------------------

    def upload_op(self, path: str, data: bytes) -> Op[str | None]:
        """Write data to an absolute path, creating parent directories.

        Returns None on success or an error code (``permission_denied``,
        ``is_directory``, ``invalid_path``, or a description).
        """
        if not path.startswith("/"):
            return _const("invalid_path")
        return self._upload_gen(path, data)

    def _upload_gen(self, path: str, data: bytes, direct: bool = False) -> Op[str | None]:
        done = yield UploadRequest(path, data, direct)
        if done is not FALLBACK:
            return done
        step = max(1, self.upload_chunk_bytes)
        offsets = range(0, len(data), step) if data else [0]
        for idx, off in enumerate(offsets):
            part = data[off : off + step]
            out = yield self._sh(_UPLOAD_SCRIPT, path, "w" if idx == 0 else "a", env={"K8_DATA": _b64(part)})
            if out.exit_code != 0:
                return _upload_error(out)
        return None

    # Download ----------------------------------------------------------------

    def download_op(self, path: str, max_bytes: int | None = None) -> Op[DownloadResult]:
        """Read an absolute path. Errors: file_not_found, is_directory,
        permission_denied, invalid_path, and too_large when the file is over
        max_bytes."""
        if not path.startswith("/"):
            return _const(DownloadResult(None, "invalid_path"))
        return self._download_gen(path, max_bytes)

    def _download_gen(self, path: str, max_bytes: int | None = None) -> Op[DownloadResult]:
        chunk = max(1, self.download_chunk_bytes)
        out = yield self._sh(_DOWNLOAD_SCRIPT, path, str(chunk), "0")
        if out.exit_code != 0:
            return DownloadResult(None, _classify_write_failure(out.stderr or out.stdout).replace("upload", "download"))
        head, _, rest = out.stdout.partition("\n")
        head = head.strip()
        if not head.startswith("K8:"):
            return DownloadResult(None, f"download failed: unexpected answer {head[:100]!r}")
        status = head[3:].split()
        if not status:
            return DownloadResult(None, "download failed: empty status")
        if status[0] == "base64_missing":
            raise Kosa8Error("the sandbox image has no base64 command, which file transfer needs")
        if status[0] != "ok":
            return DownloadResult(None, status[0])
        try:
            size = int(status[1])
        except (IndexError, ValueError):
            return DownloadResult(None, "download failed: could not read the file size")
        if max_bytes is not None and size > max_bytes:
            return DownloadResult(None, "too_large")
        buf = bytearray(_unb64(rest))
        n = 1
        while len(buf) < size:
            out = yield self._sh(_DOWNLOAD_SCRIPT, path, str(chunk), str(n))
            if out.exit_code != 0:
                return DownloadResult(None, f"download failed at byte {len(buf)}: {(out.stderr or '').strip()[:200]}")
            part = _unb64(out.stdout)
            if not part:
                break
            buf.extend(part)
            n += 1
        if len(buf) != size:
            return DownloadResult(None, f"download failed: expected {size} bytes, got {len(buf)} (the file changed?)")
        return DownloadResult(bytes(buf))

    # Commands ----------------------------------------------------------------

    def command_op(
        self,
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        stdin: bytes | None = None,
        shell: str | None = None,
    ) -> Op[ExecOutput]:
        """Run a shell command string; returns its output.

        ``timeout`` is enforced inside the sandbox with ``timeout -s KILL``;
        ``ExecOutput.timed_out`` is set when it fired.
        """
        user_env = _check_env(env)
        return self._command_gen(command, cwd=cwd, env=user_env, timeout=timeout, stdin=stdin, shell=shell)

    def _command_gen(
        self,
        command: str,
        *,
        cwd: str | None,
        env: dict[str, str],
        timeout: float | None,
        stdin: bytes | None,
        shell: str | None,
    ) -> Op[ExecOutput]:
        k8: dict[str, str] = {"K8_SHELL": shell or self.shell, "K8_CMD": command}
        if cwd is None:
            cwd = self.workspace if self.start_in_workspace else ""
        elif not cwd.startswith("/"):
            cwd = f"{self.workspace.rstrip('/')}/{cwd}"
        if cwd:
            k8["K8_CWD"] = cwd
        secs: int | None = None
        if timeout is not None and timeout > 0:
            secs = max(1, int(-(-timeout // 1)))  # whole seconds, rounded up
            k8["K8_TIMEOUT"] = str(secs)
        if stdin is not None:
            k8["K8_STDIN"] = _b64(stdin)

        def size() -> int:
            body = _exec_body(["/bin/sh", "-c", self._prelude + _RUN_SCRIPT, "k8"], {**env, **k8}, stream=False)
            return len(json.dumps(body))

        spill = self._spill_dir()
        if size() > self.body_limit and "K8_STDIN" in k8:
            name = f"{spill}/stdin-{uuid.uuid4().hex}"
            err = yield from self._upload_gen(name, stdin or b"", direct=True)
            if err:
                raise Kosa8Error(f"could not pass stdin to the command: {err}")
            del k8["K8_STDIN"]
            k8["K8_STDINFILE"] = name
        if size() > self.body_limit:
            name = f"{spill}/cmd-{uuid.uuid4().hex}"
            err = yield from self._upload_gen(name, command.encode("utf-8"), direct=True)
            if err:
                raise Kosa8Error(f"could not pass the command to the sandbox: {err}")
            del k8["K8_CMD"]
            k8["K8_CMDFILE"] = name
        if size() > self.body_limit:
            raise ValueError("the command's environment variables are too large for one exec request")

        started = time.monotonic()
        out = yield self._sh(_RUN_SCRIPT, env={**env, **k8}, timeout=timeout)
        if secs is not None and out.exit_code in (124, 137) and time.monotonic() - started >= secs - 0.5:
            out.timed_out = True
        return out


def _unb64(text: str) -> bytes:
    cleaned = re.sub(r"[^A-Za-z0-9+/=]", "", text)
    return base64.b64decode(cleaned) if cleaned else b""


def _const(value: T) -> Op[T]:
    return value
    yield  # pragma: no cover  (makes this a generator)

"""kosa8 Cloud transport shared by the kosa8 framework integrations.

This module is vendored, unchanged, into ``langchain-kosa8``, ``inspect-kosa8``
and ``harbor-kosa8``. Keep the copies in sync.

It talks to the kosa8 Cloud API (``https://api.kosa8.com``) with an API key
and needs nothing but ``httpx``. What it relies on:

* ``POST /v1/sandboxes`` to create, ``GET`` / ``DELETE /v1/sandboxes/{id}``,
  ``POST /v1/sandboxes/{id}/sleep`` and ``/wake``, ``GET /v1/templates``.
* ``POST /v1/sandboxes/{id}/exec`` with ``{"cmd": [...], "env": [...]}``.

The clients also cover the rest of the sandbox API for callers who want it:
``PATCH`` (lifetime), list with ``limit``/``cursor`` paging, and the data-plane
routes (files, checkpoints, rewind, fork, ports). The data-plane routes answer
501 (:class:`Kosa8NotSupportedError`) on hosts without the data plane, and not
every kosa8 Cloud host has it yet, so the integrations themselves do not rely
on them.

Errors are typed: :class:`Kosa8PaymentRequiredError` (402, with
``upgrade_url`` and ``agent_message``), :class:`Kosa8OrgFrozenError` (423),
:class:`Kosa8RateLimitError` (429), :class:`Kosa8ExecTimeoutError` (an exec cut
off by the API's time limit) and :class:`Kosa8NotSupportedError` (501). All are
:class:`Kosa8Error` with ``status`` and ``code``.

How commands run on kosa8 Cloud today, and what this module does about it:

* Every exec starts a fresh container from the sandbox's template image (or the
  image the request names). The
  sandbox's ``/workspace`` disk is what carries files from one command to the
  next (and through sleep). Anything written elsewhere is gone when the
  command ends. So that tools which use ``/tmp`` keep working, each command
  first links (or bind-mounts) ``/tmp`` onto ``/workspace/.kosa8-persist/tmp``.
  The list of such directories is configurable (``persist_dirs``).
* The file endpoints need the host's data plane, which production hosts do
  not all have yet, so file upload and download go through exec: the bytes travel base64-encoded, in chunks sized
  to the API's request (64 KiB) and output (1 MiB) limits.
* A buffered exec must finish within the host's request time (about 30 s).
  When the host offers streamed exec, it is used instead and long commands
  are not cut off; otherwise this module falls back to buffered exec.
* There is no stdin, user or working-directory field in the exec API; this
  module provides stdin, working directory and timeouts with a small POSIX
  shell wrapper. Commands run as the image's default user.
"""

from __future__ import annotations

import base64
import json
import os
import re
import shlex
import time
import uuid
from collections.abc import AsyncIterator, Generator, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar
from urllib.parse import quote, urlencode

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
    "Kosa8ExecTimeoutError",
    "Kosa8NotSupportedError",
    "Kosa8OrgFrozenError",
    "Kosa8PaymentRequiredError",
    "Kosa8RateLimitError",
    "Kosa8TimeoutError",
    "SandboxPage",
    "SandboxSession",
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
# The host gives a buffered exec about 30 s; wait a little longer for its answer.
BUFFERED_REQUEST_TIMEOUT_S = 45.0
EXEC_LIMIT_S = 30
# A ttl_s is from 60 s to 90 days; a fork makes 1 to 100 copies.
MIN_TTL_S = 60
MAX_TTL_S = 7_776_000
MAX_FORK = 100

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
        #: The decoded error body, when there was one.
        self.body = body


class Kosa8TimeoutError(Kosa8Error, TimeoutError):
    """A command or request did not finish in time."""


class Kosa8ExecTimeoutError(Kosa8TimeoutError):
    """An exec was cut off by the API's time limit.

    A buffered exec is limited to about 30 s today (HTTP 502 "timed out"; later
    servers answer 504 ``exec_timeout``). Streamed exec, used when the host
    offers it, is not cut off.
    """


class Kosa8PaymentRequiredError(Kosa8Error):
    """402: new work is refused until a payment method or credit is on file
    (``payment_required``), or the spending cap is reached
    (``spending_cap_reached``).

    ``upgrade_url`` is where the person fixes it; ``agent_message`` is a
    sentence an agent should relay to its person word for word, then stop.
    """

    @property
    def upgrade_url(self) -> str | None:
        return _body_str(self.body, "upgrade_url")

    @property
    def agent_message(self) -> str | None:
        return _body_str(self.body, "agent_message")


class Kosa8OrgFrozenError(Kosa8Error):
    """423 ``org_frozen``: the org is frozen in the Security Center; an owner or admin must unfreeze it."""


class Kosa8RateLimitError(Kosa8Error):
    """429: too many requests, or ``quota_exceeded`` (the org's active sandbox limit).

    ``retry_after`` is the server's Retry-After in seconds, when it sent one.
    """

    retry_after: float | None = None


class Kosa8NotSupportedError(Kosa8Error):
    """501: the sandbox's host cannot do this yet (``not_available`` / ``not_supported``)."""


def _body_str(body: Any, key: str) -> str | None:
    if isinstance(body, dict) and isinstance(body.get(key), str) and body[key]:
        return body[key]
    return None


@dataclass
class SandboxPage:
    """One page of sandboxes; ``next_cursor`` is ``None`` on the last page,
    and always on a server that does not page yet."""

    sandboxes: list[dict[str, Any]]
    next_cursor: str | None = None


@dataclass
class ExecOutput:
    """What one exec request answered."""

    exit_code: Optional[int]
    stdout: str
    stderr: str
    truncated: bool = False
    timed_out: bool = False


@dataclass
class ExecRequest:
    """One exec request: argv plus internal environment variables."""

    argv: list[str]
    env: dict[str, str] = field(default_factory=dict)
    timeout: Optional[float] = None
    image: Optional[str] = None


@dataclass
class DownloadResult:
    content: Optional[bytes]
    error: Optional[str] = None


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
    status = resp.status_code
    try:
        on_exec = resp.request.url.path.endswith("/exec")
    except RuntimeError:  # a Response built without a request
        on_exec = False
    cls: type[Kosa8Error] = Kosa8Error
    if code == "exec_timeout" or (on_exec and status in (502, 504) and _HOST_TIMEOUT_RE.search(message)):
        cls = Kosa8ExecTimeoutError
        message = f"{message} (a buffered exec is limited to about {EXEC_LIMIT_S} s)"
    elif status in (502, 504) and _HOST_TIMEOUT_RE.search(message):
        cls = Kosa8TimeoutError
    elif status == 402:
        cls = Kosa8PaymentRequiredError
    elif status == 423:
        cls = Kosa8OrgFrozenError
    elif status == 429:
        cls = Kosa8RateLimitError
    elif status == 501:
        cls = Kosa8NotSupportedError
    err = cls(message, status=status, code=code, body=body)
    if isinstance(err, Kosa8RateLimitError):
        ra = resp.headers.get("retry-after")
        try:
            err.retry_after = float(ra) if ra is not None else None
        except ValueError:
            err.retry_after = None
    return err


def _exec_body(argv: Sequence[str], env: Mapping[str, str], stream: bool, image: str | None = None) -> dict[str, Any]:
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
    return body


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
        self.exit: Optional[int] = None
        self.error: Optional[str] = None
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


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _check_lifetime(ttl_s: int | None, delete_after: int | None) -> None:
    if ttl_s is not None and delete_after is not None:
        raise ValueError("give ttl_s or delete_after, not both")
    if ttl_s is not None and (not _is_int(ttl_s) or not MIN_TTL_S <= ttl_s <= MAX_TTL_S):
        raise ValueError(f"ttl_s must be a whole number of seconds from {MIN_TTL_S} to {MAX_TTL_S}")
    if delete_after is not None and not _is_int(delete_after):
        raise ValueError("delete_after must be a Unix time in milliseconds")


def _create_body(
    template: str | None,
    region: str | None,
    sleep_after_s: int | None,
    ttl_s: int | None,
    delete_after: int | None = None,
) -> dict[str, Any]:
    _check_lifetime(ttl_s, delete_after)
    body: dict[str, Any] = {}
    if template is not None:
        body["template"] = template
    if region is not None:
        body["region"] = region
    if sleep_after_s is not None:
        body["sleep_after_s"] = sleep_after_s
    if ttl_s is not None:
        body["ttl_s"] = ttl_s
    if delete_after is not None:
        body["delete_after"] = delete_after
    return body


def _update_body(
    ttl_s: int | None, delete_after: int | None, sleep_after_s: int | None, clear_lifetime: bool
) -> dict[str, Any]:
    if clear_lifetime and (ttl_s is not None or delete_after is not None):
        raise ValueError("clear_lifetime cannot be combined with ttl_s or delete_after")
    _check_lifetime(ttl_s, delete_after)
    body: dict[str, Any] = {}
    if clear_lifetime:
        body["ttl_s"] = None
    if ttl_s is not None:
        body["ttl_s"] = ttl_s
    if delete_after is not None:
        body["delete_after"] = delete_after
    if sleep_after_s is not None:
        body["sleep_after_s"] = sleep_after_s
    if not body:
        raise ValueError("give ttl_s, delete_after, sleep_after_s or clear_lifetime")
    return body


def _list_path(all: bool, limit: int | None, cursor: str | None) -> str:
    q: dict[str, str] = {}
    if all:
        q["all"] = "1"
    if limit is not None:
        if not _is_int(limit) or limit < 1:
            raise ValueError("limit must be a positive whole number")
        q["limit"] = str(limit)
    if cursor:
        q["cursor"] = cursor
    return "/v1/sandboxes" + ("?" + urlencode(q) if q else "")


def _page(r: Any) -> SandboxPage:
    r = r or {}
    nxt = r.get("next_cursor")
    return SandboxPage(sandboxes=list(r.get("sandboxes", [])), next_cursor=nxt if isinstance(nxt, str) and nxt else None)


def _file_path(sandbox_id: str, suffix: str, path: str, **extra: str) -> str:
    if not path:
        raise ValueError("a path is required, e.g. /workspace/app.py")
    return _sandbox_path(sandbox_id, suffix) + "?" + urlencode({"path": path, **extra})


def _check_fork(n: int) -> None:
    if not _is_int(n) or not 1 <= n <= MAX_FORK:
        raise ValueError(f"n must be a whole number from 1 to {MAX_FORK}")


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
        """
        self._key = _resolve_api_key(api_key)
        self.base_url = _resolve_base_url(base_url)
        self.timeout = timeout
        self._http = http_client or httpx.Client(timeout=timeout)
        self._own_http = http_client is None
        self._stream = stream

    def __repr__(self) -> str:  # never the key
        return f"Kosa8Client(base_url={self.base_url!r})"

    def close(self) -> None:
        if self._own_http:
            self._http.close()

    def __enter__(self) -> Kosa8Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._key}",
            "Accept": "application/json",
            "User-Agent": "kosa8-integrations",
        }

    def _request(
        self, method: str, path: str, body: Any = None, timeout: float | None = None, *, content: bytes | None = None, raw: bool = False
    ) -> Any:
        headers = self._headers()
        if content is not None:
            headers["Content-Type"] = "application/octet-stream"
        if raw:
            headers["Accept"] = "application/octet-stream"
        try:
            resp = self._http.request(
                method,
                self.base_url + path,
                headers=headers,
                json=body if content is None else None,
                content=content,
                timeout=timeout if timeout is not None else self.timeout,
            )
        except httpx.TimeoutException as e:
            raise Kosa8TimeoutError(f"kosa8 Cloud did not answer {method} {path} in time") from e
        except httpx.HTTPError as e:
            raise Kosa8Error(f"could not reach kosa8 Cloud at {self.base_url}: {e}") from e
        if resp.status_code >= 400:
            raise _error_from_response(resp)
        if raw:
            return resp.content
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
        delete_after: int | None = None,
    ) -> dict[str, Any]:
        """Start a sandbox and return its record once it is running.

        ``ttl_s`` (60 s to 90 days) or ``delete_after`` (Unix ms) deletes it automatically.
        """
        body = _create_body(template, region, sleep_after_s, ttl_s, delete_after)
        r = self._request("POST", "/v1/sandboxes", body, timeout=max(self.timeout, 90.0))
        return r["sandbox"]

    def list_sandboxes(self, *, all: bool = False, limit: int | None = None, cursor: str | None = None) -> SandboxPage:
        """One page of the org's sandboxes, newest first."""
        return _page(self._request("GET", _list_path(all, limit, cursor)))

    def iter_sandboxes(self, *, all: bool = False, page_size: int | None = None) -> Iterator[dict[str, Any]]:
        """Every sandbox, following ``next_cursor``; one page on a server that does not page."""
        cursor: str | None = None
        seen: set[str] = set()
        while True:
            page = self.list_sandboxes(all=all, limit=page_size, cursor=cursor)
            yield from page.sandboxes
            if not page.next_cursor or page.next_cursor in seen:
                return
            seen.add(page.next_cursor)
            cursor = page.next_cursor

    def update_sandbox(
        self,
        sandbox_id: str,
        *,
        ttl_s: int | None = None,
        delete_after: int | None = None,
        sleep_after_s: int | None = None,
        clear_lifetime: bool = False,
    ) -> dict[str, Any]:
        """Change a sandbox's lifetime or sleep timer (PATCH)."""
        body = _update_body(ttl_s, delete_after, sleep_after_s, clear_lifetime)
        return self._request("PATCH", _sandbox_path(sandbox_id), body)["sandbox"]

    # Data plane: may answer 501 (Kosa8NotSupportedError) on current hosts ------

    def read_file(self, sandbox_id: str, path: str) -> bytes:
        """A file's bytes (at most 8 MiB). Data plane: may answer 501 on current hosts."""
        return self._request("GET", _file_path(sandbox_id, "/files", path), raw=True)

    def write_file(self, sandbox_id: str, path: str, data: bytes) -> dict[str, Any]:
        """Create or replace a file (at most 8 MiB). Data plane: may answer 501 on current hosts."""
        return (self._request("PUT", _file_path(sandbox_id, "/files", path), content=bytes(data)) or {}).get("file", {})

    def list_files(self, sandbox_id: str, path: str = DEFAULT_WORKSPACE) -> list[dict[str, Any]]:
        """A folder's entries. Data plane: may answer 501 on current hosts."""
        return (self._request("GET", _file_path(sandbox_id, "/files/list", path)) or {}).get("files", [])

    def create_checkpoint(self, sandbox_id: str, label: str | None = None) -> dict[str, Any]:
        """Save memory, processes and disk. Data plane: may answer 501 on current hosts."""
        body = {"label": label} if label is not None else {}
        return self._request("POST", _sandbox_path(sandbox_id, "/checkpoints"), body)["checkpoint"]

    def list_checkpoints(self, sandbox_id: str) -> list[dict[str, Any]]:
        """Checkpoints, oldest first. Data plane: may answer 501 on current hosts."""
        return (self._request("GET", _sandbox_path(sandbox_id, "/checkpoints")) or {}).get("checkpoints", [])

    def rewind_sandbox(self, sandbox_id: str, checkpoint_id: str) -> dict[str, Any]:
        """Go back to a checkpoint; answers ``{sandbox, checkpoint, backup, ms}``.
        Data plane: may answer 501 on current hosts."""
        return self._request("POST", _sandbox_path(sandbox_id, "/rewind"), {"checkpoint_id": checkpoint_id})

    def fork_sandbox(self, sandbox_id: str, n: int = 1) -> list[dict[str, Any]]:
        """``n`` (1 to 100) copies of the sandbox as it is now. Data plane: may answer 501 on current hosts."""
        _check_fork(n)
        return (self._request("POST", _sandbox_path(sandbox_id, "/fork"), {"n": n}) or {}).get("sandboxes", [])

    def get_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return self._request("GET", _sandbox_path(sandbox_id))["sandbox"]

    def delete_sandbox(self, sandbox_id: str, *, missing_ok: bool = True) -> None:
        try:
            self._request("DELETE", _sandbox_path(sandbox_id))
        except Kosa8Error as e:
            if not (missing_ok and e.status == 404):
                raise

    def sleep_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return self._request("POST", _sandbox_path(sandbox_id, "/sleep"))["sandbox"]

    def wake_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return self._request("POST", _sandbox_path(sandbox_id, "/wake"))["sandbox"]

    def list_templates(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v1/templates")["templates"]

    # Exec --------------------------------------------------------------------

    def exec(self, sandbox_id: str, request: ExecRequest) -> ExecOutput:
        """Run argv in the sandbox and wait for it."""
        if self._stream is not False:
            try:
                return self._exec_stream(sandbox_id, request)
            except Kosa8Error as e:
                if self._stream is True or e.status != 501:
                    raise
                self._stream = False  # this host has no streamed exec; stop asking
        body = _exec_body(request.argv, request.env, stream=False, image=request.image)
        wait = BUFFERED_REQUEST_TIMEOUT_S
        if request.timeout is not None:
            wait = max(wait, request.timeout + 15.0)
        return _buffered_output(self._request("POST", _sandbox_path(sandbox_id, "/exec"), body, timeout=wait))

    def _exec_stream(self, sandbox_id: str, request: ExecRequest) -> ExecOutput:
        body = _exec_body(request.argv, request.env, stream=True, image=request.image)
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
    """The asynchronous twin of :class:`Kosa8Client`."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = 60.0,
        http_client: httpx.AsyncClient | None = None,
        stream: bool | None = None,
    ) -> None:
        self._key = _resolve_api_key(api_key)
        self.base_url = _resolve_base_url(base_url)
        self.timeout = timeout
        self._http = http_client or httpx.AsyncClient(timeout=timeout)
        self._own_http = http_client is None
        self._stream = stream

    def __repr__(self) -> str:  # never the key
        return f"AsyncKosa8Client(base_url={self.base_url!r})"

    async def aclose(self) -> None:
        if self._own_http:
            await self._http.aclose()

    async def __aenter__(self) -> AsyncKosa8Client:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._key}",
            "Accept": "application/json",
            "User-Agent": "kosa8-integrations",
        }

    async def _request(
        self, method: str, path: str, body: Any = None, timeout: float | None = None, *, content: bytes | None = None, raw: bool = False
    ) -> Any:
        headers = self._headers()
        if content is not None:
            headers["Content-Type"] = "application/octet-stream"
        if raw:
            headers["Accept"] = "application/octet-stream"
        try:
            resp = await self._http.request(
                method,
                self.base_url + path,
                headers=headers,
                json=body if content is None else None,
                content=content,
                timeout=timeout if timeout is not None else self.timeout,
            )
        except httpx.TimeoutException as e:
            raise Kosa8TimeoutError(f"kosa8 Cloud did not answer {method} {path} in time") from e
        except httpx.HTTPError as e:
            raise Kosa8Error(f"could not reach kosa8 Cloud at {self.base_url}: {e}") from e
        if resp.status_code >= 400:
            raise _error_from_response(resp)
        if raw:
            return resp.content
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError as e:
            raise Kosa8Error(f"kosa8 Cloud answered {method} {path} with a body that is not JSON") from e

    async def create_sandbox(
        self,
        *,
        template: str | None = None,
        region: str | None = None,
        sleep_after_s: int | None = None,
        ttl_s: int | None = None,
        delete_after: int | None = None,
    ) -> dict[str, Any]:
        body = _create_body(template, region, sleep_after_s, ttl_s, delete_after)
        r = await self._request("POST", "/v1/sandboxes", body, timeout=max(self.timeout, 90.0))
        return r["sandbox"]

    async def list_sandboxes(self, *, all: bool = False, limit: int | None = None, cursor: str | None = None) -> SandboxPage:
        return _page(await self._request("GET", _list_path(all, limit, cursor)))

    async def iter_sandboxes(self, *, all: bool = False, page_size: int | None = None) -> AsyncIterator[dict[str, Any]]:
        cursor: str | None = None
        seen: set[str] = set()
        while True:
            page = await self.list_sandboxes(all=all, limit=page_size, cursor=cursor)
            for sb in page.sandboxes:
                yield sb
            if not page.next_cursor or page.next_cursor in seen:
                return
            seen.add(page.next_cursor)
            cursor = page.next_cursor

    async def update_sandbox(
        self,
        sandbox_id: str,
        *,
        ttl_s: int | None = None,
        delete_after: int | None = None,
        sleep_after_s: int | None = None,
        clear_lifetime: bool = False,
    ) -> dict[str, Any]:
        body = _update_body(ttl_s, delete_after, sleep_after_s, clear_lifetime)
        return (await self._request("PATCH", _sandbox_path(sandbox_id), body))["sandbox"]

    # Data plane: may answer 501 (Kosa8NotSupportedError) on current hosts ------

    async def read_file(self, sandbox_id: str, path: str) -> bytes:
        return await self._request("GET", _file_path(sandbox_id, "/files", path), raw=True)

    async def write_file(self, sandbox_id: str, path: str, data: bytes) -> dict[str, Any]:
        return (await self._request("PUT", _file_path(sandbox_id, "/files", path), content=bytes(data)) or {}).get("file", {})

    async def list_files(self, sandbox_id: str, path: str = DEFAULT_WORKSPACE) -> list[dict[str, Any]]:
        return (await self._request("GET", _file_path(sandbox_id, "/files/list", path)) or {}).get("files", [])

    async def create_checkpoint(self, sandbox_id: str, label: str | None = None) -> dict[str, Any]:
        body = {"label": label} if label is not None else {}
        return (await self._request("POST", _sandbox_path(sandbox_id, "/checkpoints"), body))["checkpoint"]

    async def list_checkpoints(self, sandbox_id: str) -> list[dict[str, Any]]:
        return (await self._request("GET", _sandbox_path(sandbox_id, "/checkpoints")) or {}).get("checkpoints", [])

    async def rewind_sandbox(self, sandbox_id: str, checkpoint_id: str) -> dict[str, Any]:
        return await self._request("POST", _sandbox_path(sandbox_id, "/rewind"), {"checkpoint_id": checkpoint_id})

    async def fork_sandbox(self, sandbox_id: str, n: int = 1) -> list[dict[str, Any]]:
        _check_fork(n)
        return (await self._request("POST", _sandbox_path(sandbox_id, "/fork"), {"n": n}) or {}).get("sandboxes", [])

    async def get_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return (await self._request("GET", _sandbox_path(sandbox_id)))["sandbox"]

    async def delete_sandbox(self, sandbox_id: str, *, missing_ok: bool = True) -> None:
        try:
            await self._request("DELETE", _sandbox_path(sandbox_id))
        except Kosa8Error as e:
            if not (missing_ok and e.status == 404):
                raise

    async def sleep_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return (await self._request("POST", _sandbox_path(sandbox_id, "/sleep")))["sandbox"]

    async def wake_sandbox(self, sandbox_id: str) -> dict[str, Any]:
        return (await self._request("POST", _sandbox_path(sandbox_id, "/wake")))["sandbox"]

    async def list_templates(self) -> list[dict[str, Any]]:
        return (await self._request("GET", "/v1/templates"))["templates"]

    async def exec(self, sandbox_id: str, request: ExecRequest) -> ExecOutput:
        if self._stream is not False:
            try:
                return await self._exec_stream(sandbox_id, request)
            except Kosa8Error as e:
                if self._stream is True or e.status != 501:
                    raise
                self._stream = False
        body = _exec_body(request.argv, request.env, stream=False, image=request.image)
        wait = BUFFERED_REQUEST_TIMEOUT_S
        if request.timeout is not None:
            wait = max(wait, request.timeout + 15.0)
        return _buffered_output(await self._request("POST", _sandbox_path(sandbox_id, "/exec"), body, timeout=wait))

    async def _exec_stream(self, sandbox_id: str, request: ExecRequest) -> ExecOutput:
        body = _exec_body(request.argv, request.env, stream=True, image=request.image)
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
    "k8p(){ t=$1; d=$2; [ -L \"$d\" ] && return 0; "
    "if [ ! -d \"$t\" ]; then mkdir -p \"${t%/*}\" 2>/dev/null; "
    "if mkdir \"$t\" 2>/dev/null; then [ -d \"$d\" ] && cp -a \"$d\"/. \"$t\"/ 2>/dev/null; "
    "case \"$d\" in /tmp|/var/tmp) chmod 1777 \"$t\" 2>/dev/null;; esac; fi; fi; "
    "mount --bind \"$t\" \"$d\" 2>/dev/null && return 0; "
    "rm -rf \"$d\" 2>/dev/null; ln -s \"$t\" \"$d\" 2>/dev/null; return 0; }; "
)

# Runs K8_CMD (or the file K8_CMDFILE) with K8_SHELL in K8_CWD, stdin from
# K8_STDIN (base64) or K8_STDINFILE, under /usr/bin/timeout when K8_TIMEOUT
# is set. The command never sees the K8_* variables.
_RUN_SCRIPT = (
    "c=$K8_CMD; cf=$K8_CMDFILE; w=$K8_CWD; t=$K8_TIMEOUT; i=$K8_STDIN; inf=$K8_STDINFILE; s=$K8_SHELL; "
    "unset K8_CMD K8_CMDFILE K8_CWD K8_TIMEOUT K8_STDIN K8_STDINFILE K8_SHELL; "
    "if [ -n \"$cf\" ]; then c=$(cat \"$cf\") || exit 126; rm -f \"$cf\"; fi; "
    "if [ -z \"$s\" ] || [ \"$s\" = auto ]; then if [ -x /bin/bash ]; then s=/bin/bash; else s=/bin/sh; fi; fi; "
    "if [ -n \"$w\" ]; then cd \"$w\" 2>/dev/null || { echo \"kosa8: cannot change to directory $w\" >&2; exit 126; }; fi; "
    "T=; if [ -n \"$t\" ]; then for x in /usr/bin/timeout /bin/timeout; do if [ -x \"$x\" ]; then T=$x; break; fi; done; fi; "
    "k8r(){ if [ -n \"$T\" ]; then \"$T\" -s KILL \"$t\" \"$@\"; else \"$@\"; fi; }; "
    "if [ -n \"$inf\" ]; then k8r \"$s\" -c \"$c\" < \"$inf\"; r=$?; rm -f \"$inf\"; exit $r; "
    "elif [ -n \"$i\" ]; then printf %s \"$i\" | base64 -d | k8r \"$s\" -c \"$c\"; "
    "else k8r \"$s\" -c \"$c\" < /dev/null; fi"
)

# $1 path, $2 w (truncate) or a (append), data in K8_DATA. Errors go to stderr
# as one word: K8ERR:<code>.
_UPLOAD_SCRIPT = (
    "p=$1; m=$2; d=$K8_DATA; unset K8_DATA; "
    "if [ \"$m\" = w ]; then "
    "[ -d \"$p\" ] && { echo K8ERR:is_directory >&2; exit 21; }; "
    "dir=${p%/*}; [ -z \"$dir\" ] && dir=/; "
    "if [ ! -d \"$dir\" ]; then mkdir -p \"$dir\" 2>/dev/null || { "
    "if [ -e \"$dir\" ]; then echo K8ERR:invalid_path >&2; else echo K8ERR:permission_denied >&2; fi; exit 22; }; fi; "
    "( : > \"$p\" ) 2>/dev/null || { echo K8ERR:permission_denied >&2; exit 23; }; fi; "
    "[ -z \"$d\" ] && exit 0; "
    "command -v base64 >/dev/null 2>&1 || { echo K8ERR:base64_missing >&2; exit 24; }; "
    "printf %s \"$d\" | base64 -d >> \"$p\" || { echo K8ERR:write_failed >&2; exit 25; }"
)

# $1 path, $2 chunk size, $3 chunk index. Chunk 0 starts with a status line.
_DOWNLOAD_SCRIPT = (
    "p=$1; c=$2; n=$3; "
    "if [ \"$n\" = 0 ]; then "
    "if [ -d \"$p\" ]; then echo K8:is_directory; exit 0; fi; "
    "if [ ! -e \"$p\" ]; then echo K8:file_not_found; exit 0; fi; "
    "if [ ! -r \"$p\" ]; then echo K8:permission_denied; exit 0; fi; "
    "z=$(wc -c < \"$p\" 2>/dev/null) || { echo K8:permission_denied; exit 0; }; "
    "command -v base64 >/dev/null 2>&1 || { echo K8:base64_missing; exit 0; }; "
    "echo K8:ok $z; fi; "
    "dd if=\"$p\" bs=\"$c\" skip=\"$n\" count=1 2>/dev/null | base64"
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
Op = Generator[ExecRequest, ExecOutput, T]


def drive(op: Op[T], send: Callable[[ExecRequest], ExecOutput]) -> T:
    """Run an operation, sending each exec request it yields synchronously."""
    try:
        req = next(op)
        while True:
            req = op.send(send(req))
    except StopIteration as stop:
        return stop.value


async def adrive(op: Op[T], send: Callable[[ExecRequest], Any]) -> T:
    """Run an operation, awaiting each exec request it yields."""
    try:
        req = next(op)
        while True:
            req = op.send(await send(req))
    except StopIteration as stop:
        return stop.value


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
    image: Optional[str] = None
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

    def upload_op(self, path: str, data: bytes) -> Op[Optional[str]]:
        """Write data to an absolute path, creating parent directories.

        Returns None on success or an error code (``permission_denied``,
        ``is_directory``, ``invalid_path``, or a description).
        """
        if not path.startswith("/"):
            return _const("invalid_path")
        return self._upload_gen(path, data)

    def _upload_gen(self, path: str, data: bytes) -> Op[Optional[str]]:
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
            err = yield from self._upload_gen(name, stdin or b"")
            if err:
                raise Kosa8Error(f"could not pass stdin to the command: {err}")
            del k8["K8_STDIN"]
            k8["K8_STDINFILE"] = name
        if size() > self.body_limit:
            name = f"{spill}/cmd-{uuid.uuid4().hex}"
            err = yield from self._upload_gen(name, command.encode("utf-8"))
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


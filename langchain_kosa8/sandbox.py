"""kosa8 Cloud sandbox backend for Deep Agents."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from deepagents.backends.protocol import (
    ExecuteResponse,
    FileDownloadResponse,
    FileUploadResponse,
    GrepResult,
)
from deepagents.backends.sandbox import BaseSandbox

from langchain_kosa8._grep import PROBE_CMD, build_portable_grep_cmd, parse_grep_output

from langchain_kosa8._kosa8 import (
    DEFAULT_PERSIST_DIRS,
    DEFAULT_WORKSPACE,
    AsyncKosa8Client,
    ExecOutput,
    Kosa8Client,
    Kosa8Error,
    Kosa8TimeoutError,
    SandboxSession,
    adrive,
    drive,
)

DEFAULT_TEMPLATE = "python"
"""BaseSandbox's file tools need `python3` in the image; the `python` template has it."""

TIMEOUT_EXIT_CODE = 124
ASYNC_GREP_TIMEOUT = 60


def _format_output(out: ExecOutput) -> str:
    output = out.stdout or ""
    if out.stderr and out.stderr.strip():
        output += f"\n<stderr>{out.stderr.strip()}</stderr>"
    return output


def _timeout_response(timeout: int | None, partial: ExecOutput | None = None, reason: str | None = None) -> ExecuteResponse:
    output = f"Command timed out after {timeout} seconds" if timeout else "Command timed out"
    if reason:
        output += f" ({reason})"
    if partial is not None:
        extra = _format_output(partial)
        if extra:
            output += f"\n{extra}"
    return ExecuteResponse(output=output, exit_code=TIMEOUT_EXIT_CODE, truncated=False)


_HOST_LIMIT_REASON = (
    "kosa8 Cloud ended the request; a buffered exec must finish within about "
    "30 s on hosts without streamed exec"
)


class Kosa8Sandbox(BaseSandbox):
    """A kosa8 Cloud sandbox as a Deep Agents backend.

    Implements `execute()`, `upload_files()`, `download_files()`, `id` and
    their async variants; `BaseSandbox` builds `ls`, `read`, `write`, `edit`,
    `glob` and `grep` on top of them.

    Each command runs in a fresh container over the sandbox's persistent
    `/workspace` disk. Commands start in `/workspace`, and `/tmp` is kept on
    that disk too (see `persist_dirs`). File transfer goes through exec, in
    base64 chunks, because the kosa8 Cloud API has no file endpoints on
    production hosts yet.

    Example:
        ```python
        from langchain_kosa8 import Kosa8Sandbox

        backend = Kosa8Sandbox.create()  # KOSA8_API_KEY from the environment
        try:
            print(backend.execute("python3 --version").output)
        finally:
            backend.delete()
        ```
    """

    def __init__(
        self,
        *,
        sandbox_id: str,
        api_key: str | None = None,
        base_url: str | None = None,
        client: Kosa8Client | None = None,
        async_client: AsyncKosa8Client | None = None,
        timeout: int = 30 * 60,
        workspace: str = DEFAULT_WORKSPACE,
        persist_dirs: Sequence[str] = DEFAULT_PERSIST_DIRS,
        shell: str = "auto",
        stream: bool | None = None,
    ) -> None:
        """Wrap an existing kosa8 Cloud sandbox.

        Args:
            sandbox_id: The sandbox id (`sbx_...`).
            api_key: kosa8 Cloud API key. Defaults to `KOSA8_API_KEY`.
            base_url: API address. Defaults to `KOSA8_CLOUD_URL` or
                `https://api.kosa8.com`.
            client: A `Kosa8Client` to use for sync calls.
            async_client: An `AsyncKosa8Client` to use for async calls.
            timeout: Default command timeout in seconds when `execute()` is
                called without one. `0` means no timeout.
            workspace: Directory commands start in. On kosa8 Cloud only this
                disk persists between commands.
            persist_dirs: Directories outside the workspace to keep on the
                workspace disk between commands. Defaults to `("/tmp",)`.
            shell: Shell that runs each command: `"auto"` (bash if the image
                has it, else sh) or an absolute path.
            stream: `None` to use streamed exec when the host offers it,
                `False` for buffered exec only, `True` to require streaming.
        """
        self._sandbox_id = sandbox_id
        self._api_key = api_key
        self._base_url = base_url
        self._stream = stream
        self._client = client
        self._async_client = async_client
        self._default_timeout = timeout
        self._session = SandboxSession(workspace=workspace, persist_dirs=tuple(persist_dirs), shell=shell)
        self._native_grep: bool | None = None

    # Construction ----------------------------------------------------------

    @classmethod
    def create(
        cls,
        *,
        template: str = DEFAULT_TEMPLATE,
        region: str | None = None,
        sleep_after_s: int | None = None,
        ttl_s: int | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        client: Kosa8Client | None = None,
        **kwargs: Any,
    ) -> Kosa8Sandbox:
        """Start a new kosa8 Cloud sandbox and wrap it.

        Args:
            template: Template key (see `GET /v1/templates`). Defaults to
                `python`, which has the `python3` that `BaseSandbox` needs.
            region: Region id such as `eu-hel1`, or `None` for the org default.
            sleep_after_s: Idle seconds before the sandbox sleeps (0 = never).
            ttl_s: Seconds until kosa8 Cloud deletes the sandbox.
            api_key: kosa8 Cloud API key. Defaults to `KOSA8_API_KEY`.
            base_url: API address.
            client: A `Kosa8Client` to use.
            **kwargs: Passed to the constructor.
        """
        stream = kwargs.get("stream")
        client = client or Kosa8Client(api_key, base_url=base_url, stream=stream)
        box = client.create_sandbox(template=template, region=region, sleep_after_s=sleep_after_s, ttl_s=ttl_s)
        return cls(sandbox_id=box["id"], api_key=api_key, base_url=base_url, client=client, **kwargs)

    @classmethod
    async def acreate(
        cls,
        *,
        template: str = DEFAULT_TEMPLATE,
        region: str | None = None,
        sleep_after_s: int | None = None,
        ttl_s: int | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        async_client: AsyncKosa8Client | None = None,
        **kwargs: Any,
    ) -> Kosa8Sandbox:
        """Async version of `create()`."""
        stream = kwargs.get("stream")
        async_client = async_client or AsyncKosa8Client(api_key, base_url=base_url, stream=stream)
        box = await async_client.create_sandbox(template=template, region=region, sleep_after_s=sleep_after_s, ttl_s=ttl_s)
        return cls(sandbox_id=box["id"], api_key=api_key, base_url=base_url, async_client=async_client, **kwargs)

    @property
    def client(self) -> Kosa8Client:
        if self._client is None:
            self._client = Kosa8Client(self._api_key, base_url=self._base_url, stream=self._stream)
        return self._client

    @property
    def async_client(self) -> AsyncKosa8Client:
        if self._async_client is None:
            self._async_client = AsyncKosa8Client(self._api_key, base_url=self._base_url, stream=self._stream)
        return self._async_client

    @property
    def id(self) -> str:
        """The kosa8 sandbox id."""
        return self._sandbox_id

    # Execute ---------------------------------------------------------------

    def _effective_timeout(self, timeout: int | None) -> int | None:
        t = self._default_timeout if timeout is None else timeout
        return t if t and t > 0 else None

    def _response(self, out: ExecOutput, timeout: int | None) -> ExecuteResponse:
        if out.timed_out:
            return _timeout_response(timeout, out)
        return ExecuteResponse(output=_format_output(out), exit_code=out.exit_code, truncated=out.truncated)

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        """Run a shell command in the sandbox.

        Args:
            command: Shell command string.
            timeout: Seconds to wait. `None` uses the backend default; `0`
                means no timeout. Enforced in the sandbox with `timeout`.

        Returns:
            `ExecuteResponse` with stdout, then stderr in `<stderr>` tags, and
            the exit code. A timeout answers exit code 124.
        """
        t = self._effective_timeout(timeout)
        op = self._session.command_op(command, timeout=t)
        try:
            out = drive(op, lambda req: self.client.exec(self._sandbox_id, req))
        except Kosa8TimeoutError:
            return _timeout_response(t, reason=_HOST_LIMIT_REASON)
        return self._response(out, t)

    async def aexecute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:  # noqa: ASYNC109
        """Async version of `execute()`."""
        t = self._effective_timeout(timeout)
        op = self._session.command_op(command, timeout=t)
        try:
            out = await adrive(op, lambda req: self.async_client.exec(self._sandbox_id, req))
        except Kosa8TimeoutError:
            return _timeout_response(t, reason=_HOST_LIMIT_REASON)
        return self._response(out, t)

    # Files -----------------------------------------------------------------

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        """Upload files into the sandbox, creating parent directories.

        Paths must be absolute. Errors are reported per file.
        """
        responses: list[FileUploadResponse] = []
        for path, content in files:
            try:
                err = drive(self._session.upload_op(path, content), lambda req: self.client.exec(self._sandbox_id, req))
            except Kosa8Error as e:
                err = f"upload failed: {e}"
            responses.append(FileUploadResponse(path=path, error=err))
        return responses

    async def aupload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        """Async version of `upload_files()`."""
        responses: list[FileUploadResponse] = []
        for path, content in files:
            try:
                err = await adrive(self._session.upload_op(path, content), lambda req: self.async_client.exec(self._sandbox_id, req))
            except Kosa8Error as e:
                err = f"upload failed: {e}"
            responses.append(FileUploadResponse(path=path, error=err))
        return responses

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        """Download files from the sandbox. Errors are reported per file."""
        responses: list[FileDownloadResponse] = []
        for path in paths:
            try:
                got = drive(self._session.download_op(path), lambda req: self.client.exec(self._sandbox_id, req))
                responses.append(FileDownloadResponse(path=path, content=got.content, error=got.error))
            except Kosa8Error as e:
                responses.append(FileDownloadResponse(path=path, content=None, error=f"download failed: {e}"))
        return responses

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        """Async version of `download_files()`."""
        responses: list[FileDownloadResponse] = []
        for path in paths:
            try:
                got = await adrive(self._session.download_op(path), lambda req: self.async_client.exec(self._sandbox_id, req))
                responses.append(FileDownloadResponse(path=path, content=got.content, error=got.error))
            except Kosa8Error as e:
                responses.append(FileDownloadResponse(path=path, content=None, error=f"download failed: {e}"))
        return responses

    # Search ----------------------------------------------------------------

    def grep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
        *,
        max_count: int | None = None,
    ) -> GrepResult:
        """Search file contents for a literal string.

        Uses `BaseSandbox.grep()` (ripgrep or GNU grep) when the image has
        them, and a Python 3 search with the same results otherwise (for
        example on Alpine images, whose busybox grep lacks `-Z`).
        """
        if (glob and "/" in glob) or self._grep_is_native():
            return super().grep(pattern, path, glob, max_count=max_count)
        cmd = build_portable_grep_cmd(pattern, path, glob, max_count, self.grep_exclude_dirs)
        r = self.execute(cmd)
        return parse_grep_output(r.output, r.exit_code, path, max_count)

    async def agrep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
        *,
        max_count: int | None = None,
    ) -> GrepResult:
        """Async version of `grep()`."""
        if (glob and "/" in glob) or await self._agrep_is_native():
            return await super().agrep(pattern, path, glob, max_count=max_count)
        cmd = build_portable_grep_cmd(pattern, path, glob, max_count, self.grep_exclude_dirs)
        try:
            r = await asyncio.wait_for(self.aexecute(cmd), timeout=ASYNC_GREP_TIMEOUT)
        except TimeoutError:
            return GrepResult(error=f"Error: grep timed out after {ASYNC_GREP_TIMEOUT}s. Try a more specific pattern or a narrower path.")
        return parse_grep_output(r.output, r.exit_code, path, max_count)

    def _grep_is_native(self) -> bool:
        if self._native_grep is None:
            self._native_grep = "k8-native" in self.execute(PROBE_CMD, timeout=30).output
        return self._native_grep

    async def _agrep_is_native(self) -> bool:
        if self._native_grep is None:
            self._native_grep = "k8-native" in (await self.aexecute(PROBE_CMD, timeout=30)).output
        return self._native_grep

    # Lifecycle -------------------------------------------------------------

    def sleep(self) -> None:
        """Snapshot the sandbox and free its VM; the next command wakes it."""
        self.client.sleep_sandbox(self._sandbox_id)

    def wake(self) -> None:
        """Restore a sleeping sandbox now rather than on the next command."""
        self.client.wake_sandbox(self._sandbox_id)

    def delete(self) -> None:
        """Delete the sandbox. A sandbox that is already gone is not an error."""
        self.client.delete_sandbox(self._sandbox_id)

    async def asleep(self) -> None:
        await self.async_client.sleep_sandbox(self._sandbox_id)

    async def awake(self) -> None:
        await self.async_client.wake_sandbox(self._sandbox_id)

    async def adelete(self) -> None:
        await self.async_client.delete_sandbox(self._sandbox_id)

    def close(self) -> None:
        """Close HTTP connections (does not delete the sandbox)."""
        if self._client is not None:
            self._client.close()

    async def aclose(self) -> None:
        if self._async_client is not None:
            await self._async_client.aclose()

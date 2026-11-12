"""LangChain's standard sandbox suite, run against the fake kosa8 Cloud API.

The fake API runs each exec for real, so this checks the integration's
command, upload and download wrappers against `SandboxIntegrationTests`
without kosa8 Cloud. `BaseSandbox` expects Linux userland tools, so it runs:

* in a local Linux container when ``KOSA8_TEST_CONTAINER`` names one, for
  example ``kosa8 run -d --name k8test python:3.12-alpine sleep 3600`` (the
  same image as kosa8 Cloud's `python` template; ``KOSA8_TEST_ENGINE`` picks
  the CLI, default ``kosa8``, or ``docker``);
* else directly on the machine, when it is Linux;
* and is skipped otherwise.

The run against the real service is in `tests/integration_tests`.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator

import pytest
from deepagents.backends.protocol import SandboxBackendProtocol

from langchain_kosa8 import AsyncKosa8Client, Kosa8Client, Kosa8Sandbox
from tests.standard_overrides import StandardOverrides
from tests.fake_cloud import FakeCloud, container_runner

CONTAINER = os.environ.get("KOSA8_TEST_CONTAINER")

pytestmark = pytest.mark.skipif(
    not CONTAINER and not sys.platform.startswith("linux"),
    reason="needs Linux userland: set KOSA8_TEST_CONTAINER to a running Linux container",
)


class TestKosa8SandboxAgainstFakeCloud(StandardOverrides):
    _root: str = ""

    @property
    def sandbox_root_dir(self) -> str:
        return type(self)._root + "/test_sandbox_ops/"

    @pytest.fixture(scope="class")
    def sandbox(self, tmp_path_factory: pytest.TempPathFactory) -> Iterator[SandboxBackendProtocol]:
        if CONTAINER:
            cloud = FakeCloud(runner=container_runner(CONTAINER, os.environ.get("KOSA8_TEST_ENGINE", "kosa8")))
            ws, persist = "/workspace", ("/tmp",)
            cloud.runner(["/bin/sh", "-c", "mkdir -p /workspace && rm -rf /workspace/test_sandbox_ops"], {}, 60)  # type: ignore[misc]
        else:
            cloud = FakeCloud()
            ws, persist = str(tmp_path_factory.mktemp("workspace").resolve()), ()
        type(self)._root = ws
        client = Kosa8Client(api_key=cloud.api_key, http_client=cloud.client())
        backend = Kosa8Sandbox.create(client=client, workspace=ws, persist_dirs=persist)
        backend._async_client = AsyncKosa8Client(api_key=cloud.api_key, http_client=cloud.async_client())  # noqa: SLF001
        try:
            yield backend
        finally:
            backend.delete()

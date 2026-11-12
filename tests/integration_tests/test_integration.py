"""LangChain's standard sandbox suite against kosa8 Cloud.

Skipped unless KOSA8_API_KEY is set. Creates one sandbox (template from
KOSA8_TEMPLATE, default `python`) and deletes it at the end.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from deepagents.backends.protocol import SandboxBackendProtocol

from tests.standard_overrides import StandardOverrides
from langchain_kosa8 import Kosa8Sandbox

pytestmark = pytest.mark.skipif(not os.environ.get("KOSA8_API_KEY"), reason="KOSA8_API_KEY is not set")


class TestKosa8SandboxStandard(StandardOverrides):
    @property
    def sandbox_root_dir(self) -> str:
        # On kosa8 Cloud each command is a fresh container over the sandbox's
        # /workspace disk, so the suite works there.
        return "/workspace/test_sandbox_ops/"

    @pytest.fixture(scope="class")
    def sandbox(self) -> Iterator[SandboxBackendProtocol]:
        backend = Kosa8Sandbox.create(template=os.environ.get("KOSA8_TEMPLATE", "python"), ttl_s=3600)
        try:
            yield backend
        finally:
            backend.delete()

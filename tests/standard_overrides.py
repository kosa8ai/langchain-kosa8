"""Expected failures shared by the standard-suite runs.

`langchain-tests` 1.1.9 and `deepagents` 0.7.35 (the newest releases when
this package was written) disagree on two behaviours, so these standard
tests fail for every `BaseSandbox` backend, not only this one:

* `glob()` in deepagents returns absolute paths; the tests expect paths
  relative to the search root.
* `write()` in deepagents overwrites an existing file; the test expects an
  error.

They are marked `xfail(strict=False)`, so they pass again once the two
packages agree. Commands in kosa8 Cloud template images may run as root, and root
can read a `chmod 000` file, so the permission test is an expected failure
there too.
"""

from __future__ import annotations

import pytest
from deepagents.backends.protocol import SandboxBackendProtocol
from langchain_tests.integration_tests import SandboxIntegrationTests

_GLOB = "langchain-tests 1.1.9 expects relative glob paths; deepagents 0.7.35 returns absolute paths"
_WRITE = "langchain-tests 1.1.9 expects write() to refuse an existing file; deepagents 0.7.35 overwrites it"
_ROOT = "commands may run as root, and root can read a chmod 000 file"


class StandardOverrides(SandboxIntegrationTests):
    @pytest.mark.xfail(reason=_GLOB, strict=False)
    def test_glob(self, sandbox_backend: SandboxBackendProtocol, sandbox_test_root: str) -> None:
        super().test_glob(sandbox_backend, sandbox_test_root)

    @pytest.mark.xfail(reason=_GLOB, strict=False)
    def test_glob_basic_pattern(self, sandbox_backend: SandboxBackendProtocol, sandbox_test_root: str) -> None:
        super().test_glob_basic_pattern(sandbox_backend, sandbox_test_root)

    @pytest.mark.xfail(reason=_GLOB, strict=False)
    def test_glob_with_directories(self, sandbox_backend: SandboxBackendProtocol, sandbox_test_root: str) -> None:
        super().test_glob_with_directories(sandbox_backend, sandbox_test_root)

    @pytest.mark.xfail(reason=_GLOB, strict=False)
    def test_glob_hidden_files_explicitly(self, sandbox_backend: SandboxBackendProtocol, sandbox_test_root: str) -> None:
        super().test_glob_hidden_files_explicitly(sandbox_backend, sandbox_test_root)

    @pytest.mark.xfail(reason=_GLOB, strict=False)
    def test_glob_with_character_class(self, sandbox_backend: SandboxBackendProtocol, sandbox_test_root: str) -> None:
        super().test_glob_with_character_class(sandbox_backend, sandbox_test_root)

    @pytest.mark.xfail(reason=_GLOB, strict=False)
    def test_glob_with_question_mark(self, sandbox_backend: SandboxBackendProtocol, sandbox_test_root: str) -> None:
        super().test_glob_with_question_mark(sandbox_backend, sandbox_test_root)

    @pytest.mark.xfail(reason=_WRITE, strict=False)
    def test_write_existing_file_fails(self, sandbox_backend: SandboxBackendProtocol, sandbox_test_root: str) -> None:
        super().test_write_existing_file_fails(sandbox_backend, sandbox_test_root)

    @pytest.mark.xfail(reason=_ROOT, strict=False)
    def test_download_error_permission_denied(self, sandbox_backend: SandboxBackendProtocol, sandbox_test_root: str) -> None:
        super().test_download_error_permission_denied(sandbox_backend, sandbox_test_root)

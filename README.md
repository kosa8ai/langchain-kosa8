# langchain-kosa8

[kosa8 Cloud](https://kosa8.com) sandboxes as a [Deep Agents](https://docs.langchain.com/oss/python/deepagents/sandboxes) backend.

`Kosa8Sandbox` subclasses Deep Agents' `BaseSandbox`. It implements `execute()`, `upload_files()`, `download_files()`, `id` and their async variants against the kosa8 Cloud API. `BaseSandbox` then provides the agent's file tools (`ls`, `read`, `write`, `edit`, `glob`, `grep`) on top of them.

## Install

```bash
pip install langchain-kosa8
```

```bash
uv add langchain-kosa8
```

You need a kosa8 Cloud API key. Create one in the kosa8 Cloud dashboard and export it:

```bash
export KOSA8_API_KEY=k8c_live_...
```

## Quick start

```python
from langchain_kosa8 import Kosa8Sandbox

backend = Kosa8Sandbox.create()  # template "python", key from KOSA8_API_KEY
try:
    result = backend.execute("python3 -c 'print(6 * 7)'")
    print(result.output, result.exit_code)
finally:
    backend.delete_sandbox()
```

To wrap a sandbox you already started (for example with the [kosa8 Python SDK](https://github.com/kosa8ai/kosa8-py) or the dashboard):

```python
backend = Kosa8Sandbox(sandbox_id="sbx_...")
```

## Use with Deep Agents

```python
from langchain_anthropic import ChatAnthropic
from deepagents import create_deep_agent

from langchain_kosa8 import Kosa8Sandbox

backend = Kosa8Sandbox.create(ttl_s=3600)  # deleted by kosa8 Cloud after an hour at the latest

agent = create_deep_agent(
    model=ChatAnthropic(model="claude-sonnet-4-6"),
    system_prompt="You are a coding assistant with sandbox access.",
    backend=backend,
)

result = agent.invoke({"messages": [{"role": "user", "content": "Write a hello world Python script and run it"}]})

backend.delete_sandbox()
```

Async code can use `await Kosa8Sandbox.acreate(...)`, `aexecute()`, `aupload_files()`, `adownload_files()` and `adelete_sandbox()`.

## Options

`Kosa8Sandbox.create()` takes:

| Argument | Default | Meaning |
|---|---|---|
| `template` | `"python"` | Template key (`GET /v1/templates` lists what your org can use). `BaseSandbox`'s file tools need `python3`, which the `python` template has. |
| `region` | org default | Region id, such as `eu-hel1`. |
| `sleep_after_s` | org default | Idle seconds before the sandbox sleeps (`0` = never). The next command wakes it. |
| `ttl_s` | none | Seconds until kosa8 Cloud deletes the sandbox. |
| `api_key` | `KOSA8_API_KEY` | API key. |
| `base_url` | `KOSA8_CLOUD_URL` or `https://api.kosa8.com` | API address. |

and passes the rest to the constructor:

| Argument | Default | Meaning |
|---|---|---|
| `timeout` | `1800` | Default command timeout in seconds; `0` means none. Enforced inside the sandbox with `timeout -s KILL` and by the API's own `timeout_s`. A timeout answers exit code `124`. |
| `workspace` | `/workspace` | Directory commands start in. |
| `persist_dirs` | `("/tmp",)` | Directories outside the workspace kept on the workspace disk, for hosts that start each command in a fresh container (see below). |
| `shell` | `"auto"` | Shell for each command: bash when the image has it, else `/bin/sh`. |
| `stream` | `None` | `None`: use streamed exec when the host offers it, else buffered. `False`: buffered only. `True`: require streamed exec. |
| `files_api` | `True` | Move files under `/workspace` through the kosa8 files API; `False` sends every file through exec. |

Lifecycle helpers: `sleep()`, `wake()`, `delete_sandbox()`, `close()` (and `asleep()`, `awake()`, `adelete_sandbox()`, `aclose()`). `delete()` and `adelete()` are Deep Agents' file deletes (`BaseSandbox` runs `rm -rf` in the sandbox), not the sandbox's.

## How it works, and the limits that follow

The package talks to the kosa8 Cloud REST API with `httpx`: create, exec (buffered and streamed), the files API, sleep, wake and delete.

- **State between commands.** Commands run in the sandbox's own container, so files written anywhere carry from one command to the next and through sleep and wake. Commands start in `/workspace`, so relative paths land there. Shell state (`cd`, exported variables) does not carry between commands, as with other `BaseSandbox` backends. For hosts that start each command in a fresh container, `/tmp` is also bind-mounted or linked onto `/workspace/.kosa8-persist/tmp` (`persist_dirs`); on a long-lived container this is a no-op after the first command.
- **Files.** `upload_files()` and `download_files()` use the kosa8 files API for files under `/workspace` of up to 8 MiB. Larger files, files elsewhere, hosts without the files API, and every error (so that Deep Agents' `file_not_found`, `is_directory`, `permission_denied` and `invalid_path` codes are exact) go through exec as base64 in chunks: about 36 KB per upload request (the API accepts 64 KiB of JSON per request) and 512 KiB per download request. The image needs `sh`, `base64` and `dd`, which the kosa8 templates have.
- **Long commands.** A buffered exec is held to 90 seconds. When the host offers streamed exec (kosa8 Cloud does), every command is streamed and runs to its `timeout`. On a host that only buffers, a command past 90 seconds returns exit code `124` with an explanation.
- **Output.** A buffered exec keeps the first and last 1 MiB of each stream; when output is cut, `truncated` is `True`. A streamed exec keeps all of it.
- **No stdin, user or port APIs.** Commands run as the image's default user. Each exec argument is at most 8 KiB, so commands travel in environment variables, and a command over the 64 KiB request is written to `/workspace/.kosa8-run/` and run from there.
- **grep on Alpine images.** busybox `grep` lacks the `-Z` flag that `BaseSandbox.grep()` uses. The package detects this once per sandbox and searches with a Python 3 script that returns the same results. With GNU grep or ripgrep in the image, `BaseSandbox.grep()` is used unchanged.

Not covered by this package: kosa8's snapshot, checkpoint, rewind and fork endpoints.

## Tests

```bash
uv sync --group dev
uv run pytest tests/unit_tests
uv run ruff check . && uv run ruff format --check . && uv run mypy
```

The unit tests run against an in-process fake of the kosa8 Cloud API (`tests/fake_cloud.py`) that executes each command for real, with the files API on and off, so they need no network or key.

LangChain's standard `SandboxIntegrationTests` run two ways:

- Against the fake API in a local Linux container, which mirrors the `python` template:

  ```bash
  kosa8 run -d --name k8test python:3.12-alpine sleep 3600   # or: docker run -d ...
  KOSA8_TEST_CONTAINER=k8test uv run pytest tests/unit_tests/test_standard_local.py
  # KOSA8_TEST_ENGINE=docker to use docker instead of the kosa8 CLI
  ```

- Against kosa8 Cloud itself (creates one sandbox and deletes it):

  ```bash
  KOSA8_API_KEY=k8c_live_... uv run pytest tests/integration_tests
  ```

Both runs mark a few standard tests as expected failures, with reasons in `tests/standard_overrides.py`: `langchain-tests` 1.1.9 and `deepagents` 0.7 disagree on `glob()` paths and on `write()` to an existing file, which affects every `BaseSandbox` backend.

## License

MIT

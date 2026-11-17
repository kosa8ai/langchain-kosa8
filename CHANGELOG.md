# Changelog

## 0.2.0

- Typed errors from the bundled kosa8 Cloud client: `Kosa8PaymentRequiredError` (402, with `upgrade_url` and `agent_message`), `Kosa8OrgFrozenError` (423), `Kosa8RateLimitError` (429, with `retry_after`), `Kosa8ExecTimeoutError` (an exec cut off by the API's time limit; still a `Kosa8TimeoutError`, so existing handling keeps working) and `Kosa8NotSupportedError` (501). All are exported from `langchain_kosa8`.
- `create_sandbox` checks `ttl_s` (60 s to 90 days) before sending and takes `delete_after` (Unix ms or ISO 8601).
- New client methods, sync and async: `update_sandbox`, `list_sandboxes` / `iter_sandboxes` (cursor paging), `list_files`, `create_checkpoint`, `list_checkpoints`, `rewind_sandbox` and `fork_sandbox`.
- The `Kosa8Sandbox` backend itself is unchanged.

## 0.1.0

- First version: a Deep Agents sandbox backend on kosa8 Cloud.

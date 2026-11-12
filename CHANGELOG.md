# Changelog

## 0.2.0 — 2026-11-12

Not on PyPI yet: install from GitHub (see the README).

- The bundled kosa8 Cloud client (`_kosa8.py`, shared with the other kosa8 integrations) has typed errors: `Kosa8PaymentRequiredError` (402, with `upgrade_url` and `agent_message`), `Kosa8OrgFrozenError` (423 `org_frozen`), `Kosa8RateLimitError` (429, `retry_after` from Retry-After), `Kosa8ExecTimeoutError` (504 `exec_timeout`, or the 502 "timed out" a buffered exec gets after about 30 s; a `Kosa8TimeoutError`, so existing handling still applies) and `Kosa8NotSupportedError` (501). Errors keep the server's body.
- `create_sandbox` checks `ttl_s` (60 s to 90 days) before sending and takes `delete_after`.
- New client methods, sync and async: `update_sandbox` (PATCH lifetime and sleep timer, `clear_lifetime`), `list_sandboxes` / `iter_sandboxes` (`limit`/`cursor`/`next_cursor` paging, one page on servers that do not page), and the data-plane calls `read_file`, `write_file`, `list_files`, `create_checkpoint`, `list_checkpoints`, `rewind_sandbox`, `fork_sandbox` (these may answer 501 on current hosts; the integration itself does not rely on them).
- A test pins the default address to `https://api.kosa8.com`.
- README: install from GitHub until the PyPI release; kosa8 Cloud basics (Alpine `base` template, only `/workspace` persists, the 30 s exec limit, typed errors).

## 0.1.0

- First version.

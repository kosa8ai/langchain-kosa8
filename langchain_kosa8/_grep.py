"""A grep for images without GNU grep or ripgrep.

`BaseSandbox.grep()` runs `rg` or GNU `grep -rHnFZ`. Alpine-based images
(including kosa8 Cloud's `python` template) ship busybox grep, which has no
`-Z`, so the search would silently return nothing. This module builds a
Python 3 command that prints the same `path\\0line:text` records, so the
result parses the same way.
"""

from __future__ import annotations

import base64
from typing import Any

from deepagents.backends.protocol import GrepResult

PROBE_CMD = "if command -v rg >/dev/null 2>&1; then echo k8-native; elif grep --version 2>/dev/null | grep -q '(GNU grep)'; then echo k8-native; else echo k8-portable; fi"

_TEMPLATE = """python3 -c "
import os, sys, fnmatch, base64
d = lambda s: base64.b64decode(s).decode('utf-8')
root = d('{root}')
inc = d('{inc}') or None
pats = [p.encode('utf-8') for p in d('{pat}').split(chr(10))]
excluded = set(d('{exc}').split(chr(0))) - {{''}}
cap = {cap}
out = sys.stdout.buffer
n = 0
def files():
    if os.path.isfile(root):
        if inc is None or fnmatch.fnmatchcase(os.path.basename(root), inc):
            yield root
        return
    if not os.path.isdir(root):
        return
    for top, dirs, names in os.walk(root):
        dirs[:] = sorted(x for x in dirs if x not in excluded)
        for name in sorted(names):
            if inc is None or fnmatch.fnmatchcase(name, inc):
                yield os.path.join(top, name)
for f in files():
    try:
        with open(f, 'rb') as fh:
            data = fh.read()
    except OSError:
        continue
    if bytes(1) in data[:8192]:
        continue
    for i, line in enumerate(data.split(bytes([10])), 1):
        if any(p in line for p in pats):
            out.write(f.encode('utf-8', 'surrogateescape') + bytes(1) + str(i).encode() + b':' + line + bytes([10]))
            n += 1
            if cap is not None and n > cap:
                sys.exit(0)
"
"""


def _b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def build_portable_grep_cmd(
    pattern: str,
    path: str | None,
    glob: str | None,
    max_count: int | None,
    exclude_dirs: tuple[str, ...],
) -> str:
    """A Python 3 grep with GNU `grep -rHnFZ --include` semantics for basename globs."""
    root = path or "."
    root_name = root.rstrip("/").rsplit("/", 1)[-1]
    excluded = [n for n in exclude_dirs if n != root_name]
    return _TEMPLATE.format(
        root=_b64(root),
        inc=_b64(glob or ""),
        pat=_b64(pattern),
        exc=_b64("\0".join(excluded)),
        cap=None if max_count is None else int(max_count),
    )


def parse_grep_output(output: str, exit_code: int | None, path: str | None, max_count: int | None) -> GrepResult:
    """Parse `path\\0line:text` records into a `GrepResult`."""
    text = output.rstrip("\n")
    if exit_code not in (None, 0):
        return GrepResult(error=f"Path '{path or '.'}': {text.strip() or f'exit code {exit_code}'}")
    if not text:
        return GrepResult(matches=[])
    matches: list[Any] = []
    for line in text.split("\n"):
        try:
            file_path, rest = line.split("\0", 1)
            num, body = rest.split(":", 1)
            matches.append({"path": file_path, "line": int(num), "text": body})
        except ValueError:
            continue
    if max_count is not None and len(matches) > max_count:
        return GrepResult(matches=matches[:max_count], truncated=True)
    return GrepResult(matches=matches)

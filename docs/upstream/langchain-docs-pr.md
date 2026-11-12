# Fallback: PR to langchain-ai/docs

Use only if a maintainer asks for a PR rather than the listing issue.

## Change

In `scripts/data/integration_external_docs.yaml`, under `python:` → `sandboxes:`,
append:

```yaml
  - name: Kosa8Sandbox
    pypi: langchain-kosa8
    docs_url: https://github.com/kosa8ai/langchain-kosa8#readme
```

Then run `scripts/refresh_integration_downloads.py` (it regenerates
`src/snippets/oss/python-sandboxes-downloads.mdx`) and commit both files.

## PR title

`docs: list kosa8 sandbox integration (langchain-kosa8)`

## PR description

```markdown
## Summary

Adds `Kosa8Sandbox` (PyPI: `langchain-kosa8`) to the external sandbox
integrations table.

`langchain-kosa8` subclasses `BaseSandbox` and implements `execute`,
`upload_files`, `download_files`, `id` and their async variants against the
kosa8 Cloud API. Source and docs: https://github.com/kosa8ai/langchain-kosa8

## Test plan

- [ ] `SandboxIntegrationTests` run against kosa8 Cloud: <paste result>
- [ ] Downloads table regenerated with `scripts/refresh_integration_downloads.py`
- [ ] Docs URL resolves
```

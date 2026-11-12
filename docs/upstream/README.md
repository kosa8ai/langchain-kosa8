# Getting kosa8 listed in the LangChain docs

LangChain lists third-party sandbox packages on
<https://docs.langchain.com/oss/python/integrations/sandboxes>. The listing is
generated from `scripts/data/integration_external_docs.yaml` in
[langchain-ai/docs](https://github.com/langchain-ai/docs). Their process for a
new entry is an **integration-listing issue**, not a hand-written PR: a
maintainer reviews the issue, applies the `integration-run` label, and a
workflow opens the docs PR from the issue's fields (example: issue #7084 led to
PR #7090).

The issue form requires that the package is already on PyPI. So:

1. Publish `langchain-kosa8` to PyPI (`uv build && uv publish` from the repo root).
2. Run the standard tests against kosa8 Cloud once
   (`KOSA8_API_KEY=... uv run pytest tests/integration_tests`) and keep the
   output.
3. Open the issue at
   <https://github.com/langchain-ai/docs/issues/new?template=06-integration-submission.yml>
   with the fields in [`langchain-docs-issue.md`](langchain-docs-issue.md).

If a maintainer asks for a PR instead, [`langchain-docs-pr.md`](langchain-docs-pr.md)
has the YAML entry and a PR description.

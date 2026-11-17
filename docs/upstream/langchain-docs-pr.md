# Fallback: PR to langchain-ai/docs

Use only if a maintainer asks for a PR rather than the listing issue; the docs
automation closes hand-written listing PRs from outside the org.

The branch `drvt69talati/docs-1:docs/kosa8-sandbox` makes the same four-file
change the listing automation made for BoxdSandbox (PR #7622):

- `packages.yml`: a `langchain-kosa8` entry (`name_title: Kosa8Sandbox`).
- `scripts/data/integration_external_docs.yaml`: a `Kosa8Sandbox` row under `python.sandboxes`.
- `src/oss/python/integrations/providers/all_providers.mdx`: a `Kosa8Sandbox` card, in alphabetical order.
- `src/snippets/oss/python-sandboxes-downloads.mdx`: the `Kosa8Sandbox` row (N/A until PyPI counts downloads).

The PR title and description are in the coordinator's scratchpad (`langchain-docs-pr-body.md`).

# Contributing

Thanks for taking the time to contribute.

## Development environment

The project is managed with [uv](https://docs.astral.sh/uv/). One command
installs every dependency and extra and registers the pre-commit hooks:

```bash
make dev
```

There is also a devcontainer (`.devcontainer/`) with Python 3.14, uv, Docker-in-
Docker for the integration collector, and the Claude Code CLI already installed.

Run `make help` to see every target.

## Before opening a pull request

```bash
make lint    # ruff check, ruff format --check, mypy --strict
make test    # the offline suite, with coverage
```

`make format` applies Ruff's safe fixes and reformats in place. The pre-commit
hooks installed by `make dev` run the same checks on every commit; `make
pre-commit` runs them across the whole tree.

Ruff is the entire lint and format stack — it replaces black, flake8, isort and
pylint, so please do not add those back.

### Integration tests

The default `pytest` run is entirely offline. The integration suite talks to a
real `otel-collector` in Docker and is opt-in:

```bash
make test-integration
```

It starts the collector from `docker-compose.test.yml`, runs the tests marked
`integration`, and tears the collector back down. CI runs it on every push.

### Optional extras

The core install must depend on nothing but `aiohttp`. `encoding/protobuf.py`
and `transport/grpc.py` are imported lazily behind factory functions and never
at package import time. CI's `core-only` job installs the package with no extras
and fails if `grpcio`, `protobuf`, `opentelemetry-proto` or `orjson` turn up, so
an accidental top-level import breaks the build rather than a user's install.

## Tests

New behaviour needs a test, and the project is written test-first. The JSON
encoder in particular is validated by a property-based oracle
(`tests/test_encoder_oracle.py`) that parses its output with the canonical proto
schema and compares it against the protobuf encoder — if you touch encoding,
that suite is the one to watch.

Retry and processor tests inject a fake clock, so no test should ever sleep on
wall time.

## Pull requests

Release notes are drafted automatically by
[release-drafter](https://github.com/release-drafter/release-drafter) from
merged pull requests, so the PR title becomes the changelog line. Label each PR
with one of `breaking`, `feature`, `bug`, `maintenance`, `docs`, `dependencies`
or `security` — the label picks both the changelog section and the version bump.
Use `skip-changelog` to leave a PR out.

## Releasing

1. Bump `version` in `pyproject.toml` and merge.
2. Publish the drafted GitHub release.

Publishing the release runs `.github/workflows/pythonpublish.yml`, which builds
the sdist and wheel with `uv build` and uploads them to PyPI via [Trusted
Publishing](https://docs.pypi.org/trusted-publishers/) — no API token is stored
as a repository secret.

Trusted Publishing needs a one-time setup on PyPI before the first release:
under the project's *Publishing* settings, add a GitHub publisher with owner
`wonderslug`, repository `asyncio-otlp-client`, workflow `pythonpublish.yml`,
and environment `pypi`. The repository needs a matching `pypi` environment. For
the very first upload, use the pending-publisher form on PyPI (same fields)
since the project does not exist yet, or push one release manually with `make
release`.

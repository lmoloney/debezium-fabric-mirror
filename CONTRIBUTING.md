# Contributing to Open Mirroring Debezium

Thanks for your interest in contributing! This project bridges Debezium CDC events to Microsoft Fabric Open Mirroring, and we welcome contributions of all kinds.

## Getting Started

```bash
# Clone the repo
git clone https://github.com/lmoloney/debezium-fabric-mirror.git
cd debezium-fabric-mirror

# Install uv (if needed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies (including dev)
uv sync

# Run the test suite
uv run pytest

# Lint and type check
uv run ruff check src/ tests/
uv run pyright
```

See [`docs/runbooks/local-dev-setup.md`](docs/runbooks/local-dev-setup.md) for full local development setup including EventHub and Fabric configuration.

## How to Contribute

### Reporting Bugs

Open an [issue](https://github.com/lmoloney/debezium-fabric-mirror/issues) with:
- What you expected vs. what happened
- Steps to reproduce
- Relevant logs (redact any connection strings or credentials)

### Suggesting Features

Open an issue describing the use case and proposed solution. For new source connectors, see the [Adding a New Source Connector](README.md) section.

### Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b my-feature`)
3. Make your changes
4. Ensure all checks pass:
   ```bash
   uv run ruff check src/ tests/
   uv run ruff format src/ tests/
   uv run pyright
   uv run pytest
   ```
5. Commit with a clear message
6. Open a pull request against `main`

### Adding a New Source Connector

The pipeline is connector-agnostic — Debezium normalizes the event format. To add a new source:

1. Add sample events to `tests/sample_events/<connector>_*.json`
2. Add connector-specific notes to `docs/sources/<connector>.md`
3. Update `docs/design/type-mapping.md` if type coercion differs
4. Add table config overrides in `table_config.json` if needed
5. Create a [decision record](docs/decisions/) if architectural changes are needed

## Code Style

- **Formatter/Linter:** [Ruff](https://docs.astral.sh/ruff/) (configured in `pyproject.toml`)
- **Type checking:** [Pyright](https://github.com/microsoft/pyright) in basic mode
- **Line length:** 120 characters
- **Python version:** 3.12+

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the system design and [`docs/decisions/`](docs/decisions/) for architecture decision records.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).

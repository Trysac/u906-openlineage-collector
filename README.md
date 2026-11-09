# openlineage-collector

[![PyPI version](https://img.shields.io/pypi/v/trysac-openlineage-collector)](https://pypi.org/project/trysac-openlineage-collector/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-openlineage-collector)](LICENSE)
[![CI](https://github.com/Trysac/u906-openlineage-collector/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u906-openlineage-collector/actions/workflows/ci.yml)

Emit and receive OpenLineage run events, validate them against the 2-0-2 spec and query upstream and downstream lineage of datasets and jobs

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-openlineage-collector
# or
uv add trysac-openlineage-collector
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import openlineage_collector

# normalize() trims surrounding whitespace and lower-cases the text.
print(openlineage_collector.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    openlineage_collector.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez

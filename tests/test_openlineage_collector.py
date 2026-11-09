"""Behavior tests for the public API of openlineage_collector."""

from __future__ import annotations

import pytest

import openlineage_collector


def test_normalize_trims_and_lowercases() -> None:
    assert openlineage_collector.normalize("  Hello World ") == "hello world"


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_normalize_rejects_blank_input(value: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        openlineage_collector.normalize(value)


def test_version_is_semver() -> None:
    parts = openlineage_collector.__version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)

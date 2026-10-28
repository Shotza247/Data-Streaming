"""Shared pytest fixtures."""
import pytest


@pytest.fixture
def tmp(tmp_path):
    """Temp directory (as str) used by the token-budget tests for their state file."""
    return str(tmp_path)

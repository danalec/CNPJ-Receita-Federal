"""Shared fixtures.

The whole project talks to a single module-level ``settings`` instance, so a
test that mutates it leaks that mutation into every test that runs after it.
The autouse fixture below snapshots the instance and puts it back, which keeps
tests independent regardless of the order pytest collects them in.
"""

import pytest

from src.settings import settings


@pytest.fixture(autouse=True)
def isolate_settings():
    snapshot = dict(settings.__dict__)
    yield
    settings.__dict__.clear()
    settings.__dict__.update(snapshot)


@pytest.fixture
def empty_domain_tables():
    """Pretend every domain table is still empty.

    The fake connections in these tests answer every count query with 0, so
    validate_chunk raises in strict mode. Tests that are not about FK validation
    opt into this explicitly; previously they passed only because another test
    happened to leave strict_fk_validation flipped off.
    """
    settings.strict_fk_validation = False
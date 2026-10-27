"""Provide fixtures for the tests."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import socket

import pytest


@pytest.fixture
def juseless() -> None:
    """Skip the test if not running on juseless."""
    if socket.gethostname() != "juseless":
        pytest.skip("This test is only for juseless")

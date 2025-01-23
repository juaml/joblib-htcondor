"""Provide runtime tests."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import logging
import socket

import pytest
from joblib import Parallel, delayed, parallel_config

from joblib_htcondor import register_htcondor


# Check if the test is running on juseless
if socket.gethostname() != "juseless":
    pytest.skip("These tests are only for juseless", allow_module_level=True)


register_htcondor()


def test_normal() -> None:
    """Test normal execution."""
    from operator import neg

    with parallel_config(
        backend="htcondor", n_jobs=-1, request_cpus=1, request_memory="1Gb"
    ):
        out = Parallel()(delayed(neg)(i + 1) for i in range(5))

    assert out == [-1, -2, -3, -4, -5]


def test_exception() -> None:
    """Test running with an exception."""

    def neg_with_exception(a: int) -> int:
        """Negate if a!=2, raise an exception otherwise.

        Parameters
        ----------
        a : int
            The number to negate.

        Returns
        -------
        int
            The negated number.

        Raises
        ------
        ValueError
            If `a` is 2.

        """
        if a == 2:
            raise ValueError("This is an exception")
        else:
            return -a

    with parallel_config(
        backend="htcondor",
        n_jobs=-1,
        request_cpus=1,
        request_memory="1Gb",
        verbose=1000,
        worker_log_level=logging.INFO,
    ):
        with pytest.raises(ValueError, match="This is an exception"):
            Parallel()(delayed(neg_with_exception)(i + 1) for i in range(5))

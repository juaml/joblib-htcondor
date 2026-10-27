"""Provide runtime tests."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import logging
import time

import htcondor2
import pytest
from joblib import Parallel, delayed, parallel_config

from joblib_htcondor import register_htcondor


pytestmark = pytest.mark.usefixtures("juseless")

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


@pytest.fixture
def schedd():
    """Provide a schedd client and remove the jobs left by a test.

    The test must add the batch names it submits to ``schedd.batch_names``.
    """
    client = htcondor2.Schedd()
    client.batch_names = []
    yield client
    for batch_name in client.batch_names:
        client.act(
            htcondor2.JobAction.Remove,
            f'JobBatchName == "{batch_name}"',
            reason="Cleanup by joblib_htcondor tests",
        )


def test_cancel_on_exception(schedd) -> None:
    """Test that pending jobs are cancelled when a task raises."""

    def sleep_or_raise(a: int) -> int:
        """Raise if a is 1, sleep for a long time otherwise.

        Parameters
        ----------
        a : int
            The task number.

        Returns
        -------
        int
            The task number.

        Raises
        ------
        ValueError
            If `a` is 1.

        """
        if a == 1:
            raise ValueError("Fail fast")
        time.sleep(600)
        return a

    with parallel_config(
        backend="htcondor",
        n_jobs=-1,
        request_cpus=1,
        request_memory="1Gb",
        poll_interval=1,
    ):
        parallel = Parallel()
        with pytest.raises(ValueError, match="Fail fast"):
            parallel(delayed(sleep_or_raise)(i) for i in range(1, 6))

    batch_name = parallel._backend._this_batch_name
    schedd.batch_names.append(batch_name)
    constraint = f'JobBatchName == "{batch_name}"'

    # No job of the batch is left idle (1), running (2) or held (5)
    active = schedd.query(
        constraint=f"{constraint} && JobStatus =!= 3 && JobStatus =!= 4",
        projection=["ClusterId", "ProcId", "JobStatus"],
    )
    assert active == []

    # The sleeping jobs were removed (3), either still in the queue or
    # already in the history
    removed = []
    for _ in range(30):
        removed = schedd.query(
            constraint=f"{constraint} && JobStatus == 3",
            projection=["ClusterId"],
        ) + list(
            schedd.history(
                constraint=f"{constraint} && JobStatus == 3",
                projection=["ClusterId"],
            )
        )
        if len(removed) == 4:
            break
        time.sleep(2)
    assert len(removed) == 4

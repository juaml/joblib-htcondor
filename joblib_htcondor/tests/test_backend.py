"""Provide tests for _HTCondorBackend."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import functools
import logging
import signal
import threading
import time
from concurrent.futures import Future
from types import SimpleNamespace

import classad2
import pytest
from joblib.externals.cloudpickle import cloudpickle as pickle

from joblib_htcondor.backend import _HTCondorBackend
from joblib_htcondor.delayed_submission import DelayedSubmission


def compare_backends(a, b) -> bool:
    """Compare two backends."""
    ok = True
    ok = ok and a.__class__ == b.__class__
    core_vars = [
        "_pool",
        "_schedd",
        "_universe",
        "_python_path",
        "_request_cpus",
        "_request_memory",
        "_request_disk",
        "_initial_dir",
        "_log_dir_prefix",
        "_poll_interval",
        "_shared_data_dir",
        "_delete_task_file_on_load",
        "_extra_directives",
        "_worker_log_level",
        "_lock_lifetime",
        "_throttle",
    ]
    for var in core_vars:
        ok = ok and getattr(a, var) == getattr(b, var)
    return ok


def test_pickle() -> None:
    """Test pickling of the backend."""
    backend = _HTCondorBackend(request_cpus=1, request_memory="2GB")
    pickled_backend = pickle.loads(pickle.dumps(backend))
    assert compare_backends(pickled_backend, backend)

    backend2 = _HTCondorBackend(
        pool="head2.htc.inm7.de",
        request_cpus=1,
        request_memory="8GB",
        request_disk="1GB",
        python_path="/home/fraimondo/miniconda3/ppc64le_dev/bin/python",
        extra_directives={"Requirements": 'Arch == "ppc64le"'},
        worker_log_level=logging.DEBUG,
        throttle=11,
        poll_interval=0.1,  # type: ignore
        lock_lifetime=300,
    )
    pickled_backend2 = pickle.loads(pickle.dumps(backend2))
    assert compare_backends(pickled_backend2, backend2)


class _FakeSchedd:
    """Fake schedd that records submitted batch names and removals."""

    def __init__(self) -> None:
        self.batch_names = set()
        self.removed = []
        self.submitted = []

    def query(self, constraint, projection):
        """Return a job ad if the constraint matches a submitted batch."""
        if any(
            constraint == f"JobBatchName =?= {name}"
            for name in self.batch_names
        ):
            return [classad2.ClassAd({"ClusterId": 1, "ProcId": 0})]
        return []

    def act(self, action, job_spec, reason):
        """Record the removed jobs."""
        self.removed.extend(job_spec)

    def submit(self, description, count):
        """Record the submission and return a fake submit result."""
        self.submitted.append(description)
        return SimpleNamespace(cluster=lambda: len(self.submitted))


def test_cancel_jobs_matches_submitted_batch(tmp_path) -> None:
    """Test that cancelling targets the batch name used for submission."""
    schedd = _FakeSchedd()
    backend = _HTCondorBackend(
        request_cpus=1,
        request_memory="1GB",
        schedd=schedd,
        shared_data_dir=tmp_path,
        log_dir_prefix=str(tmp_path / "logs"),
    )
    backend.configure(n_jobs=1)
    backend._submit(Future(), func=print)
    submit = backend._queued_jobs_list[0].htcondor_submit
    batch_name = submit.get("MY.JobBatchName")
    assert batch_name == f'"{backend._this_batch_name}"'
    schedd.batch_names.add(batch_name)

    backend._cancel_jobs()
    assert schedd.removed == ["1.0"]


@pytest.fixture
def running_backend(tmp_path):
    """Provide a started backend with a fake schedd, stopped afterwards."""
    backend = _HTCondorBackend(
        request_cpus=1,
        request_memory="1GB",
        schedd=_FakeSchedd(),
        shared_data_dir=tmp_path,
        log_dir_prefix=str(tmp_path / "logs"),
        poll_interval=0.1,  # type: ignore
    )
    backend.configure(n_jobs=1)
    sigterm_handler = signal.getsignal(signal.SIGTERM)
    backend.start_call()
    yield backend
    backend.stop_call()
    signal.signal(signal.SIGTERM, sigterm_handler)


def test_unpicklable_task_fails(running_backend) -> None:
    """Test that a task that cannot be pickled fails instead of hanging."""
    future = Future()
    running_backend._submit(
        future, func=functools.partial(print, threading.Lock())
    )
    with pytest.raises(TypeError, match="cannot pickle"):
        future.result(timeout=10)
    assert running_backend._client.submitted == []


def test_task_requeued_on_lock_timeout(running_backend, monkeypatch) -> None:
    """Test that a task is submitted later if the lock was not obtained."""
    results = iter([False, True])
    monkeypatch.setattr(
        DelayedSubmission, "dump", lambda *args, **kwargs: next(results)
    )
    running_backend._submit(Future(), func=print)
    for _ in range(100):
        if running_backend._client.submitted:
            break
        time.sleep(0.1)
    assert len(running_backend._client.submitted) == 1

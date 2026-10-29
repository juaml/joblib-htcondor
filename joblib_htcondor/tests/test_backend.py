"""Provide tests for _HTCondorBackend."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import functools
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace

import classad2
import htcondor2
import pytest
from joblib.externals.cloudpickle import cloudpickle as pickle

from joblib_htcondor import backend as backend_module
from joblib_htcondor.backend import (
    HTCondorJobError,
    _HTCondorBackend,
    _quote_argument,
)
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


def test_pickle(monkeypatch) -> None:
    """Test pickling of the backend."""
    # Do not connect to a schedd, so the test runs without HTCondor
    monkeypatch.setattr(htcondor2, "Schedd", lambda *args: _FakeSchedd())
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


def _make_backend(tmp_path) -> _HTCondorBackend:
    """Create a configured backend with a fake schedd."""
    backend = _HTCondorBackend(
        request_cpus=1,
        request_memory="1GB",
        schedd=_FakeSchedd(),
        shared_data_dir=tmp_path,
        log_dir_prefix=str(tmp_path / "logs"),
    )
    backend.configure(n_jobs=1)
    return backend


_SIGTERM_SCRIPT = """
import os, signal, sys, threading
from concurrent.futures import Future
import classad2
from pathlib import Path
from joblib_htcondor.backend import _HTCondorBackend

tmp_path = Path(sys.argv[1])

class FakeSchedd:
    def query(self, constraint, projection):
        return [classad2.ClassAd({"ClusterId": 1, "ProcId": 0})]
    def act(self, action, job_spec, reason):
        (tmp_path / "cancelled").touch()

backend = _HTCondorBackend(
    request_cpus=1,
    request_memory="1GB",
    schedd=FakeSchedd(),
    shared_data_dir=tmp_path / "shared",
    log_dir_prefix=str(tmp_path / "logs"),
)
backend.configure(n_jobs=1)
backend.start_call()
print(backend._current_shared_data_dir, flush=True)
threading.Timer(0.5, os.kill, (os.getpid(), signal.SIGTERM)).start()
# Wait for a task forever, as joblib does
Future().result()
"""


def test_sigterm_terminates_process(tmp_path) -> None:
    """Test that SIGTERM cancels the jobs and terminates the process."""
    script = tmp_path / "script.py"
    script.write_text(_SIGTERM_SCRIPT)
    proc = subprocess.run(
        [sys.executable, str(script), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == -signal.SIGTERM, proc.stderr
    shared_data_dir = Path(proc.stdout.splitlines()[0])
    assert not shared_data_dir.exists()
    assert (tmp_path / "cancelled").exists()


def test_sigterm_calls_previous_handler(tmp_path) -> None:
    """Test that SIGTERM cleans up and calls the previous handler."""
    calls = []

    def previous(signum, frame):
        calls.append(signum)

    original = signal.signal(signal.SIGTERM, previous)
    try:
        backend = _make_backend(tmp_path)
        backend._client.batch_names.add(f'"{backend._this_batch_name}"')
        backend.start_call()
        assert signal.getsignal(signal.SIGTERM) == backend._sigterm_handler
        os.kill(os.getpid(), signal.SIGTERM)
        # The handler runs in the main thread between bytecodes
        for _ in range(100):
            if calls:
                break
            time.sleep(0.01)
        assert calls == [signal.SIGTERM]
        assert signal.getsignal(signal.SIGTERM) is previous
        assert backend._client.removed == ["1.0"]
        assert not backend._current_shared_data_dir.exists()
        # joblib still calls stop_call afterwards
        backend.stop_call()
        assert signal.getsignal(signal.SIGTERM) is previous
    finally:
        signal.signal(signal.SIGTERM, original)


def test_stop_call_restores_sigterm_handler(tmp_path) -> None:
    """Test that the SIGTERM handler is restored after stop_call."""
    original = signal.getsignal(signal.SIGTERM)
    backend = _make_backend(tmp_path)
    backend.start_call()
    assert signal.getsignal(signal.SIGTERM) == backend._sigterm_handler
    backend.stop_call()
    assert signal.getsignal(signal.SIGTERM) is original


def test_start_call_in_thread(tmp_path) -> None:
    """Test that the backend can be used from a non-main thread."""
    original = signal.getsignal(signal.SIGTERM)
    backend = _make_backend(tmp_path)
    errors = []

    def run():
        try:
            backend.start_call()
            backend.stop_call()
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()
    assert errors == []
    assert signal.getsignal(signal.SIGTERM) is original


def _job_ad(**kwargs) -> classad2.ClassAd:
    """Create the ad of the first submitted job."""
    return classad2.ClassAd({"ClusterId": 1, **kwargs})


@pytest.mark.parametrize(
    "ads, match",
    [
        ([_job_ad(JobStatus=5, HoldReason="Out of memory")], "held: Out of"),
        ([_job_ad(JobStatus=3, RemoveReason="By user")], "removed: By user"),
        ([_job_ad(JobStatus=4)], "completed without writing the results"),
        ([], "left the queue without writing the results"),
    ],
)
def test_gone_job_fails(running_backend, monkeypatch, ads, match) -> None:
    """Test that a job that will never write results fails its task."""
    monkeypatch.setattr(backend_module, "_JOB_GONE_GRACE_PERIOD", 0.5)
    monkeypatch.setattr(running_backend._client, "query", lambda **kwargs: ads)
    future = Future()
    running_backend._submit(future, func=print)
    with pytest.raises(HTCondorJobError, match=match):
        future.result(timeout=10)
    # The job is removed from the waiting jobs after failing the task
    for _ in range(100):
        if not running_backend._waiting_jobs_deque:
            break
        time.sleep(0.1)
    assert len(running_backend._waiting_jobs_deque) == 0
    assert list(running_backend._current_shared_data_dir.iterdir()) == []


def test_removed_job_reason_kept(running_backend, monkeypatch) -> None:
    """Test that the reason is kept after a removed job left the queue."""
    monkeypatch.setattr(backend_module, "_JOB_GONE_GRACE_PERIOD", 0.5)
    ads = iter([[_job_ad(JobStatus=3, RemoveReason="By user")]])
    monkeypatch.setattr(
        running_backend._client, "query", lambda **kwargs: next(ads, [])
    )
    future = Future()
    running_backend._submit(future, func=print)
    with pytest.raises(HTCondorJobError, match="removed: By user"):
        future.result(timeout=10)


def test_gone_job_grace_period(running_backend, monkeypatch) -> None:
    """Test that results written within the grace period are used."""
    monkeypatch.setattr(backend_module, "_JOB_GONE_GRACE_PERIOD", 60)
    monkeypatch.setattr(running_backend._client, "query", lambda **kw: [])
    future = Future()
    running_backend._submit(future, func=functools.partial(max, 1, 2))
    for _ in range(100):
        if running_backend._waiting_jobs_deque:
            break
        time.sleep(0.1)
    job_meta = running_backend._waiting_jobs_deque[0]
    time.sleep(1)
    assert not future.done()
    # Write the results as the executor does
    ds = DelayedSubmission.load(job_meta.pickle_fname, lock_lifetime=10)
    ds.run()
    ds.dump(
        job_meta.pickle_fname.with_stem(f"{job_meta.pickle_fname.stem}_out"),
        result_only=True,
    )
    job_meta.pickle_fname.with_suffix(".run").touch()
    assert future.result(timeout=10) == 2


@pytest.mark.parametrize(
    "query",
    [
        lambda **kwargs: [_job_ad(JobStatus=2)],
        lambda **kwargs: (_ for _ in ()).throw(OSError("No schedd")),
    ],
    ids=["running", "query_error"],
)
def test_running_job_does_not_fail(running_backend, monkeypatch, query):
    """Test that running jobs, or jobs of unknown status, do not fail."""
    monkeypatch.setattr(backend_module, "_JOB_GONE_GRACE_PERIOD", 0)
    monkeypatch.setattr(running_backend._client, "query", query)
    future = Future()
    running_backend._submit(future, func=print)
    time.sleep(1)
    assert not future.done()
    assert len(running_backend._waiting_jobs_deque) == 1


def test_throttle_list(tmp_path) -> None:
    """Test that a throttle list with one value is used as that value."""

    def make(throttle):
        return _HTCondorBackend(
            request_cpus=1,
            request_memory="1GB",
            schedd=_FakeSchedd(),
            shared_data_dir=tmp_path,
            throttle=throttle,
        )

    assert make([5])._throttle == 5
    assert make([5, 2])._throttle == [5, 2]
    assert make(5)._throttle == 5
    with pytest.raises(ValueError, match="at least one value"):
        make([])


def test_quote_argument() -> None:
    """Test quoting an argument for HTCondor's new arguments syntax."""
    assert _quote_argument("/a/b.pickle") == "'/a/b.pickle'"
    assert _quote_argument('/a b/c\'s "d"') == "'/a b/c''s \"\"d\"\"'"


def test_submit_arguments_quoted(tmp_path) -> None:
    """Test that the task file path is quoted in the submit arguments."""
    backend = _HTCondorBackend(
        request_cpus=1,
        request_memory="1GB",
        schedd=_FakeSchedd(),
        shared_data_dir=tmp_path / "shared dir's",
        log_dir_prefix=str(tmp_path / "logs"),
    )
    backend.configure(n_jobs=1)
    backend._submit(Future(), func=print)
    job_meta = backend._queued_jobs_list[0]
    arguments = job_meta.htcondor_submit.get("arguments")
    path = job_meta.pickle_fname.as_posix().replace("'", "''")
    assert arguments.startswith('"-m joblib_htcondor.executor ')
    assert arguments.endswith(f" '{path}'\"")


def test_write_metadata_concurrent(tmp_path) -> None:
    """Test that concurrent metadata writes never leave a partial file."""
    backend = _HTCondorBackend(
        request_cpus=1,
        request_memory="1GB",
        schedd=_FakeSchedd(),
        shared_data_dir=tmp_path,
        log_dir_prefix=str(tmp_path / "logs"),
        export_metadata=True,
    )
    backend.configure(n_jobs=1)
    sigterm_handler = signal.getsignal(signal.SIGTERM)
    backend.start_call()
    try:
        for _ in range(200):
            backend._submit(Future(), func=print)
        meta_fname = (
            tmp_path / ".jht-meta" / f"{backend._this_batch_name}.json"
        )
        errors = []

        def write():
            for _ in range(100):
                backend.write_metadata()

        def read():
            for _ in range(500):
                try:
                    json.loads(meta_fname.read_text())
                except ValueError as e:
                    errors.append(e)

        threads = [threading.Thread(target=write) for _ in range(4)]
        threads.append(threading.Thread(target=read))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert errors == []
        assert [p.name for p in meta_fname.parent.iterdir()] == [
            meta_fname.name
        ]
    finally:
        backend._continue = False
        backend._polling_thread_executor.shutdown()
        signal.signal(signal.SIGTERM, sigterm_handler)

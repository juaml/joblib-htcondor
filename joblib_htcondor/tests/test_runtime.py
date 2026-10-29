"""Provide runtime tests."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import logging
import pickle
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import htcondor2
import pytest
from joblib import Parallel, delayed, parallel_config

from joblib_htcondor import register_htcondor
from joblib_htcondor.backend import HTCondorJobError
from joblib_htcondor.delayed_submission import DelayedSubmission


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

    # The four sleeping jobs were submitted
    cluster_ids = [
        job.cluster_id for job in parallel._backend._waiting_jobs_deque
    ]
    assert len(cluster_ids) == 4
    constraint = " || ".join(f"ClusterId == {c}" for c in cluster_ids)

    # They sleep for 10 minutes, so they were cancelled if they are removed
    # (3) or already left the queue. The history is not checked: removed jobs
    # leave the queue within a second but can take long to show up there.
    remaining = []
    for _ in range(60):
        remaining = schedd.query(
            constraint=constraint, projection=["ClusterId", "JobStatus"]
        )
        if all(job["JobStatus"] == 3 for job in remaining):
            break
        time.sleep(1)
    assert all(job["JobStatus"] == 3 for job in remaining), remaining


def test_unpicklable_argument() -> None:
    """Test that a task that cannot be pickled raises instead of hanging."""
    with parallel_config(
        backend="htcondor",
        n_jobs=-1,
        request_cpus=1,
        request_memory="1Gb",
        poll_interval=1,
    ):
        with pytest.raises(TypeError, match="cannot pickle"):
            Parallel()(delayed(print)(threading.Lock()) for _ in range(2))


def test_unpicklable_result() -> None:
    """Test that a result that cannot be pickled raises instead of hanging."""

    def make_lock(a: int) -> "threading.Lock":
        """Return a lock, which cannot be pickled.

        Parameters
        ----------
        a : int
            Unused.

        Returns
        -------
        threading.Lock
            A lock.

        """
        import threading

        return threading.Lock()

    with parallel_config(
        backend="htcondor",
        n_jobs=-1,
        request_cpus=1,
        request_memory="1Gb",
        poll_interval=1,
    ):
        with pytest.raises(
            pickle.PicklingError,
            match="Could not pickle the result of the task: TypeError",
        ):
            Parallel()(delayed(make_lock)(i) for i in range(2))


_SLEEPING_PARALLEL_SCRIPT = """
import sys
import time

from joblib import Parallel, delayed, parallel_config

from joblib_htcondor import register_htcondor


def sleep(a):
    time.sleep(600)
    return a


register_htcondor()
with parallel_config(
    backend="htcondor",
    n_jobs=-1,
    request_cpus=1,
    request_memory="1Gb",
    poll_interval=1,
    extra_directives={"+JhtTestTag": f'"{sys.argv[1]}"'},
):
    Parallel()(delayed(sleep)(i) for i in range(4))
"""


def test_sigterm_cancels_jobs() -> None:
    """Test that SIGTERM cancels the jobs and terminates the process."""
    client = htcondor2.Schedd()
    tag = uuid.uuid4().hex
    constraint = f'JhtTestTag == "{tag}"'
    proc = subprocess.Popen(
        [sys.executable, "-c", _SLEEPING_PARALLEL_SCRIPT, tag],
        cwd=Path(__file__).resolve().parents[2],
    )
    try:
        # Wait until all the jobs are idle (1) or running (2)
        jobs = []
        for _ in range(60):
            jobs = client.query(
                constraint=f"{constraint} && JobStatus <= 2",
                projection=["ClusterId"],
            )
            if len(jobs) == 4:
                break
            time.sleep(1)
        assert len(jobs) == 4

        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=60) == -signal.SIGTERM

        # No job is left idle (1), running (2) or held (5)
        active = client.query(
            constraint=f"{constraint} && JobStatus =!= 3 && JobStatus =!= 4",
            projection=["ClusterId", "JobStatus"],
        )
        assert active == []
    finally:
        if proc.poll() is None:
            proc.kill()


def test_job_without_results() -> None:
    """Test that a job that dies before writing its results fails."""

    def die(a: int) -> int:
        """Exit the executor without writing the results.

        Parameters
        ----------
        a : int
            Unused.

        Returns
        -------
        int
            Never returns.

        """
        import os

        os._exit(1)

    with parallel_config(
        backend="htcondor",
        n_jobs=-1,
        request_cpus=1,
        request_memory="1Gb",
        poll_interval=1,
    ):
        with pytest.raises(
            HTCondorJobError,
            match=r"(completed|left the queue) without writing the results",
        ):
            Parallel()(delayed(die)(i) for i in range(1))


@pytest.mark.parametrize(
    "command, match",
    [
        ("condor_hold", "is held: Held by joblib_htcondor tests"),
        # Removed jobs can leave the queue before the backend sees them
        ("condor_rm", r"was removed|left the queue without writing"),
    ],
    ids=["hold", "remove"],
)
def test_job_held_or_removed(command, match) -> None:
    """Test that a job held or removed outside Python fails its task."""
    client = htcondor2.Schedd()
    tag = uuid.uuid4().hex
    constraint = f'JhtTestTag == "{tag}"'

    def act_on_job() -> None:
        """Hold or remove the job once it is in the queue."""
        for _ in range(60):
            if client.query(
                constraint=f"{constraint} && JobStatus <= 2",
                projection=["ClusterId"],
            ):
                subprocess.run(
                    [
                        command,
                        "-reason",
                        "Held by joblib_htcondor tests",
                        "-constraint",
                        constraint,
                    ],
                    check=True,
                )
                return
            time.sleep(1)

    thread = threading.Thread(target=act_on_job)
    thread.start()
    try:
        with parallel_config(
            backend="htcondor",
            n_jobs=-1,
            request_cpus=1,
            request_memory="1Gb",
            poll_interval=1,
            extra_directives={"+JhtTestTag": f'"{tag}"'},
        ):
            with pytest.raises(HTCondorJobError, match=match):
                Parallel()(delayed(time.sleep)(600) for _ in range(1))
    finally:
        thread.join()
        if client.query(constraint=constraint, projection=["ClusterId"]):
            client.act(
                htcondor2.JobAction.Remove,
                constraint,
                reason="Cleanup by joblib_htcondor tests",
            )


def test_unimportable_module(tmp_path, monkeypatch) -> None:
    """Test that a task that the worker cannot load raises the error."""
    # A module that can only be imported in this process
    module_dir = tmp_path / "module"
    module_dir.mkdir()
    (module_dir / "jht_local_module.py").write_text(
        "def f(a):\n    return a\n"
    )
    monkeypatch.syspath_prepend(str(module_dir))
    import jht_local_module

    with parallel_config(
        backend="htcondor",
        n_jobs=-1,
        request_cpus=1,
        request_memory="1Gb",
        poll_interval=1,
    ):
        with pytest.raises(ModuleNotFoundError, match="jht_local_module"):
            Parallel()(delayed(jht_local_module.f)(i) for i in range(2))


def test_corrupt_task_file(monkeypatch) -> None:
    """Test that a corrupt task file raises instead of hanging."""

    def dump(self, filename, result_only=False):
        Path(filename).write_text("This is not a pickle file.")
        return True

    monkeypatch.setattr(DelayedSubmission, "dump", dump)
    with parallel_config(
        backend="htcondor",
        n_jobs=-1,
        request_cpus=1,
        request_memory="1Gb",
        poll_interval=1,
    ):
        with pytest.raises(pickle.UnpicklingError, match="file is corrupt"):
            Parallel()(delayed(max)(i, 1) for i in range(2))


def test_paths_with_spaces_and_quotes() -> None:
    """Test that the shared and log directories can contain special chars."""
    # The directories must be in the shared filesystem
    base = Path(__file__).resolve().parents[2] / f"jht test's {uuid.uuid4()}"
    try:
        with parallel_config(
            backend="htcondor",
            n_jobs=-1,
            request_cpus=1,
            request_memory="1Gb",
            poll_interval=1,
            shared_data_dir=base / "shared dir",
            log_dir_prefix=str(base / "log dir"),
        ):
            out = Parallel()(delayed(abs)(-i) for i in range(2))
        assert out == [0, 1]
    finally:
        shutil.rmtree(base, ignore_errors=True)

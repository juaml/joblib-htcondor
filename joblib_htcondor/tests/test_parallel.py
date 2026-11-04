"""Provide tests running joblib.Parallel with jobs run locally."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import shlex
import subprocess
import warnings
from operator import neg
from types import SimpleNamespace

import classad2
import pytest
from joblib import Parallel, delayed, parallel_config

from joblib_htcondor import register_htcondor


register_htcondor()


class _LocalSchedd:
    """Fake schedd that runs each job as a local executor process."""

    def __init__(self) -> None:
        self.procs = {}
        self.removed = []

    def submit(self, description, count):
        """Run the executor with the job's executable and arguments."""
        # Arguments use the new syntax: the whole string in double quotes
        args = shlex.split(description.get("arguments")[1:-1])
        cluster = len(self.procs) + 1
        self.procs[cluster] = subprocess.Popen(
            [description.get("executable"), *args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return SimpleNamespace(cluster=lambda: cluster)

    def query(self, constraint, projection):
        """Return the running jobs, finished jobs leave the queue."""
        return [
            classad2.ClassAd(
                {"ClusterId": cluster, "ProcId": 0, "JobStatus": 2}
            )
            for cluster, proc in self.procs.items()
            if proc.poll() is None
        ]

    def act(self, action, job_spec, reason):
        """Kill the removed jobs."""
        for job_id in job_spec:
            self.procs[int(job_id.split(".")[0])].kill()
            self.removed.append(job_id)


@pytest.fixture
def schedd():
    """Provide a local schedd and kill the jobs left by a test."""
    client = _LocalSchedd()
    yield client
    for proc in client.procs.values():
        proc.kill()
        proc.wait()


def _config(schedd, tmp_path):
    """Return the parallel config to run the jobs with the local schedd."""
    return parallel_config(
        backend="htcondor",
        n_jobs=-1,
        request_cpus=1,
        request_memory="1GB",
        schedd=schedd,
        shared_data_dir=tmp_path / "shared",
        log_dir_prefix=str(tmp_path / "logs"),
        poll_interval=0.1,
    )


def test_parallel(schedd, tmp_path) -> None:
    """Test running tasks through joblib.Parallel."""
    with warnings.catch_warnings():
        # joblib >= 1.5 warns if the backend only implements apply_async
        warnings.filterwarnings("error", message=".*apply_async")
        with _config(schedd, tmp_path):
            out = Parallel()(delayed(neg)(i) for i in range(5))
    assert out == [0, -1, -2, -3, -4]
    assert len(schedd.procs) == 5


def test_parallel_exception(schedd, tmp_path) -> None:
    """Test that a task exception is raised and the other jobs removed."""

    def sleep_or_raise(a: int) -> int:
        """Raise if a is 0, sleep for a long time otherwise.

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
            If `a` is 0.

        """
        import time

        if a == 0:
            raise ValueError("Fail fast")
        time.sleep(600)
        return a

    with _config(schedd, tmp_path):
        with pytest.raises(ValueError, match="Fail fast"):
            Parallel()(delayed(sleep_or_raise)(i) for i in range(3))
    assert sorted(schedd.removed) == ["2.0", "3.0"]

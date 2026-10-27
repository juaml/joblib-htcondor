"""Provide tests for _HTCondorBackend."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import logging
from concurrent.futures import Future

import classad2
from joblib.externals.cloudpickle import cloudpickle as pickle

from joblib_htcondor.backend import _HTCondorBackend


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

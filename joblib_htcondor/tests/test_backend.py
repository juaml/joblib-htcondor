"""Provide tests for _HTCondorBackend."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import logging

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
        "_extra_directives",
        "_worker_log_level",
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
        poll_interval=0.1,
    )
    pickled_backend2 = pickle.loads(pickle.dumps(backend2))
    assert compare_backends(pickled_backend2, backend2)

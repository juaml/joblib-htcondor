"""The joblib htcondor backend implementation."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

import time
import typing as tp
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

import htcondor2
from joblib.parallel import ParallelBackendBase, register_parallel_backend


if tp.TYPE_CHECKING:
    from multiprocessing.pool import AsyncResult

    from joblib import Parallel
    from joblib.parallel import BatchedCalls


__all__ = ["register"]


def register():
    """Register joblib htcondor backend."""
    register_parallel_backend("htcondor", _HTCondorBackend)


@dataclass
class _HTCondorJobMeta:
    """Class for keeping track of HTCondor job submissions."""

    tracking_future: Future
    htcondor_submit: htcondor2.Submit
    htcondor_submit_result: htcondor2.SubmitResult


class _HTCondorBackend(ParallelBackendBase):
    """Class for HTCondor backend for joblib.

    Parameters
    ----------
    pool : str, htcondor2.ClassAd, list of str or None, optional
        Pool to initiate htcondor2.Collector client with (default None).
    schedd : htcondor2.Schedd or None, optional
        Scheduler to use for submitting jobs (default None).
    universe : str, optional
        HTCondor universe to use (default "vanilla").
    python_path : str, optional
        Path to the Python binary (default "python").
    request_cpus : int, optional
        HTCondor CPUs to request (default 1).
    request_memory : str, optional
        HTCondor memory to request (default "8GB").
    request_disk : str, optional
        HTCondor disk to request (default "8G").
    initial_dir : str, optional
        HTCondor initial directory for job. Any HTCondor specific
        macro is evaluated when submitting (default "$ENV(HOME)").
    log_dir_prefix : str, optional
        Prefix for the log directory. Any HTCondor specific macro
        is evaluated when submitting. The directory prefix needs
        to exist before submitting as HTCondor demands
        (default "$(initial_dir)/logs").

    Raises
    ------
    RuntimeError
        If htcondor2.Schedd client cannot be created.

    """

    # supports_inner_max_num_threads = False
    # supports_retrieve_callback = False
    # Set to bypass Parallel._get_sequential_output() trigger in any case
    # inside Parallel.__call__()
    default_n_jobs = 2

    def __init__(
        self,
        pool=None,
        schedd=None,
        universe="vanilla",
        python_path="python",
        request_cpus=1,
        request_memory="8GB",
        request_disk="8GB",
        initial_dir="$ENV(HOME)",
        log_dir_prefix="$(initial_dir)/logs",
    ) -> None:
        super().__init__()

        # condor_submit stuff
        self._universe = universe
        self._python_path = python_path
        self._request_cpus = request_cpus
        self._request_memory = request_memory
        self._request_disk = request_disk
        self._initial_dir = initial_dir
        self._log_dir_prefix = log_dir_prefix

        # Create new scheduler client
        if schedd is None:
            # Try to create a scheduler client using local daemon
            try:
                schedd = htcondor2.Schedd()
            except RuntimeError as err:
                # Initiate collector client
                collector = htcondor2.Collector(pool)
                # Query for scheduler ads
                schedd_ads = collector.query(
                    ad_type=htcondor2.AdType.Schedd,
                    projection=["Name", "MyAddress", "CondorVersion"],
                )
                # Sanity check for scheduler ads
                if not schedd_ads:
                    raise RuntimeError(
                        "Unable to locate local daemon."
                    ) from err
                # Create scheduler client
                self._client = htcondor2.Schedd(schedd_ads[0])

        else:
            self._client = schedd

        # Create placholder for polling thread executor, initialized in
        # start_call() and stopped in stop_call()
        self._polling_thread_executor: tp.Optional[ThreadPoolExecutor] = None

        # Create tracking capabilities for waiting and completed jobs
        self._waiting_jobs_deque: tp.Deque[_HTCondorJobMeta] = deque()
        self._completed_jobs_list: tp.List[_HTCondorJobMeta] = []

        self._n_jobs = self.default_n_jobs

    def effective_n_jobs(self, n_jobs: int) -> int:
        """Guesstimate of actual jobs.

        Parameters
        ----------
        n_jobs : int
            Theoretical number of jobs.

        Returns
        -------
        int
            Actual number of jobs.

        """
        return n_jobs

    def configure(
        self,
        n_jobs: int = 1,
        parallel: tp.Optional[tp.Type["Parallel"]] = None,
        **backend_args: tp.Any,
    ) -> int:
        """Reconfigure the backend and return the number of workers.

        It's called inside joblib.Parallel.__call__() quite early on so should
        set correct self._n_jobs for use.

        Parameters
        ----------
        n_jobs : int, optional
            Number of jobs (default 1).
        parallel : joblib.Parallel instance or None, optional
            The joblib.Parallel instance used (default None).
        **backend_args : any
            Keyword arguments to pass to the backend.

        """
        self._n_jobs = self.effective_n_jobs(n_jobs)
        self.parallel = parallel
        return self._n_jobs

    def apply_async(
        self, func: "BatchedCalls", callback: tp.Optional[tp.Callable] = None
    ) -> tp.Type["AsyncResult"]:
        """Call ``func`` and if provided ``callback`` after ``func`` runs.

        It's called inside joblib.Parallel._dispatch() .

        Parameters
        ----------
        func : joblib.parallel.BatchedCalls
            Batched functions to run.
        callback : callable or None, optional
            Callback to run after ``func`` is complete (default None).

        Returns
        -------
        multiprocess.pool.AsyncResult like object
            The object to update result of calling ``func`` asynchronously.

        """
        # Create Future to provide completion info
        f = Future()
        # Match multiprocessing.pool.AsyncResult
        f.get = f.result
        # Submit to HTCondor
        self._submit(f)

        return f

    def _submit(self, submission_future: Future) -> None:
        """Submit a HTCondor job and adds to waiting jobs deque.

        Parameters
        ----------
        submission_future : concurrent.futures.Future
            The Future to attach to job submission.

        """
        # Submit to htcondor
        submit = htcondor2.Submit(
            {
                "universe": self._universe,
                "executable": (
                    f"{self._python_path} -m joblib_htcondor.executor"
                ),
                # TODO: add pickle file
                "arguments": "",
                "request_cpus": self._request_cpus,
                "request_memory": self._request_memory,
                "request_disk": self._request_disk,
                # "initial_dir": self._initial_dir,
                "transfer_executable": "False",
                "log": (
                    f"{self._log_dir_prefix}/"
                    "$(ClusterId).$(ProcId).joblib.log"
                ),
                "output": (
                    f"{self._log_dir_prefix}/"
                    "$(ClusterId).$(ProcId).joblib.out"
                ),
                "error": (
                    f"{self._log_dir_prefix}/"
                    "$(ClusterId).$(ProcId).joblib.err"
                ),
                "+JobBatchName": "joblib_htcondor",
            }
        )
        submit_result = self._client.submit(submit, count=self._n_jobs)

        self._waiting_jobs_deque.append(
            _HTCondorJobMeta(
                tracking_future=submission_future,
                htcondor_submit=submit,
                htcondor_submit_result=submit_result,
            )
        )

    def _poller(self) -> None:
        """Long poller for job tracking via schedd query.

        Parameters
        ----------
        future : concurrent.futures.Future
            The future to set after polling.
        submit_result : htcondor2.SubmitResult
            The result submission object to query with for polling.

        """
        # Start with an initial sleep so that jobs can get submitted
        time.sleep(10)  # 10 secs
        poll = True
        while poll:
            # Query schedd
            query_result = [
                (
                    result.lookup("ClusterId"),
                    result.lookup("ProcId"),
                    result.lookup("JobStatus"),
                )
                for result in self._client.query(
                    constraint="JobBatchName =?= joblib_htcondor",
                    projection=["ClusterId", "ProcId", "JobStatus"],
                )
            ]
            # All jobs are done
            if not query_result:
                poll = False
            # Update tracking
            else:
                # Make set for cluster ids
                cluster_ids = {res[0] for res in query_result}
                # Iterate over waiting jobs to see if they are in the query
                # result; if not, move them to completed jobs
                # Make new list to track job metas to remove from deque
                to_remove = []
                for job_meta in self._waiting_jobs_deque:
                    # Job is done
                    if (
                        job_meta.htcondor_submit_result.cluster()
                        not in cluster_ids
                    ):
                        # Add to list to remove
                        to_remove.append(job_meta)
                        # Add to completed list
                        self._completed_jobs_list.append(job_meta)

                # Remove completed jobs
                if to_remove:
                    for item in to_remove:
                        self._waiting_jobs_deque.remove(item)

                # Sleep for 60 secs
                time.sleep(60)

    def start_call(self) -> None:
        """Start resources before actual computation."""
        self._continue = True
        self._polling_thread_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="schedd_poll",
        )
        # Initialize long polling
        # TODO: create shared data dir during initialization
        self._polling_thread_executor.submit(self._poller)

    def stop_call(self) -> None:
        """Stop resources after actual computation."""
        self._continue = False
        self._polling_thread_executor.shutdown()
        # TODO: delete shared data dir after poller shutdown

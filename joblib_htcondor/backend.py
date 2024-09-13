"""The joblib htcondor backend implementation."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

import logging
import shutil
import sys
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Deque, List, Optional, Type
from uuid import uuid1

import htcondor2
from joblib.parallel import (
    AutoBatchingMixin,
    ParallelBackendBase,
    register_parallel_backend,
)

from .delayed_submission import DelayedSubmission


if TYPE_CHECKING:
    from multiprocessing.pool import AsyncResult

    from joblib import Parallel
    from joblib.parallel import BatchedCalls


__all__ = ["register"]

logger = logging.getLogger("joblib_htcondor.backend")


def register():
    """Register joblib htcondor backend."""
    register_parallel_backend("htcondor", _HTCondorBackend)


@dataclass
class _HTCondorJobMeta:
    """Class for keeping track of HTCondor job submissions."""

    tracking_future: Future
    htcondor_submit: htcondor2.Submit
    htcondor_submit_result: Optional[htcondor2.SubmitResult]
    callback: Optional[Callable]
    pickle_fname: Path

    def __repr__(self) -> str:
        submit_info = ""
        if self.htcondor_submit_result:
            submit_info = (
                f"[ClusterId: {self.htcondor_submit_result.cluster()}]"
            )

        out = (
            "<HTCondorJobMeta("
            f"pickle_fname={self.pickle_fname})"
            f"{submit_info}>"
        )
        return out


class _HTCondorBackend(AutoBatchingMixin, ParallelBackendBase):
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
        Path to the Python binary (defaults to current python executable).
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
    poll_interval : int, optional
        Interval in seconds to poll the scheduler for job status
        (default 5).
    shared_data_dir : str or Path, optional
        Directory to store shared data between jobs (defaults to current
        working directory).

    Raises
    ------
    RuntimeError
        If htcondor2.Schedd client cannot be created.

    """

    # supports_inner_max_num_threads = False
    # supports_retrieve_callback = False
    # Set to bypass Parallel._get_sequential_output() trigger in any case
    # inside Parallel.__call__()
    default_n_jobs = 1

    def __init__(
        self,
        pool=None,
        schedd=None,
        universe="vanilla",
        python_path=None,
        request_cpus=1,
        request_memory="8GB",
        request_disk="8GB",
        initial_dir="$ENV(HOME)",
        log_dir_prefix="$(initial_dir)/logs",
        poll_interval=5,
        shared_data_dir=None,
    ) -> None:
        super().__init__()
        logger.debug("Initializing HTCondor backend.")

        if shared_data_dir is None:
            shared_data_dir = Path.cwd() / "joblib_htcondor_shared_data"

        if python_path is None:
            python_path = sys.executable
        # Make shared data directory if doest no exist
        shared_data_dir.mkdir(exist_ok=True, parents=True)
        # condor_submit stuff
        self._universe = universe
        self._python_path = python_path
        self._request_cpus = request_cpus
        self._request_memory = request_memory
        self._request_disk = request_disk
        self._initial_dir = initial_dir
        self._log_dir_prefix = log_dir_prefix
        self._poll_interval = poll_interval
        self._shared_data_dir = shared_data_dir

        logging.debug(f"Universe: {self._universe}")
        logging.debug(f"Python path: {self._python_path}")
        logging.debug(f"Request CPUs: {self._request_cpus}")
        logging.debug(f"Request memory: {self._request_memory}")
        logging.debug(f"Request disk: {self._request_disk}")
        logging.debug(f"Initial dir: {self._initial_dir}")
        logging.debug(f"Log dir prefix: {self._log_dir_prefix}")
        logging.debug(f"Poll interval: {self._poll_interval}")
        logging.debug(f"Shared data dir: {self._shared_data_dir}")

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
                schedd = htcondor2.Schedd(schedd_ads[0])

        self._client = schedd

        # Create placholder for polling thread executor, initialized in
        # start_call() and stopped in stop_call()
        self._polling_thread_executor: Optional[ThreadPoolExecutor] = None

        # Create tracking capabilities for queued, waiting and completed jobs
        self._queued_jobs_list: Deque[_HTCondorJobMeta] = deque()
        self._waiting_jobs_deque: Deque[_HTCondorJobMeta] = deque()
        self._completed_jobs_list: List[_HTCondorJobMeta] = []

        self._n_jobs = self.default_n_jobs

        # Set some initial values for job scheduling
        self._current_shared_data_dir = Path()
        self._next_task_id = 0
        self._this_batch_name = "missingbatchname"
        logger.debug("HTCondor backend initialised.")

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
        parallel: Optional[Type["Parallel"]] = None,
        **backend_args: Any,
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
        logger.debug("Configuring HTCondor backend.")
        logger.debug(f"n_jobs: {n_jobs}")
        logger.debug(f"parallel: {parallel}")
        logger.debug(f"backend_args: {backend_args}")
        self._n_jobs = self.effective_n_jobs(n_jobs)
        self.parallel = parallel

        # Create unique id for job batch
        self._uuid = uuid1().hex
        logger.debug(f"UUID: {self._uuid}")
        self._this_batch_name = f"joblib_htcondor-{self._uuid}"
        logger.debug(f"Batch name: {self._this_batch_name}")
        logger.debug("HTCondor backend configured.")
        return self._n_jobs

    def apply_async(
        self, func: "BatchedCalls", callback: Optional[Callable] = None
    ) -> Type["AsyncResult"]:
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
        self._submit(f, func=func, callback=callback)

        return f

    def _submit(
        self,
        submission_future: Future,
        func: "BatchedCalls",
        callback: Optional[Callable] = None,
    ) -> None:
        """Submit a HTCondor job and adds to waiting jobs deque.

        Parameters
        ----------
        submission_future : concurrent.futures.Future
            The Future to attach to job submission.
        func : joblib.parallel.BatchedCalls
            The function to submit.
        callback : callable or None, optional
            The callback to run after the function is complete (default None).

        """
        # Pickle the function to a file
        pickle_fname = (
            self._current_shared_data_dir / f"task-{self._next_task_id}.pickle"
        )
        self._next_task_id += 1

        # Create the DelayedSubmission object
        ds = DelayedSubmission(func)
        ds.dump(pickle_fname)

        arguments = "-m joblib_htcondor.executor" f" {pickle_fname.as_posix()}"
        # Creat the job submission dictionary
        submit_dict = {
            "universe": self._universe,
            "executable": f"{self._python_path}",
            "arguments": arguments,
            "request_cpus": self._request_cpus,
            "request_memory": self._request_memory,
            "request_disk": self._request_disk,
            "initial_dir": self._initial_dir,
            "transfer_executable": "False",
            "log": (
                f"{self._log_dir_prefix}/" "$(ClusterId).$(ProcId).joblib.log"
            ),
            "output": (
                f"{self._log_dir_prefix}/" "$(ClusterId).$(ProcId).joblib.out"
            ),
            "error": (
                f"{self._log_dir_prefix}/" "$(ClusterId).$(ProcId).joblib.err"
            ),
            "+JobBatchName": f'"joblib_htcondor-{self._uuid}"',
        }

        logger.debug(f"Submit dict: {submit_dict}")
        # Createthe Submit to htcondor
        submit = htcondor2.Submit(submit_dict)

        # Add to queue so the poller can take care of it
        self._queued_jobs_list.append(
            _HTCondorJobMeta(
                tracking_future=submission_future,
                htcondor_submit=submit,
                htcondor_submit_result=None,
                callback=callback,
                pickle_fname=pickle_fname,
            )
        )

    def _watcher(self) -> None:
        """Long poller for job tracking via schedd query.

        Parameters
        ----------
        future : concurrent.futures.Future
            The future to set after polling.
        submit_result : htcondor2.SubmitResult
            The result submission object to query with for polling.

        """
        # Start with an initial sleep so that jobs can get submitted
        last_poll = time.time()
        while self._continue:
            # First check if there are any queued jobs to be submitted
            while self._queued_jobs_list:
                to_submit = self._queued_jobs_list.popleft()
                # Submit job
                to_submit.htcondor_submit_result = self._client.submit(
                    to_submit.htcondor_submit,
                    count=1,
                )
                # Move to waiting jobs
                self._waiting_jobs_deque.append(to_submit)

            # Second, if enough time passed, poll the schedd to see if any
            # jobs are done.
            if time.time() - last_poll > self._poll_interval:
                self._poll_jobs()
                last_poll = time.time()

        # Clean up running jobs in case we were shutted down

    def _poll_jobs(self) -> None:
        logger.debug("Polling HTCondor jobs.")
        # Query schedd
        query_result = [
            (
                result.lookup("ClusterId"),
                result.lookup("ProcId"),
                result.lookup("JobStatus"),
            )
            for result in self._client.query(
                constraint="JobBatchName =?= \"{self._this_batch_name}\"",
                projection=["ClusterId", "ProcId", "JobStatus"],
            )
        ]
        logger.debug(f"Query result: {query_result}")
        if (
            not query_result and
            len(self._waiting_jobs_deque) == 0
            and len(self._queued_jobs_list) == 0
        ):
            # No jobs to track, stop polling
            logger.debug("No jobs to track, stopping polling.")
            self._continue = False
        else:
            # Make set for cluster ids
            cluster_ids = {res[0] for res in query_result}
            # Iterate over waiting jobs to see if they are in the query
            # result; if not, move them to completed jobs
            # Make new list to track job metas to remove from deque
            logger.debug(f"Cluster ids: {cluster_ids}")
            to_remove = []
            for job_meta in self._waiting_jobs_deque:
                logger.debug(f"Checking job: {job_meta}")
                # Job is done
                if (
                    job_meta.htcondor_submit_result.cluster()
                    not in cluster_ids
                ):
                    logger.debug(f"Job {job_meta} is done.")
                    # Add to list to remove
                    to_remove.append(job_meta)
                    # Add to completed list
                    self._completed_jobs_list.append(job_meta)
                    ds = DelayedSubmission.load(job_meta.pickle_fname)
                    result = ds.result()
                    if ds.error():
                        logger.debug(f"Job {job_meta} raised an exception.")
                        typ, exc, tb = result
                        job_meta.tracking_future.set_exception(exc)
                    else:
                        logger.debug(f"Job {job_meta} completed successfully.")
                        job_meta.tracking_future.set_result(result)
                        if job_meta.callback:
                            job_meta.callback(result)

            # Remove completed jobs
            if to_remove:
                logger.debug(f"Removing jobs from waiting list: {to_remove}")
                for item in to_remove:
                    self._waiting_jobs_deque.remove(item)
        logger.debug("Polling HTCondor jobs done.")

    def start_call(self) -> None:
        """Start resources before actual computation."""
        logger.debug("Starting HTCondor backend.")
        self._continue = True
        self._polling_thread_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="schedd_poll",
        )
        # Initialize long polling
        self._current_shared_data_dir = (
            self._shared_data_dir / f"{self._this_batch_name}"
        )
        self._current_shared_data_dir.mkdir(exist_ok=True, parents=True)
        self._polling_thread_executor.submit(self._watcher)
        self._next_task_id = 1

    def stop_call(self) -> None:
        logger.debug("Stopping HTCondor backend.")
        """Stop resources after actual computation."""
        self._continue = False
        self._polling_thread_executor.shutdown()
        shutil.rmtree(self._current_shared_data_dir)

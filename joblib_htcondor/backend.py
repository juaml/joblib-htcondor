"""The joblib htcondor backend implementation."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
#          Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import json
import logging
import shutil
import signal
import sys
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)
from uuid import uuid1

import htcondor2
from joblib.parallel import (
    ParallelBackendBase,
    SequentialBackend,
    register_parallel_backend,
)

from .delayed_submission import DelayedSubmission
from .logging import logger


if TYPE_CHECKING:
    from multiprocessing.pool import AsyncResult

    from classad2 import ClassAd
    from joblib import Parallel
    from joblib.parallel import BatchedCalls


__all__ = ["register"]


def register() -> None:
    """Register joblib htcondor backend."""
    register_parallel_backend("htcondor", _HTCondorBackend)


@dataclass
class _BackendMeta:
    """Class for metadata I/O.

    This class defines the interface for writing and reading metadata files
    stored as JSON in the filesystem. Files are written by the backend and
    used by the UI machinery.

    """

    uuid: str
    parent: Optional[str] = field(default=None)  # Parent backend batch id
    recursion_level: int = field(default=0)  # Recursion level of the backend
    throttle: int = 0  # Throttle value for the backend
    shared_data_dir: Optional[Path] = field(
        default=None
    )  # Shared data directory for the backend
    n_tasks: int = field(default=0)  # Number of tasks received
    start_timestamp: datetime = field(
        default_factory=datetime.now
    )  # Start timestamp of the backend
    update_timestamp: datetime = field(
        default_factory=datetime.now
    )  # Update timestamp of the backend

    def asdict(self) -> Dict[str, Any]:
        """Represent object as dictionary.

        Returns
        -------
        dict
            The object as a dictionary.

        """
        return {
            "uuid": self.uuid,
            "parent": self.parent,
            "recursion_level": self.recursion_level,
            "throttle": self.throttle,
            "shared_data_dir": self.shared_data_dir.as_posix()
            if self.shared_data_dir is not None
            else None,
            "n_tasks": self.n_tasks,
            "start_timestamp": self.start_timestamp.isoformat(),
            "update_timestamp": self.update_timestamp.isoformat(),
        }

    @classmethod
    def from_json(
        cls: Type["_BackendMeta"], data: Dict[str, Any]
    ) -> "_BackendMeta":
        """Load object from JSON.

        Parameters
        ----------
        cls : _BackendMeta instance
            The type of instance to create.
        data : dict
            The data to initialise object with.

        Returns
        -------
        _BackendMeta instance
            The initialised object instance.

        """
        return cls(
            uuid=data["uuid"],
            parent=data["parent"],
            recursion_level=data["recursion_level"],
            throttle=data["throttle"],
            shared_data_dir=Path(data["shared_data_dir"]),
            n_tasks=data["n_tasks"],
            start_timestamp=datetime.fromisoformat(data["start_timestamp"]),
            update_timestamp=datetime.fromisoformat(data["update_timestamp"]),
        )


@dataclass
class _HTCondorJobMeta:
    """Class for keeping track of HTCondor job submissions."""

    tracking_future: Future
    htcondor_submit: htcondor2.Submit
    htcondor_submit_result: Optional[htcondor2.SubmitResult]
    callback: Optional[Callable]
    pickle_fname: Path
    delayed_submission: DelayedSubmission

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

    def cluster_id(self):
        """Get the cluster id of the job."""
        if self.htcondor_submit_result:
            return self.htcondor_submit_result.cluster()
        return None


# TODO:
# 1) Think about which parameters should not be optional: request_cpus and
# request_memory should be mandatory.
#
# 2) parent_uuid and recursion_level does not make any sense in the
# constructor. However, they are needed for pickle to load the object from
# disk. Maybe there's another way.


class _HTCondorBackend(ParallelBackendBase):
    """Class for HTCondor backend for joblib.

    Parameters
    ----------
    pool : str, classad2.ClassAd, list of str or None, optional
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
    initial_dir : str or pathlib.Path or None, optional
        HTCondor initial directory for job. If None, will resolve to current
        working directory (default None).
    log_dir_prefix : str or None, optional
        Prefix for the log directory. The directory prefix needs
        to exist before submitting as HTCondor demands. If None, will resolve
        to `"`initial_dir`/logs/<generated unique batch name>"`
        (default None).
    poll_interval : int, optional
        Interval in seconds to poll the scheduler for job status
        (default 5).
    shared_data_dir : str or pathlib.Path or None, optional
        Directory to store shared data between jobs. If None, will resolve to
        `"<current working directory>/joblib_htcondor_shared_data"`
        (default None).
    extra_directives : dict or None, optional
        Extra directives to pass to the HTCondor submit file (default None).
    worker_log_level : int, optional
        Log level for the worker (default is logger.WARNING).
    throttle : int or list of int or None, optional
        Throttle the number of jobs submitted at once. If list, the first
        element is the throttle for the current level and the rest are
        for the nested levels (default None).
    recursion_level : int, optional
        Recursion level of the backend. With each nested
        call, the recursion level increases by 1 (default 0).
    max_recursion_level : int, optional
        Maximum recursion level of the backend. Once the
        recursion level reaches this value, the backend will switch to a
        Sequential backend. If -1, the switching is disabled (default -1).
    parent_uuid : str or None, optional
        UUID of the parent backend (default None).

    Raises
    ------
    ValueError
        If `throttle` is an empty list.
    RuntimeError
        If htcondor2.Schedd client cannot be created.

    """

    def __init__(
        self,
        pool: Union[str, "ClassAd", List[str], None] = None,
        schedd: Optional[htcondor2.Schedd] = None,
        universe: str = "vanilla",
        python_path: Optional[str] = None,
        request_cpus: int = 1,
        request_memory: str = "8GB",
        request_disk: str = "0GB",
        initial_dir: Union[str, Path, None] = None,
        log_dir_prefix: Optional[str] = None,
        poll_interval: int = 5,
        shared_data_dir: Union[str, Path, None] = None,
        extra_directives: Optional[Dict] = None,
        worker_log_level: int = logging.INFO,
        throttle: Union[int, List[int], None] = None,
        recursion_level: int = 0,
        max_recursion_level: int = -1,
        parent_uuid: Optional[str] = None,
    ) -> None:
        super().__init__()

        logger.debug("Initializing HTCondor backend.")
        # Set initial directory
        if initial_dir is None:
            initial_dir = Path.cwd()
        # Set shared data directory
        if shared_data_dir is None:
            shared_data_dir = Path.cwd() / "joblib_htcondor_shared_data"
        # Set Python executable
        if python_path is None:
            python_path = sys.executable
        # Make shared data directory if doesn't exist
        shared_data_dir.mkdir(exist_ok=True, parents=True)

        # condor_submit stuff
        self._pool = pool
        self._schedd = schedd
        self._universe = universe
        self._python_path = python_path
        self._request_cpus = request_cpus
        self._request_memory = request_memory
        self._request_disk = request_disk
        self._initial_dir = initial_dir
        self._log_dir_prefix = log_dir_prefix
        self._poll_interval = poll_interval
        self._shared_data_dir = shared_data_dir
        self._extra_directives = extra_directives
        self._worker_log_level = worker_log_level
        self._recursion_level = recursion_level
        self._max_recursion_level = max_recursion_level
        self._parent_uuid = parent_uuid

        # Check and set throttle
        if isinstance(throttle, list):
            if len(throttle) == 1:
                self._throttle = throttle[0]
            elif len(throttle) == 0:
                raise ValueError(
                    "Throttle parameter must have at least one value."
                )
        self._throttle = throttle

        logger.debug(f"Universe: {self._universe}")
        logger.debug(f"Python path: {self._python_path}")
        logger.debug(f"Request CPUs: {self._request_cpus}")
        logger.debug(f"Request memory: {self._request_memory}")
        logger.debug(f"Request disk: {self._request_disk}")
        logger.debug(f"Initial dir: {self._initial_dir}")
        logger.debug(f"Log dir prefix: {self._log_dir_prefix}")
        logger.debug(f"Poll interval: {self._poll_interval}")
        logger.debug(f"Shared data dir: {self._shared_data_dir}")
        logger.debug(f"Extra directives: {self._extra_directives}")
        logger.debug(f"Worker log level: {self._worker_log_level}")
        logger.debug(f"Throttle: {self._throttle}")
        logger.debug(f"Recursion level: {self._recursion_level}")
        logger.debug(f"Max recursion level: {self._max_recursion_level}")
        logger.debug(f"Parent UUID: {self._parent_uuid}")

        # Create new scheduler client
        if schedd is None:
            # Try to create a scheduler client using local daemon
            try:
                schedd = htcondor2.Schedd()
            except RuntimeError as err:
                # Initiate collector client
                collector = htcondor2.Collector(self._pool)
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

        # Create placeholder for polling thread executor, initialized in
        # start_call() and stopped in stop_call()
        self._polling_thread_executor: Optional[ThreadPoolExecutor] = None

        # Create tracking capabilities for queued, waiting and completed jobs
        self._queued_jobs_list: Deque[_HTCondorJobMeta] = deque()
        self._waiting_jobs_deque: Deque[_HTCondorJobMeta] = deque()
        self._completed_jobs_list: List[_HTCondorJobMeta] = []

        self._n_jobs = 1
        self._backend_meta = None

        # Set some initial values for job scheduling
        self._current_shared_data_dir = Path()
        self._next_task_id = 0
        self._this_batch_name = "missingbatchname"
        logger.debug("HTCondor backend initialised.")

    def write_metadata(self):
        """Write metadata to a file."""
        if self._backend_meta is None:
            meta = _BackendMeta(
                uuid=self._this_batch_name,
                parent=self._parent_uuid,
                recursion_level=self._recursion_level,
                throttle=self.get_current_throttle(),
                shared_data_dir=self._current_shared_data_dir,
                n_tasks=self._next_task_id - 1,
            )
            self._backend_meta = meta
        else:
            self._backend_meta.update_timestamp = datetime.now()
            self._backend_meta.n_tasks = self._next_task_id - 1
        meta_dir = self._shared_data_dir / ".jht-meta"
        meta_dir.mkdir(exist_ok=True, parents=True)
        meta_fname = meta_dir / f"{self._this_batch_name}.json"
        with meta_fname.open("w") as f:
            json.dump(self._backend_meta.asdict(), f)

    def get_nested_backend(self) -> Tuple["ParallelBackendBase", int]:
        """Return the nested backend and the number of workers.

        Returns
        -------
        ParallelBackendBase
            The nested backend.
        int
            The number of jobs.

        """
        if self._recursion_level == self._max_recursion_level:
            logger.info(
                "Maximum recursion level reached. Switching to Sequential "
                "backend."
            )
            return SequentialBackend(
                nesting_level=self._recursion_level
            ), self._n_jobs

        if self._poll_interval < 1:
            logger.warning(
                "You are about to use nested parallel calls. "
                "Poll interval is less than 1 second. This could lead to "
                "high load on the filesystem and/or scheduler. Please "
                "consider increasing the poll interval to a few seconds "
                "as this will not have any considerable effect on the "
                "throughput."
            )
        throttle = self._throttle
        if not isinstance(throttle, list):
            logger.warning(
                "You are about to use nested parallel calls and the throttle "
                "parameter you set would not have the desired effect. "
                "In nested parallel calls, each worker in the parent parallel"
                "will throttle the number of jobs submitted to the child "
                "with the same settings. For example, if you set "
                "`throttle=10` in the parent, then each child will submit 10 "
                "jobs at a time, leading to a total of 110 jobs submitted at "
                "once, which is far from the selected value of 10. In order "
                "to manipulate the thottle value for each level, you need to "
                "set throttle as a list of integers."
            )
        elif len(throttle) > 1:
            throttle = throttle[1:]

        return _HTCondorBackend(
            pool=self._pool,
            schedd=self._schedd,
            universe=self._universe,
            python_path=self._python_path,
            request_cpus=self._request_cpus,
            request_memory=self._request_memory,
            request_disk=self._request_disk,
            initial_dir=self._initial_dir,
            log_dir_prefix=self._log_dir_prefix,
            poll_interval=self._poll_interval,
            shared_data_dir=self._shared_data_dir,
            extra_directives=self._extra_directives,
            worker_log_level=self._worker_log_level,
            throttle=throttle,
            recursion_level=self._recursion_level + 1,
            max_recursion_level=self._max_recursion_level,
            parent_uuid=self._this_batch_name,
        ), -1

    def __reduce__(self) -> Tuple[Type["_HTCondorBackend"], Tuple]:
        return (
            _HTCondorBackend,
            (
                self._pool,
                self._schedd,
                self._universe,
                self._python_path,
                self._request_cpus,
                self._request_memory,
                self._request_disk,
                self._initial_dir,
                self._log_dir_prefix,
                self._poll_interval,
                self._shared_data_dir,
                self._extra_directives,
                self._worker_log_level,
                self._throttle,
                self._recursion_level,
                self._max_recursion_level,
                self._parent_uuid,
            ),
        )

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
        if n_jobs == -1:
            if self._throttle is None:
                logger.warning(
                    "Setting HTCondor backend n_jobs to -1 does not make much "
                    "sense. We cannot determine the effective number of jobs "
                    "as HTCondor is actually a queuing system. Ideally, you "
                    "should set n_jobs to the number of total jobs you will "
                    "queue, or an arbitrary large number.\n"
                    "IMPORTANT: the number of jobs that joblib with submit "
                    "depends on the pre_dispatch parameter of Parallel, which "
                    "is usually a function of n_jobs. You need to consider "
                    "that the data transfer is done through the filesystem, "
                    "so an extremely large number of jobs with large data "
                    "could lead to a large shared storage requirement. "
                    "If you set n_jobs to a fixed number, joblib will submit "
                    "in batches of that number of jobs, so you won't exploit "
                    "the full potential of this backend. What you might want "
                    "is to throttle the number of jobs submitted at once, "
                    "which can be done with the throttle parameter of the "
                    "backend. As a sane default, we will set the throttle to "
                    "1000 and the number of jobs as the maximum possible "
                    "number of jobs."
                )
                self._throttle = 1000
            n_jobs = sys.maxsize // 2
            logger.info(
                f"Setting n_jobs to {n_jobs} but "
                f"throttling to {self._throttle}"
            )
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
        logger.debug(f"\tn_jobs: {n_jobs}")
        logger.debug(f"\tparallel: {parallel}")
        logger.debug(f"\tbackend_args: {backend_args}")
        self._n_jobs = self.effective_n_jobs(n_jobs)
        self.parallel = parallel

        # Create unique id for job batch
        self._uuid = uuid1().hex
        logger.debug(f"\tUUID: {self._uuid}")
        self._this_batch_name = f"jht-{self._uuid}-l{self._recursion_level}"
        logger.info(f"\tBatch name: {self._this_batch_name}")
        logger.debug("HTCondor backend configured.")

        if self._log_dir_prefix is None:
            self._current_log_dir_prefix = (
                f"{self._initial_dir}/logs/{self._this_batch_name}"
            )
        else:
            self._current_log_dir_prefix = self._log_dir_prefix

        logger.info(
            f"Setting log dir prefix to {self._current_log_dir_prefix}"
        )
        log_path = Path(self._current_log_dir_prefix)
        if not log_path.exists() and log_path.is_absolute():
            log_path.mkdir(parents=True)

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
        f.get = f.result  # type: ignore
        # Submit to queue
        self._submit(f, func=func, callback=callback)
        # Write metadata
        self.write_metadata()
        return f  # type: ignore

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
                f"{self._current_log_dir_prefix}/$(ClusterId).$(ProcId).joblib.log"
            ),
            "output": (
                f"{self._current_log_dir_prefix}/$(ClusterId).$(ProcId).joblib.out"
            ),
            "error": (
                f"{self._current_log_dir_prefix}/$(ClusterId).$(ProcId).joblib.err"
            ),
            "+JobBatchName": f'"jht-{self._uuid}"',
            "+JobPrio": f"{self._recursion_level}",
        }
        if self._extra_directives:
            submit_dict.update(self._extra_directives)

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
                delayed_submission=ds,
            )
        )

    def get_current_throttle(self) -> int:
        """Get the current throttle value.

        Returns
        -------
        int
            The current throttle value.

        """
        out = sys.maxsize
        if isinstance(self._throttle, int):
            out = self._throttle
        elif isinstance(self._throttle, list):
            out = self._throttle[0]
        return out

    def _watcher(self) -> None:
        """Long poller for job tracking.

        Parameters
        ----------
        future : concurrent.futures.Future
            The future to set after polling.
        submit_result : htcondor2.SubmitResult
            The result submission object to query with for polling.

        """
        last_poll = time.time()
        throttle = self.get_current_throttle()
        logger.info("Starting HTCondor backend watcher.")
        logger.info(f"Polling every {self._poll_interval} seconds.")
        logger.info(f"Throttle set to {throttle}.")
        while self._continue:
            if (time.time() - last_poll) > self._poll_interval:
                # Enough time passed, poll the jobs
                n_running = self._poll_jobs()
                last_poll = time.time()
                if n_running < throttle:
                    # We don't have enough jobs int he condor queue, submit
                    newly_queued = 0
                    to_queue = throttle - n_running
                    # First check if there are any queued jobs to be submitted
                    while self._queued_jobs_list and newly_queued < to_queue:
                        to_submit = self._queued_jobs_list.popleft()
                        # Dump pickle file
                        to_submit.delayed_submission.dump(
                            to_submit.pickle_fname
                        )
                        # Submit job
                        to_submit.htcondor_submit_result = self._client.submit(
                            to_submit.htcondor_submit,
                            count=1,
                        )
                        # Move to waiting jobs
                        self._waiting_jobs_deque.append(to_submit)
                        newly_queued += 1
            # logger.debug("Waiting 0.1 seconds")
            time.sleep(0.1)

    def _poll_jobs(self) -> int:
        """Poll the schedd for job status.

        Returns
        -------
        int
            number of jobs that are queued or running.

        """
        logger.debug("Polling HTCondor jobs.")

        # If we are cancelled, stop the backend so all the jobs are cancelled
        if (
            self._next_task_id > 1  # at least one job was queued
            and len(self._waiting_jobs_deque) == 0  # nothing waiting
            and len(self._queued_jobs_list) == 0  # nothing scheduled
        ):
            # No jobs to track, stop polling
            logger.debug("No jobs to track, stopping polling.")
            self._continue = False
        else:
            done_jobs = []
            # Query output files
            for job_meta in self._waiting_jobs_deque:
                logger.log(level=9, msg=f"Checking job {job_meta}")
                out_fname = job_meta.pickle_fname.with_stem(
                    f"{job_meta.pickle_fname.stem}_out"
                )
                if out_fname.exists():
                    logger.log(level=9, msg=f"Job {job_meta} is done.")
                    done_jobs.append(job_meta)
                    out_fname = job_meta.pickle_fname.with_stem(
                        f"{job_meta.pickle_fname.stem}_out"
                    )
                    # Load the DelayedSubmission object
                    ds = DelayedSubmission.load(out_fname)
                    result = ds.result()
                    if ds.error():
                        logger.log(
                            level=9,
                            msg=f"Job {job_meta} raised an exception.",
                        )
                        typ, exc, tb = result
                        job_meta.tracking_future.set_exception(exc)
                    else:
                        logger.log(
                            level=9,
                            msg=f"Job {job_meta} completed successfully.",
                        )
                        logger.log(level=9, msg=f"Result: {result}")
                        job_meta.tracking_future.set_result(result)
                        if job_meta.callback:
                            job_meta.callback(result)

                        # Free up resources
                        out_fname.unlink()
                        job_meta.pickle_fname.unlink()
                        del ds

                        # Add to completed list
                        self._completed_jobs_list.append(job_meta)

            # Remove completed jobs
            if len(done_jobs) > 0:
                logger.debug(f"Removing jobs from waiting list: {done_jobs}")
                for job_meta in done_jobs:
                    self._waiting_jobs_deque.remove(job_meta)

        logger.debug("Polling HTCondor jobs done.")
        return len(self._waiting_jobs_deque)

    def start_call(self) -> None:
        """Start resources before actual computation."""
        logger.debug("Starting HTCondor backend.")
        # Set flag for joblib
        self._continue = True
        # Start polling thread executor
        self._polling_thread_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="schedd_poll",
        )
        # Create shared data directory
        self._current_shared_data_dir = (
            self._shared_data_dir / f"{self._this_batch_name}"
        )
        logger.info(
            f"Setting shared data dir to {self._current_shared_data_dir}"
        )
        self._current_shared_data_dir.mkdir(exist_ok=True, parents=True)
        # Initialize long polling
        self._polling_thread_executor.submit(self._watcher)
        # Set task ID for pickle file name
        self._next_task_id = 1
        # Write metadata file
        self.write_metadata()
        # Register custom handler for SIGTERM;
        # If we are cancelled, stop the backend so all the jobs are cancelled
        signal.signal(signal.SIGTERM, self._sigterm_handler)

    def stop_call(self) -> None:
        """Stop resources after actual computation."""
        logger.debug("Stopping HTCondor backend.")
        # Cleanup
        self._sigterm_handler(None, None)

    def _sigterm_handler(self, signum, stackframe) -> None:
        """Handle SIGTERM."""
        # Set flag for joblib
        self._continue = False
        # Shutdown polling thread executor
        self._polling_thread_executor.shutdown()  # type: ignore
        # Cancel unfinished (idle, on-hold or running ) jobs
        self._cancel_jobs()
        # Remove shared data directory
        shutil.rmtree(self._current_shared_data_dir)

    def _cancel_jobs(self) -> None:
        """Cancel idle, on-hold or running jobs."""
        logger.debug("Cancelling HTCondor jobs.")
        # Query schedd
        query_result = [
            f"{result.lookup('ClusterId')}.{result.lookup('ProcId')}"
            for result in self._client.query(
                constraint=f'JobBatchName =?= "{self._this_batch_name}"',
                projection=["ClusterId", "ProcId"],
            )
        ]
        # Cancel jobs
        if len(query_result) > 0:
            logger.debug(f"Cancelling: {query_result}")
            _ = self._client.act(
                action=htcondor2.JobAction.Remove,
                job_spec=query_result,
                reason="Cancelled by htcondor_joblib",
            )
            logger.debug("HTCondor jobs cancelled.")

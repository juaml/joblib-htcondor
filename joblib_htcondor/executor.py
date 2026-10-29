"""Run pickled DelayedSubmission objects."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
#          Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

import logging
import pickle
import time
from datetime import datetime
from pathlib import Path

from joblib_htcondor.delayed_submission import DelayedSubmission


logger = logging.getLogger("joblib_htcondor.executor")

# Number of times to try loading the task before giving up. A file in the
# shared filesystem can look incomplete for a short time.
_LOAD_ATTEMPTS = 10


def _load_task(fname: Path, lock_lifetime: int) -> DelayedSubmission:
    """Load the task, retrying if the file cannot be unpickled.

    Parameters
    ----------
    fname : pathlib.Path
        The file to load the DelayedSubmission object from.
    lock_lifetime : int
        The lifetime of the lock on the file, in seconds.

    Returns
    -------
    DelayedSubmission
        The loaded DelayedSubmission object.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    pickle.UnpicklingError
        If the file could not be unpickled after all the attempts.

    """
    if not fname.exists():
        raise FileNotFoundError(f"File {fname} not found.")
    for attempt in range(1, _LOAD_ATTEMPTS + 1):
        ds = DelayedSubmission.load(fname, lock_lifetime=lock_lifetime)
        if ds is not None:
            return ds
        if attempt < _LOAD_ATTEMPTS:
            logger.warning(
                f"Could not load DelayedSubmission object from {fname}. "
                "Retrying in 1 second."
            )
            time.sleep(1)  # Wait 1 second before retrying
    raise pickle.UnpicklingError(
        f"Could not load the task from {fname} after {_LOAD_ATTEMPTS} "
        "attempts, the file is corrupt."
    )


def _dump_result(ds: DelayedSubmission, out_fname: Path) -> None:
    """Dump the result of the task, or the error if it cannot be pickled.

    Parameters
    ----------
    ds : DelayedSubmission
        The DelayedSubmission object to dump.
    out_fname : pathlib.Path
        The file to dump the result to.

    """
    logger.info(f"Dumping DelayedSubmission (result only) to {out_fname}")
    dumped = False
    replaced_result = False
    while not dumped:
        try:
            dumped = ds.dump(out_fname, result_only=True)
        except Exception as e:
            if replaced_result:
                # Not even the error can be pickled, give up
                raise
            # The result cannot be pickled and retrying will not help, so
            # send the error back instead of the result
            logger.error(f"Could not pickle the result of the task: {e}")
            error = pickle.PicklingError(
                "Could not pickle the result of the task: "
                f"{type(e).__name__}: {e}"
            )
            error.__cause__ = e
            ds.set_error(error)
            replaced_result = True
            continue
        if not dumped:
            logger.warning(
                f"Could not dump DelayedSubmission to {out_fname}. "
                "Retrying in 1 second."
            )
            time.sleep(1)


def logger_level(arg):
    """Parse logging level argument.

    Parameters
    ----------
    arg : str
        The argument to parse.

    Returns
    -------
    int or str
        The parsed argument.

    Raises
    ------
    argparse.ArgumentTypeError
        If the argument cannot be parsed.

    """
    try:
        return int(arg)  # try convert to int
    except ValueError:
        pass
    if arg.upper() in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        return arg.upper()
    raise argparse.ArgumentTypeError(
        "x must be an int or one of 'DEBUG', 'INFO', 'WARNING', 'ERROR'"
    )


if __name__ == "__main__":
    import argparse
    import sys
    import warnings

    from joblib_htcondor.logging import _logging_types, configure_logging

    # Setup logger
    # Create log stream handler
    lh = logging.StreamHandler(sys.stdout)
    # Create log formatter
    formatter = logging.Formatter(
        "%(asctime)s %(name)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Set formatter to handler
    lh.setFormatter(formatter)
    # Add log handler
    logger.addHandler(lh)

    # Create CLI argument parser
    parser = argparse.ArgumentParser(
        description="Run a pickled DelayedSubmission object."
    )
    # Add file argument
    parser.add_argument(
        "filename",
        type=str,
        help="The name of the file to load the DelayedSubmission object from.",
    )
    # Add lock lifetime argument
    parser.add_argument(
        "--lock-lifetime",
        type=int,
        help="The number of seconds to wait for obtaining the lock on the "
        "file containing the DelayedSubmission object before giving up. ",
    )
    # Add delete file on load argument
    parser.add_argument(
        "--delete-file-on-load",
        action="store_true",
        help="Whether to delete the file containing the DelayedSubmission "
        "object after loading it. This can be useful to save disk space, "
        "but can cause issues if the scheduler cancels the job and re-queues "
        "it, as the file will be missing. Use with caution.",
    )

    # Add verbosity argument
    parser.add_argument(
        "--verbose",
        type=logger_level,
        default="INFO",
        help="The logging verbosity to use.",
    )
    # Parse arguments
    args = parser.parse_args()

    # Set logger level
    log_level = args.verbose
    if isinstance(log_level, str):
        log_level = _logging_types[log_level]
    logger.setLevel(log_level)
    logger.info(f"Setting logging level to {args.verbose}")
    configure_logging(level=log_level)

    logger.debug(f"Executor called with {args}")

    # Check and parse file argument
    fname = Path(args.filename)
    out_fname = fname.with_stem(f"{fname.stem}_out")

    # Create file for run
    run_fname = fname.with_suffix(".run")
    with run_fname.open("w") as f:
        f.write(datetime.now().isoformat())

    # Load file
    logger.info(f"Loading DelayedSubmission object from {fname}")
    try:
        ds = _load_task(fname, lock_lifetime=args.lock_lifetime)
    except Exception as e:
        # Retrying will not help, send the error back so the task fails
        logger.exception(f"Could not load the task from {fname}")
        ds = DelayedSubmission(None, lock_lifetime=args.lock_lifetime)  # type: ignore
        ds.set_error(e)
        _dump_result(ds, out_fname)
        sys.exit(1)

    if args.delete_file_on_load:
        try:
            logger.info("Deleting file containing DelayedSubmission object.")
            fname.unlink()
            logger.info(f"Deleted file {fname}.")
            # Best-effort cleanup of associated lock file (<task>.lock)
            lock_fname = fname.parent / (fname.name + ".lock")
            try:
                lock_fname.unlink(missing_ok=True)
                logger.info(f"Deleted lock file {lock_fname}.")
            except RuntimeError as e:
                logger.warning(
                    f"Could not delete lock file {lock_fname} after loading "
                    f"DelayedSubmission. Error: {e}"
                )
        except RuntimeError as e:
            logger.warning(
                f"Could not delete file {fname} after loading "
                f"DelayedSubmission. Error: {e}"
            )

    # Issue warning for re-running
    if ds.done():
        warnings.warn(
            "The DelayedSubmission object has already been run.",
            stacklevel=1,
        )
    # Run file
    logger.info("Running DelayedSubmission object")
    ds.run()
    logger.info("DelayedSubmission finished")
    _dump_result(ds, out_fname)
    logger.info("Done.")

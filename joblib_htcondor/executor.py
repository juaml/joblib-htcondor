"""Run pickled DelayedSubmission objects."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
#          Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

import time
from datetime import datetime


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
    import logging
    import sys
    import warnings
    from pathlib import Path

    from joblib_htcondor.delayed_submission import DelayedSubmission
    from joblib_htcondor.logging import _logging_types, configure_logging

    # Setup logger
    logger = logging.getLogger("joblib_htcondor.executor")
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
    if not fname.exists():
        raise FileNotFoundError(f"File {fname} not found.")

    # Create file for run
    run_fname = fname.with_suffix(".run")
    with run_fname.open("w") as f:
        f.write(datetime.now().isoformat())

    # Load file
    logger.info(f"Loading DelayedSubmission object from {fname}")
    ds = None
    while ds is None:
        ds = DelayedSubmission.load(fname)
        if ds is None:
            logger.warning(
                f"Could not load DelayedSubmission object from {fname}. "
                "Retrying in 1 second."
            )
            time.sleep(1)  # Wait 1 second before retrying

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
    old_stem = fname.stem
    out_fname = fname.with_stem(f"{old_stem}_out")
    logger.info(f"Dumping DelayedSubmission (result only) to {out_fname}")
    # Dump output
    dumped = False
    while not dumped:
        dumped = ds.dump(out_fname, result_only=True)
        if not dumped:
            logger.warning(
                f"Could not dump DelayedSubmission to {out_fname}. "
                "Retrying in 1 second."
            )
            time.sleep(1)
    logger.info("Done.")

"""Run pickled DelayedSubmission objects."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
#          Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

from datetime import datetime


if __name__ == "__main__":
    import argparse
    import logging
    import sys
    import warnings
    from pathlib import Path

    from joblib_htcondor.delayed_submission import DelayedSubmission

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
        type=int,
        default=logging.INFO,
        help="The logging verbosity to use.",
    )
    # Parse arguments
    args = parser.parse_args()

    # Set logger level
    logger.setLevel(args.verbose)

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
    ds = DelayedSubmission.load(fname)
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
    ds.dump(out_fname, result_only=True)
    logger.info("Done.")

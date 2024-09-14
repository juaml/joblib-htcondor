"""Run pickled DelayedSubmission objects."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
#          Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

import logging
from pathlib import Path


logger = logging.getLogger("joblib_htcondor.executor")
lh = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s %(name)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
lh.setFormatter(formatter)
logger.addHandler(lh)

if __name__ == "__main__":
    import argparse
    import warnings

    from joblib_htcondor.delayed_submission import DelayedSubmission

    parser = argparse.ArgumentParser(
        description="Run a pickled DelayedSubmission object."
    )
    parser.add_argument(
        "filename",
        type=str,
        help="The name of the file to load the DelayedSubmission object from.",
    )

    parser.add_argument(
        "--verbose",
        type=int,
        default=logging.INFO,
        help="The logging verbosity to use.",
    )
    args = parser.parse_args()
    logger.setLevel(args.verbose)
    logger.debug(f"Executor called with {args}")
    fname = Path(args.filename)
    if not fname.exists():
        raise FileNotFoundError(f"File {fname} not found.")
    logger.info(f"Loading DelayedSubmission object from {fname}")
    ds = DelayedSubmission.load(fname)
    if ds.done():
        warnings.warn(
            "The DelayedSubmission object has already been run.",
            stacklevel=1,
        )
    logger.info("Running DelayedSubmission object")
    ds.run()
    logger.info("DelayedSubmission finished")
    old_stem = fname.stem
    out_fname = fname.with_stem(f"{old_stem}_out")
    logger.info(f"Dumping DelayedSubmission object to {out_fname}")
    ds.dump(out_fname)
    logger.info("Done.")

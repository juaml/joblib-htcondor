"""Run pickled DelayedSubmission objects."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
#          Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

from pathlib import Path


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

    args = parser.parse_args()
    fname = Path(args.filename)
    if not fname.exists():
        raise FileNotFoundError(f"File {fname} not found.")
    ds = DelayedSubmission.load(fname)
    if ds.done():
        warnings.warn(
            "The DelayedSubmission object has already been run.",
            stacklevel=1,
        )
    ds.run()
    ds.dump(args.filename)


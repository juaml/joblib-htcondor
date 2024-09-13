"""Run pickled DelayedSubmission objects."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
#          Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

if __name__ == "__main__":
    import argparse
    import warnings

    from joblib_htcondor.delayed_submission import DelayedSubmission

    parser = argparse.ArgumentParser(
        description="Run a pickled DelayedSubmission object."
    )
    parser.add_argument(
        "filename",
        type=argparse.FileType("rb"),
        help="The name of the file to load the DelayedSubmission object from.",
    )

    args = parser.parse_args()
    ds = DelayedSubmission.load(args.filename)
    if ds.done():
        warnings.warn(
            "The DelayedSubmission object has already been run.",
            stacklevel=1,
        )
    ds.run()
    ds.dump(args.filename)


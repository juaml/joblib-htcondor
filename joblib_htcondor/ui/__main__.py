"""The joblib htcondor UI module entrypoint."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import argparse
import logging
from pathlib import Path

from .app import MonitorApp
from .uilogging import init_logging


def dir_or_file_path(string: str) -> Path:
    """Return the metadata directory or file given a path.

    Parameters
    ----------
    string : str
        A shared data directory, its metadata directory or a metadata file.

    Returns
    -------
    pathlib.Path
        The metadata directory or file.

    Raises
    ------
    argparse.ArgumentTypeError
        If the path is not a shared data directory or a metadata file.

    """
    t_path = Path(string)
    if t_path.is_file() or t_path.name == ".jht-meta":
        return t_path
    if (t_path / ".jht-meta").is_dir():
        return t_path / ".jht-meta"
    raise argparse.ArgumentTypeError(
        f"This is not a shared data dir: {string}"
    )


def main(argv: list[str] | None = None) -> None:
    """Run the HTCondor Joblib Monitor.

    Parameters
    ----------
    argv : list of str or None, optional
        The command line arguments (default None, uses sys.argv).

    """
    parser = argparse.ArgumentParser(description="HTCondor Joblib Monitor")
    parser.add_argument(
        "--path",
        type=dir_or_file_path,
        required=True,
        help="Shared data directory, or the metadata file of a run.",
    )
    parser.add_argument(
        "--refresh",
        type=float,
        default=5,
        help="The refresh interval in seconds.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        help="Write the log to this file.",
    )
    parser.add_argument(
        "--verbose",
        type=int,
        default=logging.INFO,
        help="The logging verbosity to use in the log file.",
    )
    args = parser.parse_args(argv)
    if args.log_file is not None:
        init_logging(args.verbose, args.log_file)
    MonitorApp(args.path, args.refresh).run()


if __name__ == "__main__":
    main()

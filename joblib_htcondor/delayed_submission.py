"""The joblib htcondor DelayedSubmission implementation."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
#          Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

from concurrent.futures.process import _ExceptionWithTraceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, Union

from flufl.lock import Lock, TimeOutError  # type: ignore
from joblib.externals.cloudpickle import cloudpickle  # type: ignore

from .logging import logger


__all__ = ["DelayedSubmission"]


def _get_lock(fname: Union[Path, str], *args: Any, **kwargs: Any) -> Lock:
    """Get a `flufl.lock.Lock` object.

    Parameters
    ----------
    fname : pathlib.Path or str
        The lockfile path.
    *args
        Positional arguments passed to `flufl.lock.Lock`.
    **kwargs
        Keyword arguments passed to `flufl.lock.Lock`.

    Returns
    -------
    flufl.lock.Lock
        Lock object.

    """
    if not isinstance(fname, Path):
        fname = Path(fname)
    lock_fname = fname.with_suffix(".lock")

    return Lock(lock_fname.as_posix(), *args, **kwargs)


class DelayedSubmission:
    """Delayed submission object to be run in the worker.

    Implements an object that wraps a function call and its arguments so they
    can be pickled and executed in the workers.

    Parameters
    ----------
    func : callable
        The function to call.
    *args
        Positional arguments to pass to the function.
    **kwargs
        Keyword arguments to pass to the function.

    """

    def __init__(
        self,
        func: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs
        # Initialize tracking variables
        self._result = None
        self._done = False
        self._error = False
        self._done_timestamp = None

    def run(self) -> None:
        """Run the function with the arguments and store the result."""
        try:
            self._result = self.func(*self.args, **self.kwargs)  # type: ignore
        except BaseException as e:  # noqa: BLE001
            self._result = _ExceptionWithTraceback(
                e,
                e.__traceback__,  # type: ignore
            )
            self._error = True
        self._done_timestamp = datetime.now()
        self._done = True

    def done(self) -> bool:
        """Return whether the function has been run.

        Returns
        -------
        bool
            Whether the function has been run.

        """
        return self._done

    def done_timestamp(self) -> Optional[datetime]:
        """Return the timestamp when the function has finished.

        Returns
        -------
        datetime
            The timestamp when the function has been run. If the function has
            not been run, returns None.

        """
        return self._done_timestamp

    def error(self) -> bool:
        """Return whether the function raised an exception.

        Returns
        -------
        bool
            Whether the function raised an exception.

        """
        return self._error

    def result(self) -> Any:
        """Return the result of the function.

        Returns
        -------
        Any
            The result of the function.

        """
        return self._result

    def dump(
        self, filename: Union[str, Path], result_only: bool = False
    ) -> bool:
        """Dump the object to a file.

        Parameters
        ----------
        filename : str or pathlib.Path
            The file to dump the object to.
        result_only : bool, optional
            Whether to dump only the result (default False).

        """
        if result_only:
            # Avoid pickling function and arguments
            tmp_func = self.func
            tmp_args = self.args
            tmp_kwargs = self.kwargs
            self.func = None
            self.args = None
            self.kwargs = None
        # Get lockfile
        flock = _get_lock(fname=filename, lifetime=120)  # Max 2 minutes
        # Dump in the lockfile
        try:
            with flock:
                with open(filename, "wb") as file:
                    cloudpickle.dump(self, file)
        except TimeOutError:
            logger.error(f"Could not obtain lock for {filename} in 2 minutes.")
            return False
        # Set to original values
        if result_only:
            self.func = tmp_func
            self.args = tmp_args
            self.kwargs = tmp_kwargs

        return True

    @classmethod
    def load(
        cls: type["DelayedSubmission"], filename: Union[str, Path]
    ) -> Optional["DelayedSubmission"]:
        """Load a DelayedSubmission object from a file.

        Parameters
        ----------
        filename : str or pathlib.Path
            The file to load the object from.

        Returns
        -------
        DelayedSubmission or None
            The loaded DelayedSubmission object. If a TimeOutError is raised
            while obtaining the lock, returns None.

        Raises
        ------
        TypeError
            If loaded object is not of type `cls`.

        """
        # Get lockfile
        flock = _get_lock(filename, lifetime=120)  # Max 2 minutes
        # Load from the lockfile
        try:
            with flock:
                with open(filename, "rb") as file:
                    obj = cloudpickle.load(file)
            if not (isinstance(obj, cls)):
                raise TypeError(
                    "Loaded object is not a DelayedSubmission object."
                )
        except TimeOutError:
            logger.error(f"Could not obtain lock for {filename} in 2 minutes.")
            return None
        return obj

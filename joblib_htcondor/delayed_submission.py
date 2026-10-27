"""The joblib htcondor DelayedSubmission implementation."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
#          Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import traceback
from concurrent.futures.process import _ExceptionWithTraceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, Union

from flufl.lock import Lock, NotLockedError, TimeOutError  # type: ignore
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


def _release_lock(flock: Lock, fname: Union[Path, str]) -> None:
    """Release a `flufl.lock.Lock` that might have been broken.

    If the lock lifetime expires while it is held, another actor can break it.
    This is not an error for the holder: the file was completely written (or
    read) before releasing the lock.

    Parameters
    ----------
    flock : flufl.lock.Lock
        Lock object to release.
    fname : pathlib.Path or str
        The file the lock protects.

    """
    try:
        flock.unlock()
    except NotLockedError:
        logger.warning(
            f"The lock for {fname} expired and was broken by another process "
            "before being released. Consider increasing `lock_lifetime`."
        )


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
        lock_lifetime: int = 120,
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
        self.context_func = None
        self._lock_lifetime = lock_lifetime

    def run(self) -> None:
        """Run the function with the arguments and store the result."""
        if self.context_func is not None:
            self.context_func()
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

    def set_context_func(self, context_func: Callable) -> None:
        """Set a context function to be called prior to the main function.

        Parameters
        ----------
        context_func : callable
            The context function to call before running the main function.

        """
        self.context_func = context_func

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

    def set_error(self, error: BaseException) -> None:
        """Replace the result with an exception.

        Parameters
        ----------
        error : BaseException
            The exception to set as the result.

        """
        self._result = _ExceptionWithTraceback(
            error,
            error.__traceback__,  # type: ignore
        )
        self._error = True

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

        Returns
        -------
        bool
            Whether the object was dumped. If the lock could not be obtained,
            returns False.

        Raises
        ------
        Exception
            If the object cannot be pickled (e.g. ``pickle.PicklingError`` or
            ``TypeError``). No file is left behind in this case.

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
        flock = _get_lock(fname=filename, lifetime=self._lock_lifetime)
        try:
            try:
                flock.lock()
            except TimeOutError:
                logger.error(
                    f"Could not obtain lock for {filename} in "
                    f"{self._lock_lifetime} seconds."
                )
                logger.error(traceback.format_exc())
                return False
            # Dump in the lockfile
            try:
                with open(filename, "wb") as file:
                    cloudpickle.dump(self, file)
            except BaseException:
                # Pickling errors are not transient, do not leave a partially
                # written file that a reader would try to load
                Path(filename).unlink(missing_ok=True)
                raise
            finally:
                _release_lock(flock, filename)
        finally:
            # Set to original values
            if result_only:
                self.func = tmp_func
                self.args = tmp_args
                self.kwargs = tmp_kwargs

        return True

    @classmethod
    def load(
        cls: type["DelayedSubmission"],
        filename: Union[str, Path],
        lock_lifetime: int,
    ) -> Optional["DelayedSubmission"]:
        """Load a DelayedSubmission object from a file.

        Parameters
        ----------
        filename : str or pathlib.Path
            The file to load the object from.
        lock_lifetime : int
            The number of seconds to wait for obtaining the lock on the file
            before returning None.

        Returns
        -------
        DelayedSubmission or None
            The loaded DelayedSubmission object. If the lock could not be
            obtained or the file is incomplete (e.g. the writer's lock expired
            and was broken while dumping), returns None.

        Raises
        ------
        TypeError
            If loaded object is not of type `cls`.

        """
        # Get lockfile
        flock = _get_lock(filename, lifetime=lock_lifetime)
        try:
            flock.lock()
        except TimeOutError:
            logger.error(
                f"Could not obtain lock for {filename} in "
                f"{lock_lifetime} seconds."
            )
            logger.error(traceback.format_exc())
            return None
        # Load from the lockfile
        try:
            with open(filename, "rb") as file:
                obj = cloudpickle.load(file)
        except (cloudpickle.pickle.UnpicklingError, EOFError) as e:
            logger.error(
                f"Could not unpickle DelayedSubmission object from {filename}:"
                f" {e}"
            )
            logger.error(traceback.format_exc())
            return None
        finally:
            _release_lock(flock, filename)
        if not (isinstance(obj, cls)):
            raise TypeError("Loaded object is not a DelayedSubmission object.")
        return obj

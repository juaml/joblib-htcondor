"""The joblib htcondor DelayedSubmission implementation."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
#          Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import hashlib
import traceback
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
        lock_lifetime: int = 120,
        **kwargs: Any,
    ) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.context_func = None
        # Initialize runtime parameters
        self._lock_lifetime = lock_lifetime
        # Initialize tracking variables
        self._result = None
        self._done = False
        self._error = False
        self._done_timestamp = None
        self._cache_dir = None
        self._file_hash = None

    def set_cache(
        self, cache_dir: Union[str, Path], obj_hash: Optional[str] = None
    ) -> None:
        """Set the cache directory.

        Parameters
        ----------
        cache_dir : str or pathlib.Path
            The cache directory to use.
        obj_hash : str or None, optional
            The hash of the cloudpickled object's file. None means that the
            hash was not computed yet as the object was not saved to a file.

        """
        self._cache_dir = Path(cache_dir)
        self._file_hash = obj_hash

    def get_cache_filename(self) -> Optional[Path]:
        """Get the cache filename.

        Returns
        -------
        pathlib.Path
            The cache filename.

        """
        return f"{self._file_hash}.pkl"

    def run(self) -> None:
        """Run the function with the arguments and store the result."""
        # if the result is in the cache, just read it and skip running the
        # function
        if self._cache_dir is not None:
            if self._file_hash is None:
                raise ValueError(
                    "Cache directory is set but file hash is not set. This"
                    "should  not happen, as the file hash should be computed "
                    "when loading the object from a file. Please report "
                    "this issue."
                )
            cache_file = self._cache_dir / self.get_cache_filename()
            if cache_file.exists():
                logger.info(
                    f"Cache hit for {self._file_hash}. "
                    "Reading result from cache."
                )
                with cache_file.open("rb") as f:
                    dump_obj = cloudpickle.load(f)
                    self._result = dump_obj._result
                    self._done = dump_obj._done
                    self._error = dump_obj._error
                self._done_timestamp = datetime.now()
                return
            else:
                logger.info(
                    f"Cache miss for {self._file_hash}. "
                    "Running function."
                )
        # No cache or cache miss, we need to compute the result
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
        # store current cache parameters and set to None to avoid pickling them
        tmp_cache_dir = self._cache_dir
        tmp_cache_hash = self._file_hash
        self._cache_dir = None
        self._file_hash = None

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
        # Dump in the lockfile
        out = True
        try:
            with flock:
                with open(filename, "wb") as file:
                    cloudpickle.dump(self, file)
        except TimeOutError:
            logger.error(
                f"Could not obtain lock for {filename} in "
                f"{self._lock_lifetime} seconds."
            )
            logger.error(traceback.format_exc())
            out = False
        except cloudpickle.pickle.PicklingError as e:
            logger.error(
                f"Could not pickle DelayedSubmission object to {filename}: {e}"
            )
            logger.error(traceback.format_exc())
            out = False
        # Set to original values
        if result_only:
            self.func = tmp_func
            self.args = tmp_args
            self.kwargs = tmp_kwargs

        # Restore cache parameters
        self._cache_dir = tmp_cache_dir
        self._file_hash = tmp_cache_hash
        return out

    @classmethod
    def load(
        cls: type["DelayedSubmission"],
        filename: Union[str, Path],
        lock_lifetime: int,
        cache_dir: Optional[Union[str, Path]] = None,
    ) -> Optional["DelayedSubmission"]:
        """Load a DelayedSubmission object from a file.

        Parameters
        ----------
        filename : str or pathlib.Path
            The file to load the object from.
        lock_lifetime : int
            The number of seconds to wait for obtaining the lock on the file
            before returning None.
        cache_dir : str or pathlib.Path or None, optional
            Cache directory to use. If set, this will be passed to the
            DelayedSubmission object and the hash will be computed from the
            pickled object and used as filename in the cache directory.

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
        flock = _get_lock(filename, lifetime=lock_lifetime)
        # Load from the lockfile
        try:
            with flock:
                with open(filename, "rb") as file:
                    obj = cloudpickle.load(file)
                    if cache_dir is not None:
                        logger.info(
                            "Computing hash for cache with cache_dir "
                            f"{cache_dir}"
                        )
                        file.seek(0)
                        obj_hash = hashlib.file_digest(
                            file, "sha256"
                        ).hexdigest()
                        logger.info(f"Computed hash {obj_hash} for cache")
                        obj.set_cache(cache_dir, obj_hash=obj_hash)

            if not (isinstance(obj, cls)):
                raise TypeError(
                    "Loaded object is not a DelayedSubmission object."
                )
        except TimeOutError:
            logger.error(
                f"Could not obtain lock for {filename} in "
                f"{lock_lifetime} seconds."
            )
            logger.error(traceback.format_exc())
            return None
        except cloudpickle.pickle.UnpicklingError as e:
            logger.error(
                f"Could not unpickle DelayedSubmission object from {filename}:"
                f" {e}"
            )
            logger.error(traceback.format_exc())
            return None
        return obj

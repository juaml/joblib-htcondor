"""The joblib htcondor DelayedSubmission implementation."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
#          Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

from concurrent.futures.process import _ExceptionWithTraceback
from pathlib import Path
from typing import Any, Callable, Type, Union

from flufl.lock import Lock
from joblib.externals.cloudpickle import cloudpickle  # type: ignore


def get_lock(fname, *args, **kwargs):
    """Get a lock object."""
    if isinstance(fname, Path):
        lock_fname = Path(fname).with_suffix(".lock")
    else:
        lock_fname = Path(fname + ".lock")
    return Lock(lock_fname.as_posix(), *args, **kwargs)


class DelayedSubmission:
    """Delayed submission object to be run in the worker.

    Implements an object that wraps a function call and its arguments so they
    can be pickled and executed in the workers.

    Parameters
    ----------
    func : Callable
        The function to call
    args : Any
        The arguments to pass to the function
    kwargs : Dict[str, Any]
        The keyword arguments to pass to the function

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

        self._result = None
        self._done = False
        self._error = False

    def run(self) -> None:
        """Run the function with the arguments and store the result."""
        try:
            self._result = self.func(*self.args, **self.kwargs)
        except BaseException as e:  # noqa: BLE001
            self._result = _ExceptionWithTraceback(
                e,
                e.__traceback__,  # type: ignore
            )
            self._error = True
        self._done = True

    def done(self) -> bool:
        """Return whether the function has been run.

        Returns
        -------
        bool
            Whether the function has been run.

        """
        return self._done

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

    def dump(self, filename: Union[str, Path], result_only=False) -> None:
        """Dump the object to a file.

        Parameters
        ----------
        filename : str
            The name of the file to dump the object to.
        result_only : bool, optional
            Whether to dump only the result, by default False
        """
        if result_only:
            # Avoid pickling function and arguments
            tmp_func = self.func
            tmp_args = self.args
            tmp_kwargs = self.kwargs
            self.func = None
            self.args = None
            self.kwargs = None
        flock = get_lock(filename, lifetime=120)  # Max 2 minutes
        with flock:
            with open(filename, "wb") as file:
                cloudpickle.dump(self, file)
        if result_only:
            self.func = tmp_func
            self.args = tmp_args
            self.kwargs = tmp_kwargs

    @classmethod
    def load(
        cls: Type["DelayedSubmission"], filename: Union[str, Path]
    ) -> "DelayedSubmission":
        """Load a DelayedSubmission object from a file.

        Parameters
        ----------
        filename : str
            The name of the file to load the object from.

        Returns
        -------
        DelayedSubmission
            The loaded DelayedSubmission object.

        """
        flock = get_lock(filename, lifetime=120)  # Max 2 minutes
        with flock:
            with open(filename, "rb") as file:
                obj = cloudpickle.load(file)
        if not (isinstance(obj, cls)):
            raise ValueError(
                "Loaded object is not a DelayedSubmission object."
            )
        return obj

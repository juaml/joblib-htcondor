"""Provide tests for DelayedSubmission."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
#          Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

import tempfile
from concurrent.futures.process import _ExceptionWithTraceback
from pathlib import Path

import pytest

from joblib_htcondor.delayed_submission import DelayedSubmission


def test_delayed_submission_noargs() -> None:
    """Test DelayedSubmission with no arguments."""

    def myfunc():
        return 100

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        fname = tmpdir / "test.pickle"
        ds = DelayedSubmission(myfunc)
        assert not ds.done()
        assert ds.args == ()
        assert ds.kwargs == {}
        ds.dump(fname)
        ds.run()
        assert ds.done()
        assert ds.result() == 100
        assert ds.done_timestamp() is not None
        del ds

        ds2 = DelayedSubmission.load(fname)
        assert ds2.args == ()
        assert ds2.kwargs == {}
        assert not ds2.done()
        ds2.run()
        assert ds2.done()
        assert ds2.done_timestamp() is not None
        assert ds2.result() == 100
        fname2 = tmpdir / "test2.pickle"
        ds2.dump(fname2)
        del ds2

        ds3 = DelayedSubmission.load(fname2)
        assert ds3.args == ()
        assert ds3.kwargs == {}
        assert ds3.done()
        assert ds3.result() == 100


def test_delayed_submission_args() -> None:
    """Test DelayedSubmission with arguments."""

    def myfunc(a, b):
        return a + b

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        fname = tmpdir / "test.pickle"
        ds = DelayedSubmission(myfunc, 10, 20)
        assert not ds.done()
        assert ds.args == (10, 20)
        assert ds.kwargs == {}
        ds.dump(fname)
        ds.run()
        assert ds.done()
        assert ds.done_timestamp() is not None
        assert ds.result() == 30
        del ds

        ds2 = DelayedSubmission.load(fname)
        assert ds2.args == (10, 20)
        assert ds2.kwargs == {}
        assert not ds2.done()
        ds2.run()
        assert ds2.done()
        assert ds2.done_timestamp() is not None
        assert ds2.result() == 30
        fname2 = tmpdir / "test2.pickle"
        ds2.dump(fname2)
        del ds2

        ds3 = DelayedSubmission.load(fname2)
        assert ds3.args == (10, 20)
        assert ds3.kwargs == {}
        assert ds3.done()
        assert ds3.done_timestamp() is not None
        assert ds3.result() == 30


def test_delayed_submission_kwargs() -> None:
    """Test DelayedSubmission with kwarguments."""

    def myfunc(a, b):
        return a + b

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        fname = tmpdir / "test.pickle"
        ds = DelayedSubmission(myfunc, a=10, b=20)
        assert not ds.done()
        assert ds.args == ()
        assert ds.kwargs == {"a": 10, "b": 20}
        ds.dump(fname)
        ds.run()
        assert ds.done()
        assert ds.done_timestamp() is not None
        assert ds.result() == 30
        del ds

        ds2 = DelayedSubmission.load(fname)
        assert ds2.args == ()
        assert ds2.kwargs == {"a": 10, "b": 20}
        assert not ds2.done()
        assert ds2.done_timestamp() is None
        ds2.run()
        assert ds2.done()
        assert ds2.done_timestamp() is not None
        assert ds2.result() == 30
        fname2 = tmpdir / "test2.pickle"
        ds2.dump(fname2)
        del ds2

        ds3 = DelayedSubmission.load(fname2)
        assert ds3.args == ()
        assert ds3.kwargs == {"a": 10, "b": 20}
        assert ds3.done()
        assert ds3.done_timestamp() is not None
        assert ds3.result() == 30


def test_delayed_submission_allwargs() -> None:
    """Test DelayedSubmission with args and kwargs."""

    def myfunc(a, b):
        return a + b

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        fname = tmpdir / "test.pickle"
        ds = DelayedSubmission(myfunc, 10, b=20)
        assert not ds.done()
        assert ds.done_timestamp() is None
        assert ds.args == (10,)
        assert ds.kwargs == {"b": 20}
        ds.dump(fname)
        ds.run()
        assert ds.done()
        assert ds.done_timestamp() is not None
        assert ds.result() == 30
        del ds

        ds2 = DelayedSubmission.load(fname)
        assert ds2.args == (10,)
        assert ds2.kwargs == {"b": 20}
        assert not ds2.done()
        ds2.run()
        assert ds2.done()
        assert ds2.done_timestamp() is not None
        assert ds2.result() == 30
        fname2 = tmpdir / "test2.pickle"
        ds2.dump(fname2)
        del ds2

        ds3 = DelayedSubmission.load(fname2)
        assert ds3.args == (10,)
        assert ds3.kwargs == {"b": 20}
        assert ds3.done()
        assert ds3.done_timestamp() is not None
        assert ds3.result() == 30


def test_delayed_submission_error() -> None:
    """Test DelayedSubmission with no arguments."""

    def myfunc():
        raise ValueError("Test error")

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        fname = tmpdir / "test.pickle"
        ds = DelayedSubmission(myfunc)
        assert not ds.done()
        assert ds.args == ()
        assert ds.kwargs == {}
        ds.run()
        ds.dump(fname)
        assert ds.done()
        assert ds.done_timestamp() is not None
        assert ds.error()
        out = ds.result()

        # Before pickling this should be an _ExceptionWithTraceback
        assert isinstance(out, _ExceptionWithTraceback)
        rebuild, args = out.__reduce__()
        exception = rebuild(*args)  # type: ignore
        assert exception.args == ("Test error",)
        with pytest.raises(ValueError, match="Test error"):
            raise exception
        del ds
        del out
        del exception
        del rebuild
        del args

        # After pickling it should be an exception
        ds2 = DelayedSubmission.load(fname)
        assert ds2.done()
        assert ds2.done_timestamp() is not None
        assert ds2.error()
        out2 = ds2.result()
        assert isinstance(out2, ValueError)
        with pytest.raises(ValueError, match="Test error"):
            raise out2


def test_delayed_submission_results_only() -> None:
    """Test DelayedSubmission with args and kwargs."""

    def myfunc(a, b):
        return a + b

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        fname = tmpdir / "test.pickle"
        ds = DelayedSubmission(myfunc, 10, b=20)
        assert not ds.done()
        assert ds.done_timestamp() is None
        assert ds.args == (10,)
        assert ds.kwargs == {"b": 20}
        ds.run()
        ds.dump(fname, result_only=True)
        assert ds.done()
        assert ds.done_timestamp() is not None
        assert ds.result() == 30
        assert ds.func is not None
        assert ds.args == (10,)
        assert ds.kwargs == {"b": 20}
        del ds

        ds2 = DelayedSubmission.load(fname)
        assert ds2.done()
        assert ds2.done_timestamp() is not None
        assert ds2.result() == 30
        assert ds2.func is None
        assert ds2.args is None
        assert ds2.kwargs is None

# How To Contribute

## Setting up the local development environment

#. Fork the https://github.com/juaml/joblib-htcondor repository on GitHub. If you
   have never done this before,
   [follow the official guide](https://guides.github.com/activities/forking/).

#. Clone your fork locally as described in the same guide.

#. Install your local copy into a Python virtual environment. You can
   [read this guide to learn more](https://realpython.com/python-virtual-environments-a-primer/)
   about them and how to create one.

   ```console
   $ pip install -e ".[dev]"
   ```

#. Create a branch for local development using the `main` branch as a
   starting point. Use `fix`, `refactor`, `chore`, or `feat` as a prefix.

   ```console
   $ git checkout main
   $ git checkout -b <prefix>/<name-of-your-branch>
   ```

   Now you can make your changes locally.

#. Make sure you install git pre-commit hooks like so:

   ```console
   $ pre-commit install
   ```

#. When making changes locally, it is helpful to `git commit` your work
   regularly. On one hand to save your work and on the other hand, the smaller
   the steps, the easier it is to review your work later. Please use
   [semantic commit messages](http://karma-runner.github.io/2.0/dev/git-commit-msg.html).

   ```console
   $ git add .
   $ git commit -m "<prefix>: <summary of changes>"
   ```

   In case, you want to commit some WIP (work-in-progress) code, please indicate
   that in the commit message and use the flag `--no-verify` with
   `git commit` like so:

   ```console
   $ git commit --no-verify -m "WIP: <summary of changes>"
   ```

#. When you're done making changes, check that your changes pass our linting.
   This is all included with `tox`.

   ```console
   $ tox -e ruff
   ```

#. Push your branch to GitHub.

   ```console
   $ git push origin <prefix>/<name-of-your-branch>
   ```

#. Open the link displayed in the message when pushing your new branch in order
   to submit a pull request. Please follow the template presented to you in the
   web interface to complete your pull request.


## GitHub Pull Request guidelines

Before you submit a pull request, check that it meets these guidelines:

#. If the pull request adds functionality, the `README.md` should be
   updated accordingly.

#. Make sure to create a Draft Pull Request. If you are not sure how to do it,
   check
   [here](https://github.blog/2019-02-14-introducing-draft-pull-requests/).

#. Note the pull request ID assigned after completing the previous step and
   create a *newsfragment* for letting
   [towncrier](https://towncrier.readthedocs.io/en/stable/index.html) know to update
   the `CHANGELOG.md` on release. Check
   [here](https://towncrier.readthedocs.io/en/stable/markdown.html) on how to go
   about it.

#. Someone from the core team will review your work and guide you to a successful
   contribution.

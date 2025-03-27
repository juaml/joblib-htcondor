# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This project uses [*towncrier*](https://towncrier.readthedocs.io/) and the changes for the upcoming release can be found in <https://github.com/twisted/my-project/tree/main/changelog.d/>.

<!-- towncrier release notes start -->

## [0.2.0](https://github.com/juaml/joblib-htcondor/tree/0.2.0) - 2025-03-27

### Fixed

- Fix a bug in which the total disk size diplayed in the UI was not correct ([#8](https://github.com/juaml/joblib-htcondor/issues/8))
- Fix a bug in which the watcher thread might crash due to an empty `.run` file ([#9](https://github.com/juaml/joblib-htcondor/issues/9))
- Fix a bug in which the scheduler submit call might fail and job will be lost ([#10](https://github.com/juaml/joblib-htcondor/issues/10))
- Fix a bug in which exceptions raised during delayed computation broke the backend without reporting the error and finishing. ([#11](https://github.com/juaml/joblib-htcondor/issues/11))
- Relax `htcondor` version to allow package installation ([#12](https://github.com/juaml/joblib-htcondor/issues/12))


## [0.1.1](https://github.com/juaml/joblib-htcondor/tree/0.1.1) - 2024-10-25

### Fixed

- Retry load/dump of the DelayedSubmission object in case a TimeOutError from the flufl.lock is raised ([#6](https://github.com/juaml/joblib-htcondor/issues/6))


## [0.1.0](https://github.com/juaml/joblib-htcondor/tree/0.1.0) - 2024-10-22

### Added

- Introduce initial public version of the package

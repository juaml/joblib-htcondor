"""Test scikit-learn cross-platform compatibility."""

import logging
from pprint import pprint  # To print in a pretty way

import numpy as np
import pandas as pd
from joblib import parallel_config
from julearn import run_cross_validation  # type: ignore
from julearn.config import set_config
from julearn.pipeline import PipelineCreator  # type: ignore
from seaborn import load_dataset
from sklearn.datasets import make_classification

from joblib_htcondor import register_htcondor
from joblib_htcondor.logging import configure_logging


register_htcondor()

configure_logging(level=logging.INFO)

set_config("disable_x_verbose", True)
set_config("disable_xtypes_verbose", True)
set_config("disable_xtypes_check", True)
set_config("disable_x_check", True)


# df = load_dataset("iris")
# X = df.columns[:-1].tolist()
# y = "species"
# X_types = {"continuous": X}

X, y = make_classification(
    n_samples=1000,
    n_features=1000,
    n_informative=2,
    n_classes=2,
    random_state=42,
)

X_names = [f"feat_{x}" for x in range(X.shape[1])]
X_types = {"continuous": X_names}

df = pd.DataFrame(np.c_[X, y], columns=X_names + ["target"])

y = "target"
X = ["feat_.*"]
X_types = {"continuous": "feat_.*"}

N_JOBS = -1

creator = PipelineCreator(problem_type="classification")
creator.add("zscore")
creator.add(
    "svm",
    C=[0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000, 1000000],
    kernel="rbf",
    gamma=[
        1e-7,
        1e-6,
        1e-5,
        1e-4,
        1e-3,
        1e-2,
        1e-1,
        1,
        10,
        100,
        1000,
    ],
    probability=True,
)


with parallel_config(
    backend="htcondor",
    pool="head2.htc.inm7.de",
    n_jobs=N_JOBS,
    request_cpus=1,
    request_memory="8GB",
    request_disk="1GB",
    verbose=1000,
    python_path="/home/fraimondo/miniconda3/ppc64le_dev/bin/python",
    extra_directives={"Requirements": 'Arch == "ppc64le"'},
    worker_log_level=logging.DEBUG,
    throttle=[100, 20],
    poll_interval=1,
):
    scores_tuned = run_cross_validation(
        X=X_names,
        y=y,
        data=df,
        X_types=X_types,
        model=creator,
        return_estimator="cv",
        cv=5,
        search_params={"kind": "grid", "pre_dispatch": "all", "cv": 2},
    )

    print(
        f"Scores with best hyperparameter: {scores_tuned['test_score'].mean()}"
    )
    # pprint(model_tuned.best_params_)

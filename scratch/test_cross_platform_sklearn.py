"""Test scikit-learn cross-platform compatibility."""
import logging
from pprint import pprint  # To print in a pretty way

from joblib import parallel_config
from julearn import run_cross_validation  # type: ignore
from julearn.pipeline import PipelineCreator  # type: ignore
from seaborn import load_dataset

from joblib_htcondor import register_htcondor


register_htcondor()
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(name)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("joblib_htcondor.backend").setLevel(logging.INFO)


df = load_dataset("iris")
X = df.columns[:-1].tolist()
y = "species"
X_types = {"continuous": X}

# The dataset has three kind of species. We will keep two to perform a binary
# classification.
df = df[df["species"].isin(["versicolor", "virginica"])]

creator = PipelineCreator(problem_type="classification")
creator.add("zscore")
creator.add("svm", C=[0.01, 0.1, 1, 10], gamma=[0.01, 0.1, 1, 10])


with parallel_config(
    backend="htcondor",
    pool="head2.htc.inm7.de",
    n_jobs=-1,
    request_cpus=1,
    request_memory="8GB",
    request_disk="1GB",
    verbose=1000,
    python_path="/home/fraimondo/miniconda3/ppc64le_dev/bin/python",
    extra_directives={"Requirements": 'Arch == "ppc64le"'},
    worker_log_level=logging.DEBUG,
    throttle=11,
    poll_interval=0.1
):
    scores_tuned, model_tuned = run_cross_validation(
        X=X,
        y=y,
        data=df,
        X_types=X_types,
        model=creator,
        return_estimator="all",
        search_params={"kind": "grid", "pre_dispatch": "all"},
    )

    print(
        f"Scores with best hyperparameter: {scores_tuned['test_score'].mean()}"
    )
    pprint(model_tuned.best_params_)

import logging
from operator import neg

from joblib import Parallel, delayed, parallel_config

from joblib_htcondor import register_htcondor


register_htcondor()
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(name)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("joblib_htcondor.backend").setLevel(logging.INFO)

with parallel_config(
    backend="htcondor",
    pool="head2.htc.inm7.de",
    n_jobs=5000,
    request_cpus=1,
    request_memory="8GB",
    request_disk="1GB",
    verbose=1000,
    python_path="/home/fraimondo/miniconda3/ppc64le_dev/bin/python",
    extra_directives={"Requirements": 'Arch == "ppc64le"'},
):
    result = Parallel()(delayed(neg)(i + 1) for i in range(5000))
    print(result)

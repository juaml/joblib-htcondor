from operator import neg

from joblib import Parallel, delayed, parallel_config
from joblib_htcondor import register_htcondor

import logging

register_htcondor()
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(name)s %(levelname)-8s %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
)
logging.getLogger("joblib_htcondor.backend").setLevel(logging.DEBUG)

with parallel_config(
    backend="htcondor",
    pool="head2.htc.inm7.de",
    n_jobs=5,
    verbose=1000,
):
    result = Parallel()(delayed(neg)(i + 1) for i in range(20))
    print(result)

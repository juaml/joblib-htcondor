import logging


logger = logging.getLogger("joblib_htcondor.ui")

def init_logging(level):
    logger.setLevel(level)
    fh = logging.FileHandler("ui.log")
    fh.setLevel(level)
    logger.addHandler(fh)

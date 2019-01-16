import os
import logging
import yaml

def setup_logging(settings):
    """
    Setup logging configuration
    """

    try:
        # This is a fix for logging in multiple processes
        # Source: https://github.com/jruere/multiprocessing-logging.git
        from cacheer import multiprocessing_logging
        multiprocessing_logging.install_mp_handler()
        logging.config.dictConfig(settings)
    except:
        logging.basicConfig(level=logging.INFO)


def load_config(path=None):
    if path is None:
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.yaml')
    with open(path, 'r') as f:
        conf = yaml.load(f)
    return conf


conf = load_config(path=os.getenv('CACHEER_CONFIG'))

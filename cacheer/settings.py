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


def load_config(path):
    if path is None:
        return {}
    
    if not os.path.exists(path):
        return {}

    with open(path, 'r') as f:
        conf = yaml.load(f)
    return conf


conf = load_config(path=os.getenv('CACHEER_CONFIG'))

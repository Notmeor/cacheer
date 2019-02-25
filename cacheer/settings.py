import os
import logging
import yaml


def load_config(path):
    if path is None:
        return {}
    
    if not os.path.exists(path):
        return {}

    with open(path, 'r') as f:
        conf = yaml.load(f)
    return conf


conf = load_config(path=os.getenv('CACHEER_CONFIG'))

# -*- coding: utf-8 -*-

import os
import logging.config

import yaml

import time
import functools

import hashlib

from pandas.io.pickle import pkl


def serialize(obj):
    return pkl.dumps(obj, pkl.HIGHEST_PROTOCOL)


def deserialize(b):
    return pkl.loads(b)


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0_ = time.time()
        ret = func(*args, **kwargs)
        print('%s in %.6f secs' % (
            func.__name__, time.time() - t0_))
        return ret
    return wrapper


def logit(log, before=None, after=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            if before:
                log(before)
            ret = func(*args, **kw)
            if after:
                log(after)
            return ret
        return wrapper
    return decorator


def gen_md5(b):
    return hashlib.md5(serialize(b)).hexdigest()


def setup_logging(default_path=None,
                  default_level=logging.INFO,
                  env_key='LOG_CFG'):
    """
    Setup logging configuration
    """
    if default_path:
        path = default_path
    else:
        path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())['logging']
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def load_config(path=None):
    if path is None:
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.yaml')
    with open(path, 'r') as f:
        conf = yaml.load(f)
    return conf


conf = load_config()

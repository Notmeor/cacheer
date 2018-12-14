# -*- coding: utf-8 -*-

import os
import logging.config

import yaml

import time
import functools

import hashlib
import __main__

import pandas as pd
from pandas.io.pickle import pkl
from pandas.io import packers


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        t0_ = time.time()
        ret = func(*args, **kwargs)
        print('%s in %.6f secs' % (
            func.__name__, time.time() - t0_))
        return ret
    return wrapper


@timeit
def serialize(obj):
    if isinstance(obj, bytes):
        return obj
    return pkl.dumps(obj, pkl.HIGHEST_PROTOCOL)


@timeit
def deserialize(b):
    return pkl.loads(b)


@timeit
def serialize_exp(obj):
    if isinstance(obj, pd.DataFrame):
        if obj.memory_usage(deep=True).sum() > 1000000:
            print('msgpack')
            return packers.to_msgpack(None, obj)
    return serialize(obj)


@timeit
def deserialize_exp(b):
    try:
        return deserialize(b)
    except:
        return packers.read_msgpack(b)


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


@timeit
def gen_md5(b, value=False):
    bytes_ = b if isinstance(b, bytes) else serialize(b)
    md5 = hashlib.md5(bytes_).hexdigest()
    if value:
        return md5, bytes_
    return md5


def is_defined_in_shell(func):
    if func.__module__ != '__main__':
        return False
    return not hasattr(__main__, '__file__')


def get_api_name(func):
    """
    Deprecated
    """
    api_name = func.__module__ + '.' + func.__qualname__
    if func.__module__ == '__main__':
        if not hasattr(__main__, '__file__'):
            print('cwd:', os.getcwd())
            return api_name
        api_name = __main__.__file__ + ':' + func.__qualname__
    return api_name


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
        config
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


conf = load_config(path=os.getenv('CACHEER_CONFIG'))
setup_logging(default_path=os.getenv('CACHEER_CONFIG'))


# This is a fix for logging in multiple processes
# Source: https://github.com/jruere/multiprocessing-logging.git
from cacheer import multiprocessing_logging
multiprocessing_logging.install_mp_handler()

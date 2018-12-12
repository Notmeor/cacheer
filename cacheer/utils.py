# -*- coding: utf-8 -*-

import os
import logging.config

import yaml

import time
import functools

import hashlib

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
def gen_md5(b):
    bytes_ = b if isinstance(b, bytes) else serialize(b)
    return hashlib.md5(bytes_).hexdigest()


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


conf = load_config()

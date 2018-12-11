# -*- coding: utf-8 -*-

import inspect
import functools
import collections

from pandas.io.pickle import pkl
import hashlib

import pymongo

import logging

from cacheer.store import LmdbStore as Store, MongoMetaDB as MetaDB
from cacheer.utils import conf, gen_md5, setup_logging, logit


setup_logging()
LOG = logging.getLogger(__file__)


def gen_cache_key(func, *args, **kw):

    # func has yet to get its __self__ attr
    def _is_bound_method(fn):
        qualname = fn.__qualname__
        if '.' not in qualname:
            return False
        else:
            cls_name = qualname.split('.')[0]

        if not args:
            return False

        # check whether staticmethod
        # by qualname and first argument
        is_staticmethod = False

        if isinstance(args[0], type):
            if args[0].__name__ != cls_name:
                is_staticmethod = True
        elif type(args[0]).__name__ != cls_name:
            is_staticmethod = True

        if is_staticmethod:
            return False

        return True

    signature = inspect.signature(func)

    bound_arg = signature.bind(*args, **kw)
    bound_arg.apply_defaults()
    arg = bound_arg.arguments

    if _is_bound_method(func):  # pop first argument
        print('Bound')
        arg = collections.OrderedDict(list(arg.items())[1:])

    arg.update({'__api_meta': func._api_meta})

    print('arg:', arg)
    key = hashlib.md5(pkl.dumps(arg, pkl.HIGHEST_PROTOCOL)).hexdigest()
    print('hashed: {}'.format(key))
    return key


class Cache:
    def __init__(self, header=None, body=None):
        self.header = header
        self.body = body


class CacheProvider:

    def __init__(self, manager=None):
        self._cache_manager = manager or self

    def get(self, key):
        if key not in self._cache_manager.get_all_keys():
            self._cache_manager.add(key)
        return self._cache_manager.load(key)


class CacheStore:

    def __init__(self):
        self._store = Store()

    def read(self, key):
        return self._store.read(key)

    def write(self, key, value):
        self._store.write(key, value)


class CacheManager:

    def __init__(self, cache_store, metadb):
        self._cache_store = cache_store
        self._metadb = metadb

        self._token_prefix = '__token_'

    def register_api(self, api_name, block_id):
        self._metadb.add_api(api_name, block_id)

    def write_cache(self, key, cache):
        try:
            key_ = self._token_prefix + key
            self._cache_store.write(key_, cache.header)
            self._cache_store.write(key, cache.body)
        except:
            self._cache_store.delete(key_)
            raise

    def read_cache_token(self, key):
        key_ = self._token_prefix + key
        return self._cache_store.read(key_)

    def read_cache_value(self, key):
        return self._cache_store.read(key)

    def update_cache_token(self, key, token):
        key_ = self._token_prefix + key
        self._cache_store.write(key_, token)

    def delete_cache(self, key):

        key_ = self._token_prefix + key
        self._cache_store.delete(key_)
        self._cache_store.delete(key)

    def _parse_key_as_params(self, key):
        """
        cache naming
            {class_name}_{class_signature}_{method_name}_{arguments}
        """

    def get_latest_token(self, block_id):
        return self._metadb.get_latest_token(block_id)

    def get_block_id(self, api_name):
        return self._metadb.get_block_id(api_name)

    def compare_equal(self, el1, el2):
        return gen_md5(el1) == gen_md5(el2)

    def cache(self, src=None, api_meta={}):
        def _cache(func):

            api_name = func.__qualname__
            func._api_meta = {'api_name': api_name}
            func._api_meta.update(api_meta)

            if src is not None:
                self.register_api(api_name, src)

            @functools.wraps(func)
            def wrapper(*args, **kw):

                key = gen_cache_key(func, *args, **kw)

                block_id = self.get_block_id(api_name)
                latest_token = self.get_latest_token(block_id)
                token = self.read_cache_token(key)

                # case 0: api not registered, hence cannot retrive latest token
                if latest_token is None:
                    LOG.info('{}: unregistered'.format(api_name))
                    return func(*args, **kw)

                # case 1: cache not found
                if token is None:
                    new_value = func(*args, **kw)
                    cache = Cache()
                    cache.header = latest_token
                    cache.body = new_value
                    self.write_cache(key, cache)
                    LOG.info('{}: cache not found, return new value '
                             'and write cache'.format(api_name))
                    return new_value

                # case 2: token outdated
                if token != latest_token:

                    cache_value = self.read_cache_value(key)
                    new_value = func(*args, **kw)

                    # case 2.1: value unchanged, only update token
                    if self.compare_equal(cache_value, new_value):
                        self.update_cache_token(key, latest_token)
                        LOG.info('{}: value unchanged, '
                                 'only update token'.format(api_name))
                        return new_value

                    # case 2.2: value changed, update cache
                    else:
                        cache = Cache()
                        cache.header = latest_token
                        cache.body = new_value
                        self.write_cache(key, cache)
                        LOG.info('{}: cache overwritten'.format(api_name))
                        return new_value

                # case 3: token validated
                if token == latest_token:
                    LOG.info('{}: cache hit'.format(api_name))
                    return self.read_cache_value(key)

            return wrapper
        return _cache


cache_manager = CacheManager(Store(), MetaDB())

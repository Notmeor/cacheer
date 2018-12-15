# -*- coding: utf-8 -*-

import os
import time
import inspect
import functools
import collections
import __main__
from pandas.io.pickle import pkl
import hashlib
import contextlib

import logging

# from cacheer.store import LmdbStore as Store, MongoMetaDB as MetaDB
from cacheer.store import SqliteCacheStore as Store, MongoMetaDB as MetaDB
from cacheer.utils import (conf, gen_md5, setup_logging, logit, timeit,
                           is_defined_in_shell)

LOG = logging.getLogger(__file__)

BASE_BLOCK_ID = '${api-fullname}'


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
        arg = collections.OrderedDict(list(arg.items())[1:])

    arg.update({'__api_meta': func._api_meta})

    key = hashlib.md5(pkl.dumps(arg, pkl.HIGHEST_PROTOCOL)).hexdigest()

    return key, arg


class Cache:
    token = ''
    hash = ''
    value = ''


class CacheManager:

    def __init__(self, cache_store, metadb):
        # TODO: implement CacheStore over LmdbStore
        self._cache_store = cache_store
        self._metadb = metadb

        self._token_prefix = '__token_'
        self._cache_meta_key = '__cache_meta'

        self._global_tags = []

        self.enable_cache()

    def __call__(self, *args, **kw):
        return self.cache(*args, **kw)

    @classmethod
    def enable_cache(cls):
        os.environ['USE_LAB_CACHE'] = 'true'

    @classmethod
    def disable_cache(cls):
        os.environ['USE_LAB_CACHE'] = 'false'

    @property
    def is_using_cache(self):
        return os.getenv('USE_LAB_CACHE') == 'true'

    @contextlib.contextmanager
    def no_cache(self):
        # TODO: sync lock
        _use_cache = self.is_using_cache
        self.disable_cache()
        yield None
        if _use_cache:
            self.enable_cache()

    def use_cache(self):
        # couterpart of no_cache
        raise NotImplementedError

    def add_tag(self, block_id, api_name=None):
        if api_name is None:  # a global tag
            self._global_tags.append(block_id)
        else:
            raise NotImplementedError

    def register_api(self, api_name, block_id):
        self._metadb.add_api(api_name, block_id)

    def _get_all_keys(self):
        # FIXME: only valid with SqliteCacheStore
        res = self._cache_store._store.read_distinct(['key'])
        return [i['key'] for i in res]

    @timeit
    def write_cache(self, key, cache):

        # TODO: remove expired cache value only when limit is about to be hit

        # write cache meta
        cache_meta = self.read_cache_meta()

        has_value = cache.hash in [v['hash'] for v in cache_meta.values()]
        
        cache_meta[key] = {
            'key': key,
            'token': cache.token,
            'hash': cache.hash
        }
        self._cache_store.write(self._cache_meta_key, cache_meta)
        
        value_stored = cache.hash in self._get_all_keys()

        if has_value or value_stored:
            LOG.info('{}: cache value already exists or is being created'
                     .format(key))
        else:
            self._cache_store.write(cache.hash, cache.value)
            LOG.info('{}: cache written'.format(key))

    @timeit
    def read_cache_meta(self, key=None):
        cache_meta = self._cache_store.read(self._cache_meta_key) or {}
        if key is not None:
            return cache_meta.get(key)
        return cache_meta

    def read_cache_token(self, key):
        meta = self.read_cache_meta(key)
        if meta is None:
            return None
        return meta['token']

    def read_cache_hash(self, key):
        meta = self.read_cache_meta(key)
        if meta is None:
            return None
        return meta['hash']

    @timeit
    def read_cache_value(self, key):
        meta = self.read_cache_meta(key)
        cache_key = meta['hash']
        # cache value might be still in writing
        # wait until value found or timeout 
        seconds_before_timeout = 3600
        while cache_key not in self._get_all_keys():
            LOG.warning('{}: cache value might be still in writing, '
                        'wait until retrival or timeout'.format(key))
            seconds_before_timeout -= 1
            if seconds_before_timeout < 0:
                raise Exception('read_cache_value timeout')
        if seconds_before_timeout < 3600:
            LOG.info(f'Waited {3600 - seconds_before_timeout} seconds')
        return self._cache_store.read(cache_key)

    def update_cache_token(self, key, token):
        cache_meta = self.read_cache_meta()
        cache_meta[key]['token'] = token
        self._cache_store.write(self._cache_meta_key, cache_meta)

    def delete_cache(self, key):

        cache_meta = self.read_cache_meta()

        # check if current key is being referred
        is_referred = False
        for k, v in cache_meta.items():
            if v['src_key'] == key:
                is_referred = True
                break

        if is_referred:
            LOG.info('{}: is currently being referred, would not remove it'
                     .format(key))
            return

        cache_meta.pop(key)
        self._cache_store.write(self._cache_meta_key, cache_meta)

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

    @timeit
    def compare_equal(self, el1, el2):
        return gen_md5(el1) == gen_md5(el2)

    def cache(self, block_id=BASE_BLOCK_ID, api_meta={}):
        def _cache(func):

            api_name = func.__module__ + '.' + func.__qualname__

            # TODO: cache for functions defined in shell
            # namespace is obscure ('__main__') for functions defined in shell
            is_shell = is_defined_in_shell(func)
            if is_shell:
                LOG.warning('{}: cache would have no effect for '
                            'functions/methods defined in interactive '
                            'shell'.format(api_name))

                @functools.wraps(func)
                def _wrapper(*args, **kw):
                    return func(*args, **kw)

                return _wrapper

            if func.__module__ == '__main__':
                api_name = __main__.__file__ + ':' + func.__qualname__

            func._api_meta = {'api_name': api_name}
            func._api_meta.update(api_meta)

            if block_id is not None:
                block_id_ = block_id
                if BASE_BLOCK_ID not in block_id:
                    block_id_ = BASE_BLOCK_ID + ';' + block_id
                block_id_ = block_id_.replace(BASE_BLOCK_ID, api_name)
                self.register_api(api_name, block_id_)

            @functools.wraps(func)
            def wrapper(*args, **kw):

                # cache disabled
                if not self.is_using_cache:
                    return func(*args, **kw)

                key, api_arg = gen_cache_key(func, *args, **kw)
                LOG.info('Request: {}, hash={}'.format(api_arg, key))

                block_id = self.get_block_id(api_name)
                tag = ';'.join([block_id] + self._global_tags)
                latest_token = self.get_latest_token(tag)
                token = self.read_cache_token(key)

                # case 0: api not registered, hence cannot retrive latest token
                if latest_token is None:
                    LOG.info('{}: fail to find upstream status in metadb'
                             .format(api_name))
                    return func(*args, **kw)

                # case 1: cache not found
                if token is None:
                    new_value = func(*args, **kw)
                    cache = Cache()
                    cache.token = latest_token
                    cache.hash, cache.value = gen_md5(new_value, value=True)
                    LOG.info('{}: cache not found, return new value '
                             'and write cache'.format(api_name))
                    self.write_cache(key, cache)
                    return new_value

                # case 2: token outdated
                if token != latest_token:

                    # cache_value = self.read_cache_value(key)
                    cache_hash = self.read_cache_hash(key)
                    new_value = func(*args, **kw)
                    new_value_hash, new_value_bytes = gen_md5(
                        new_value, value=True)

                    # case 2.1: value unchanged, only update token
                    # if self.compare_equal(cache_value, new_value):
                    if cache_hash == new_value_hash:
                        self.update_cache_token(key, latest_token)
                        LOG.info('{}: value unchanged, '
                                 'only update token'.format(api_name))
                        return new_value

                    # case 2.2: value changed, update cache
                    else:
                        cache = Cache()
                        cache.token = latest_token
                        cache.value = new_value_bytes
                        cache.hash = new_value_hash
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


@cache_manager.cache()
def try_sth2(a, b, c=None):
    return '{}+{}+{}'.format(a, b, c)




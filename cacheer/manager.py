# -*- coding: utf-8 -*-

import os
import sys
import time
import inspect
import functools
import collections
import __main__
from pandas.io.pickle import pkl
import hashlib
import contextlib

import logging

from cacheer.store import SqliteCacheStore as Store, MongoMetaDB as MetaDB
from cacheer.serializer import serializer
from cacheer.utils import timeit, is_defined_in_shell

LOG = logging.getLogger('cacheer.manager')

BASE_BLOCK_ID = '${api-fullname}'

JPY_USER = os.getenv('JPY_USER', 'null')


class CacheDataNotFound(Exception):
    pass


class CacheCorrupted(Exception):
    pass


class OriginalCallFailure(Exception):

    def __init__(self, e, *args, **kw):
        self.original_exc = e
        super().__init__(*args, **kw)


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

    meta = func._api_meta.copy()

    if _is_bound_method(func):  # pop first argument

        owner = list(arg.values())[0]
        arg = collections.OrderedDict(list(arg.items())[1:])

        for k, v in meta.items():
            if '&self.' in v:
                v_ = v.replace('&self', 'owner')
                meta[k] = eval(v_)

    arg.update({'__api_meta': meta})

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

        self._mark_as_outdated = False

        self.enable_cache()

    def __call__(self, *args, **kw):
        return self.cache(*args, **kw)

    @classmethod
    def enable_cache(cls):
        os.environ['USE_LAB_CACHE'] = 'true'

    @classmethod
    def disable_cache(cls):
        os.environ['USE_LAB_CACHE'] = 'false'

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

    @contextlib.contextmanager
    def outdate(self):
        self._mark_as_outdated = True
        yield None
        self._mark_as_outdated = False

    def use_cache(self):
        # couterpart of no_cache
        raise NotImplementedError
    
    def notify_source_update(self, block_id, meta, **kw):
        self._metadb.update(block_id, meta, **kw)

    def add_tag(self, api_name, block_id, scope='system'):
        """
        绑定标签

        Parameters
        ----------
        block_id: `str`
            数据块标签
        api_name: `str`
            接口名称, 默认不指定api，即将标签绑定至所有接口
        scope: `str`
            作用范围：
                ‘user'作用于当前实例下的所有接口，‘system'作用于所有实例
        """

        if callable(api_name):
            api_name = api_name._api_meta['__api_name']

        if scope != 'system':
            raise NotImplementedError

        tag = self._metadb.get_block_id(api_name)
        if block_id not in tag:
            self._metadb.add_api(api_name, tag + ';' + block_id)

    def remove_tag(self, api_name, block_id):
        if callable(api_name):
            api_name = api_name._api_meta['__api_name']

        tag = self._metadb.get_block_id(api_name)
        if block_id in tag:
            new_tag = tag.replace(f'{block_id}', '')
            self._metadb.add_api(api_name, new_tag)

    def list_tags(self, api_name):
        if callable(api_name):
            api_name = api_name._api_meta['__api_name']

        tag = self._metadb.get_block_id(api_name)
        return tag

    def register_api(self, api_name, block_id):
        try:
            self._metadb.add_api(api_name, block_id)
        except:
            LOG.info('Register api failed, this is ok if you haven"t been'
                     ' granted to do so')

    def get_latest_token(self, api_name):
        block_id = self._metadb.get_block_id(api_name)
        return self._metadb.get_latest_token(block_id)

    def _get_all_keys(self):
        # FIXME: only valid with SqliteCacheStore
        res = self._cache_store._store.read_distinct(['key'])
        return [i['key'] for i in res]

    def _has_key(self, key):
        return self._cache_store.has_key(key)

    def write_cache(self, key, cache):

        # TODO: remove expired cache value only when limit is about to be hit

        # write cache meta
        cache_meta = self.read_cache_meta()

        has_value = cache.hash in [v['hash'] for v in cache_meta.values()]

        meta = {
            'key': key,
            'token': cache.token,
            'hash': cache.hash
        }
        self._cache_store.write_meta(key, meta)

        # value_stored = cache.hash in self._get_all_keys()
        value_stored = self._has_key(cache.hash)

        if has_value or value_stored:
            LOG.info('{}: cache value already exists or is being created'
                     .format(key))
        else:
            self._cache_store.write(cache.hash, cache.value)
            LOG.info('{}: cache written'.format(key))

        self.clear_expired()

    def read_cache_meta(self, key=None):
        if key is None:
            return self._cache_store.read_all_meta()
        return self._cache_store.read_meta(key)

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

    def read_cache_value(self, key, meta=None):
        if meta is None:
            meta = self.read_cache_meta(key)
        cache_key = meta['hash']

        # cache value might be still in writing
        # or, if a database in use get deleted, it would lose all cache data
        # when another sqlite connection starts, but the cache metadata might
        # somehow oddly persist (bug?)

        # if cache_key not in self._get_all_keys():
        if not self._has_key(cache_key):
            LOG.warning(f'{key}: fail to retrieve cache value')
            if 'failure_time' not in meta:
                meta['failure_time'] = time.time()
                self._cache_store.write_meta(key, meta)
            else:
                if time.time() - meta['failure_time'] > 600:
                    self._cache_store.delete_meta(key)
                    LOG.warning(f'{key}: cache corrupted, would be removed')
                    raise CacheCorrupted
            raise CacheDataNotFound

        cache_value = self._cache_store.read(cache_key)
        if cache_value is None:
            if not serializer.gen_md5(cache_value) == cache_key:
                LOG.warning(f'{key}; cache value might be lost for a db reset')
                raise CacheDataNotFound

        LOG.info(f'{key}: cache loaded')
        return cache_value

    def update_cache_meta(self, key, meta):
        self._cache_store.write_meta(key, meta)

    def delete_cache(self, key):

        # logically delete cache
        self._cache_store.delete_meta(key)
        # TODO: remove expired

    def clear_expired(self):
        pass

    def _remove_corrupted_cache(self, key):
        self._cache_store.delete_meta(key)

    def _parse_key_as_params(self, key):
        """
        cache naming
            {class_name}_{class_signature}_{method_name}_{arguments}
        """

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

            func._api_meta = {'__api_name': api_name}
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
                if self.is_using_cache() is False:
                    try:
                        return func(*args, **kw)
                    except:
                        raise

                try:

                    key, api_arg = gen_cache_key(func, *args, **kw)
                    LOG.info('JPY_USER: {}, Request: {}, hash={}'.format(
                        JPY_USER, api_arg, key))

                    latest_token = self.get_latest_token(api_name)

                    cache_meta = self.read_cache_meta(key) or {}
                    token = cache_meta.get('token')

                    # case 0: api not registered, hence cannot retrive
                    # latest token
                    # TODO: to be removed
                    if latest_token is None:
                        LOG.info('{}: fail to find upstream status in metadb'
                                 .format(api_name))

                        try:
                            return func(*args, **kw)
                        except Exception as e:
                            raise OriginalCallFailure(e)

                    # case 1: cache not found
                    if token is None:

                        try:
                            new_value = func(*args, **kw)
                        except Exception as e:
                            raise OriginalCallFailure(e)

                        cache = Cache()
                        cache.token = latest_token
                        cache.hash, cache.value = serializer.gen_md5(
                            new_value, value=True)

                        LOG.info('{}: cache not found, return new value '
                                 'and write cache'.format(api_name))
                        self.write_cache(key, cache)
                        return new_value

                    # case 2: token outdated
                    if token < latest_token or self._mark_as_outdated:

                        # cache_value = self.read_cache_value(key)
                        cache_hash = cache_meta['hash']

                        try:
                            new_value = func(*args, **kw)
                        except Exception as e:
                            raise OriginalCallFailure(e)

                        new_value_hash, new_value_bytes = serializer.gen_md5(
                            new_value, value=True)

                        # case 2.1: value unchanged, only update token
                        # if self.compare_equal(cache_value, new_value):
                        if cache_hash == new_value_hash:
                            cache_meta['token'] = latest_token
                            self.update_cache_meta(key, cache_meta)
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
                    if token >= latest_token:
                        LOG.info('{}: cache hit'.format(api_name))
                        try:
                            return self.read_cache_value(key, meta=cache_meta)
                        except (CacheDataNotFound, CacheCorrupted):

                            try:
                                ret = func(*args, **kw)
                                LOG.info(f'{api_name}: skip cache')
                                return ret
                            except Exception as e:
                                raise OriginalCallFailure(e)

                except OriginalCallFailure as e:
                    LOG.info(f'{api_name}: original call failed')
                    raise e.original_exc
                except KeyboardInterrupt:
                    raise
                except:
                    LOG.error(f'{api_name}: cached call failed, '
                              'fallback to original call', exc_info=True)
                    try:
                        self._remove_corrupted_cache(key)
                    except:
                        LOG.error(
                            'Remove corrupted cache failed', exc_info=True)
                    return func(*args, **kw)

            return wrapper
        return _cache


cache_manager = CacheManager(Store(), MetaDB())

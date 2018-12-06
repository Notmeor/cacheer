# -*- coding: utf-8 -*-

import inspect
import functools

from pandas.io.pickle import pkl
import hashlib

import pymongo

import logging

from cacheer.store import LmdbStore as Store, LmdbMetaDB as MetaDB
from cacheer.utils import conf, gen_md5, setup_logging, logit


setup_logging()
LOG = logging.getLogger(__file__)


def gen_cache_key(func, *args, **kw):
    signature = inspect.signature(func)
    bound_arg = signature.bind(*args, **kw)
    bound_arg.apply_defaults()
    arg = bound_arg.arguments
    key = hashlib.md5(pkl.dumps(arg, pkl.HIGHEST_PROTOCOL)).hexdigest()
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

    def __init__(self, cache_store, cache_monitor):
        self._cache_store = cache_store
        self._cache_monitor = cache_monitor

    def add(self, key, cache):
        self._cache_store.write(key, cache)

    def read(self, key):
        cache = self._cache_store.read(key)
        return cache

    def delete(self, key):
        pass

    def _parse_key_as_params(self, key):
        """
        cache naming
            {class_name}_{class_signature}_{method_name}_{arguments}
        """

    def compare_equal(self, el1, el2):
        return gen_md5(el1) == gen_md5(el2)

    def cache(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            api_name = func.__qualname__
            key = gen_cache_key(func, *args, **kw)
            cache = self.read(key)
            # cache_ts, value = cache.header, cache.body

            block_id = api_map[api_name]
            latest_token = self._cache_monitor.get_latest_token(block_id)

            if latest_token is None:
                return func(*args, **kw)
                LOG.info('meta of {} not found, '
                         'cache disabled'.format(block_id))

            if cache is not None:
                LOG.info('cache loaded: {}'.format(cache.header))
                if latest_token != cache.header:
                    LOG.info('cache validated: {}'.format(cache.header))
                    return cache.body

            new_value = func(*args, **kw)

            if cache is not None:
                if self.compare_equal(cache.body, new_value):
                    LOG.info('compared equal: {}'.format(cache.header))
                    return cache.body

            self.add(key, Cache(latest_token, new_value))
            LOG.info('cache overwritten: {}'.format(latest_token))
            return new_value
        return wrapper


api_map = {
    'TestPortal.get_quote': 'main-db:test_cacheer.quote',
    'TestPortal.get_stock_day_forward': 'stock_quote.day_new_forward'
}


class CacheMonitor:

    def __init__(self):
        self._metadb = MetaDB()

    def get_latest_token(self, block_id):
        return self._metadb.get_latest_token(block_id)

    def validate(self, api_name, token):
        block_id = api_map[api_name]
        if token != self.get_latest_token(block_id):
            return True
        return False


#class CacheMonitor:
#
#    def __init__(self):
#        self._stat_coll = None
#        self._update_status = {}
#        self._init_meta_db()
#
#    def _init_meta_db(self):
#        """
#        block_id takes the form of 'database-alias:table-namespace[;block_id]'
#        """
#        self._stat_coll = pymongo.MongoClient(
#            conf['mongo_uri'])['__update_history']['status']
#        self.refresh_datasource_stat()
#
#    @logit(LOG.info, after='refreshed_datasource_stat')
#    def refresh_datasource_stat(self):
#        docs = list(self._stat_coll.find())
#        self._update_status = {doc['block_id']: doc for doc in docs}
#
#    def get_last_ts(self, ns):
#        self.refresh_datasource_stat()
#        return self._update_status[ns]['dt']
#
#    def validate(self, api_name, cache_ts):
#        ns = api_map[api_name]
#        if cache_ts > self.get_last_ts(ns):
#            return True
#        return False


cache_manager = CacheManager(CacheStore(), CacheMonitor())

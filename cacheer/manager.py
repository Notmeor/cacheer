# -*- coding: utf-8 -*-

import inspect
import functools

from pandas.io.pickle import pkl
import hashlib

from cacheer.store import LmdbStore as Store 


def gen_cache_key(func, *args, **kw):
    signature = inspect.signature(func)
    bound_arg = signature.bind(*args, **kw)
    bound_arg.apply_defaults()
    arg = bound_arg.arguments
    key = hashlib.md5(pkl.dumps(arg, pkl.HIGHEST_PROTOCOL)).hexdigest()
    return key


class Cache:
    def __init__(self, header, body):
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

    def __init__(self, cache_store):
        self._cache_store = cache_store

    def add(self, key, value):
        self._cache_store.write(key, value)

    def read(self, key):
        return self._cache_store.read(key)

    def delete(self, key):
        pass

    def _parse_key_as_params(self, key):
        """
        cache naming
            {class_name}_{class_signature}_{method_name}_{arguments}
        """

    def cache(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            api_name = func.__qualname__
            key = gen_cache_key(func, *args, **kw)
            cache = self.read(key)
            cache_ts, value = cache.header, cache.body
            
            if value is not None:
                print('读取cache')
                if self._cache_moitor.validate(api_name, cache_ts):
                    return value

            value = func(*args, **kw)
            ts = self._cache_monitor.get_last_ts(api_map[api_name])
            self.add(key, Cache(ts, value))
            return value
        return wrapper


api_map = {
    'TestPortal.get_quote': 'test_cacheer.quote0'
}


class CacheMonitor:
    
    def __init__(self):
        self._update_status = {}
    
    def get_last_ts(self, ns):
        return self._update_status[ns]['dt']
    
    def validate(self, api_name, cache_ts):
        ns = api_map[api_name]
        if cache_ts > self.get_last_ts(ns):
            return True
        
        return False


cache_manager = CacheManager(CacheStore())



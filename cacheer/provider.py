# -*- coding: utf-8 -*-


class CacheProvider:

    def __init__(self, manager=None):
        self._cache_manager = manager or self

    def get(self, key):
        if key not in self._cache_manager.get_all_keys():
            self._cache_manager.add(key)
        return self._cache_manager.load(key)

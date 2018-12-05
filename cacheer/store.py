# -*- coding: utf-8 -*-

import os
import lmdb

from pandas.io.pickle import pkl


def serialize(obj):
    return pkl.dumps(obj, pkl.HIGHEST_PROTOCOL)


def deserialize(b):
    return pkl.loads(b)


class LmdbStore:

    def __init__(self):
        try:
            db_path = os.path.join(os.path.dirname(__file__), 'lmdb')
        except NameError:  # so it would work in python shell
            db_path = os.path.join(os.path.realpath(''), 'lmdb')
        self._env = lmdb.open(db_path, map_size=1024 * 1024 * 10)

    def __enter__(self):
        pass

    def __exit__(self, *args, **kw):
        pass

    def write(self, key, value):
        with self._env.begin(write=True) as txn:
            txn.put(key.encode(), serialize(value))

    def read(self, key):
        with self._env.begin() as txn:
            value = txn.get(key.encode())
        if value is not None:
            return deserialize(value)

    def delete(self, key):
        with self._env.begin(write=True) as txn:
            txn.delete(key.encode())

    def close(self):
        self._env.close()

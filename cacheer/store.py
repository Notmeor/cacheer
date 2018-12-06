# -*- coding: utf-8 -*-

import os
import lmdb

from cacheer.utils import serialize, deserialize


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


class MetaDB:
    """
    Collects update stats of all lab databases
    """

    def get_latest_token(self, block_id):
        raise NotImplementedError

    def update(self, block_id, meta):
        raise NotImplementedError


class LmdbMetaDB(MetaDB):

    def __init__(self):
        self._prefix = '__meta_'
        self._store = LmdbStore()

    def get_latest_token(self, block_id):
        key = self._prefix + block_id
        meta = self._store.read(key)
        if meta:
            token = meta['dt']
            return token

    def update(self, block_id, meta):
        key = self._prefix + block_id
        self._store.write(key, meta)

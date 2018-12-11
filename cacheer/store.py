# -*- coding: utf-8 -*-

import os
import lmdb
import pymongo

import logging

from cacheer.utils import serialize, deserialize, conf, timeit

LOG = logging.getLogger(__file__)


class LmdbStore:

    def __init__(self):

        db_path = conf['lmdb-uri']
        if not db_path:
            try:
                db_path = os.path.join(os.path.dirname(__file__), 'lmdb')
            except NameError:  # so it would work in python shell
                db_path = os.path.join(os.path.realpath(''), 'lmdb')

        map_size = conf['map-size'] or 1024 * 1024 * 10
        self._env = lmdb.open(db_path, map_size=map_size)

    def __enter__(self):
        pass

    def __exit__(self, *args, **kw):
        pass

    def write(self, key, value):
        with self._env.begin(write=True) as txn:
            txn.put(key.encode(), serialize(value))

    @timeit
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

    def add_api(self, api_name, block_id):
        raise NotImplementedError

    def get_block_id(self, api_key):
        raise NotImplementedError

    def get_latest_token(self, block_id):
        raise NotImplementedError

    def update(self, block_id, meta):
        raise NotImplementedError


class MongoMetaDB(MetaDB):

    def __init__(self):

        self._update_coll = pymongo.MongoClient(
            conf['metadb-uri'])['__update_history']['status']
        self._api_map_coll = pymongo.MongoClient(
            conf['metadb-uri'])['__update_history']['api_map']

        self._api_map = {}
        self.load_api_map()

    def add_api(self, api_name, block_id):
        self._api_map_coll.update_one(
            filter={'api_name': api_name},
            update={'$set': {'api_name': api_name,
                             'block_id': block_id}},
            upsert=True
        )

        self.load_api_map()

    def load_api_map(self):
        docs = list(self._api_map_coll.find())
        self._api_map = {doc['api_name']: doc for doc in docs}

    def get_block_id(self, api_id):
        return self._api_map[api_id]['block_id']

    def get_latest_token(self, block_id):

        meta = self._update_coll.find_one({
            'block_id': block_id})

        if meta:
            token = meta['dt']
            return token

    def update(self, block_id, meta):

        self._update_coll.update_one(
            filter={'block_id': block_id},
            update={'$set': meta},
            upsert=True)


class LmdbMetaDB(MetaDB):

    def __init__(self):
        self._prefix = '__meta_'
        self._store = LmdbStore()

    def get_latest_token(self, block_id):
        key = self._prefix + block_id
        LOG.warning('get: {} {}'.format(block_id, key))
        meta = self._store.read(key)
        if meta:
            token = meta['dt']
            return token

    def update(self, block_id, meta):
        key = self._prefix + block_id
        LOG.warning('set: {} {}'.format(block_id, key))
        self._store.write(key, meta)

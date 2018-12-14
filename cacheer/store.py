# -*- coding: utf-8 -*-

import os
import datetime

import contextlib
import functools
import lmdb
import pymongo

import logging

from cacheer.utils import serialize, deserialize, conf, timeit, gen_md5

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
        b_value = serialize(value)
        value_hash = gen_md5(b_value)
        with self._env.begin(write=True) as txn:
            txn.put(key.encode(), b_value)
        return value_hash

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


#class CacheMeta:
#
#    key = ''
#    hash = ''
#    token = ''
#
#
#class CacheStore(LmdbStore):
#
#    def __init__(self):
#        self._cache_meta_key = b'__cache_meta'
#        # self._cache_meta_value = {}
#
#    def write(self, key, value):
#        with self._env.begin(write=True) as txn:
#
#            meta_key = self._cache_meta_key
#            res = txn.get(meta_key)
#            if res is None:
#                meta_value = []
#            else:
#                meta_value = deserialize(res)
#
#            b_value = serialize(value)
#            b_value_hash = gen_md5(b_value)
#
#            meta_value.append({
#                    })
#
#            txn.put(meta_key,
#                    serialize(self._cache_meta_value))
#            txn.put(key.encode(), serialize(value))


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

        self._metadb_uri = conf['metadb-uri']
        self._update_coll = '__update_history.status'
        self._api_map_coll = '__update_history.api_map'

        self._api_map = {}
        self.load_api_map()

    @contextlib.contextmanager
    def _open_mongo(self, ns):
        db_name, coll_name = ns.split('.', 1)
        client = pymongo.MongoClient(self._metadb_uri)
        coll = client[db_name][coll_name]
        yield coll
        client.close()

    def add_api(self, api_name, block_id):
        with self._open_mongo(self._api_map_coll) as coll:
            coll.update_one(
                filter={'api_name': api_name},
                update={'$set': {'api_name': api_name,
                                'block_id': block_id}},
                upsert=True
            )

            self.load_api_map(coll)

    def load_api_map(self, coll=None):
        if coll is None:
            with self._open_mongo(self._api_map_coll) as coll:
                docs = list(coll.find())
        else:
            docs = list(coll.find())
        self._api_map = {doc['api_name']: doc for doc in docs}

    def get_block_id(self, api_id):
        return self._api_map[api_id]['block_id']

    def _split_block_id(self, block_id):
        return [i for i in block_id.split(';') if i != '']
    
    def get_latest_token(self, block_id):

        sub_block_ids = self._split_block_id(block_id)

        with self._open_mongo(self._update_coll) as coll:
            token = None
            for sub_id in sub_block_ids:
                meta = coll.find_one({
                    'block_id': sub_id})
                if meta:
                    if token is None:
                        token = meta['dt']
                    elif token < meta['dt']:
                        token = meta['dt']

        return token

    def update(self, block_id, meta):
        
        sub_block_ids = self._split_block_id(block_id)

        with self._open_mongo(self._update_coll) as coll:
            for sub_id in sub_block_ids:
                coll.update_one(
                    filter={'block_id': sub_id},
                    update={'$set': meta},
                    upsert=True)


def update_metadb(block_id):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            mt = MongoMetaDB()
            ret = func(*args, **kw)
            mt.update(block_id, meta={'dt': datetime.datetime.now()})
            return ret
        return wrapper
    return decorator

notify_data_change = update_metadb


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

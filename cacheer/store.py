# -*- coding: utf-8 -*-

import os
import datetime

import contextlib
import functools
import lmdb
import pymongo
import sqlite3

import logging

import threading

from cacheer.utils import serialize, deserialize, conf, timeit, gen_md5

LOG = logging.getLogger(__file__)


class LmdbStore:

    def __init__(self):

        self.db_path = db_path = conf['lmdb-uri']
        if not db_path:
            try:
                db_path = os.path.join(os.path.dirname(__file__), 'lmdb')
            except NameError:  # so it would work in python shell
                db_path = os.path.join(os.path.realpath(''), 'lmdb')

        self.map_size = map_size = conf['map-size'] or 1024 * 1024 * 10

        self._envs = {}

        # init lmdb
        env = lmdb.open(db_path, map_size=map_size)
        env.close()
        self._env = lmdb.open(db_path, map_size=map_size, readonly=True)

#    @property
#    def _env(self):
#        pid = os.getpid()
#        if pid not in self._envs:
#            self._envs[pid] = lmdb.open(self.db_path, map_size=self.map_size)
#        return self._envs[pid]

    def __enter__(self):
        pass

    def __exit__(self, *args, **kw):
        pass

    def write(self, key, value):
        # self._write(key, value, env=self._write_env)
        threading.Thread(target=self._write, args=(key, value)).start()

    def _write(self, key, value, env=None):
        env = env or lmdb.open(
            self.db_path, map_size=self.map_size)

        b_value = serialize(value)
        value_hash = gen_md5(b_value)
        with env.begin(write=True) as txn:
            txn.put(key.encode(), b_value)
        
        env.close()
        return value_hash

    @timeit
    def read(self, key):
        with self._env.begin() as txn:
            value = txn.get(key.encode())
        if value is not None:
            return deserialize(value)

    def delete(self, key):
        # self._delete(key, env=self._write_env)
        threading.Thread(target=self._delete, args=(key,)).start()

    def _delete(self, key, env=None):
        env = env or lmdb.open(
            self.db_path, map_size=self.map_size)

        with env.begin(write=True) as txn:
            txn.delete(key.encode())
        
        env.close()

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


class SqliteStore(object):

    # TODO: use sqlalchemy

    def __init__(self, db_name, table_name, fields):
        self.table_name = table_name

        assert 'id' not in [f.lower() for f in fields]
        self.fields = fields

        self._conn = sqlite3.connect(db_name + '.db')

        # self._conn.row_factory = sqlite3.Row
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
        self._conn.row_factory = dict_factory

        self.assure_table(table_name)

    def close(self):
        self._conn.commit()
        self._conn.close()

    def __del__(self):
        # FIXME: use context manager
        self.close()

    def assure_table(self, name):
        try:
            cursor = self._conn.cursor()
            cursor.execute("SELECT * FROM {} LIMIT 1".format(name))
        except sqlite3.OperationalError:
            fields_str = ','.join(self.fields)
            fields_str = 'ID INTEGER PRIMARY KEY,' + fields_str
            cursor.execute("CREATE TABLE {} ({})".format(name, fields_str))
            self._conn.commit()

    def write(self, doc):

        cursor = self._conn.cursor()
        statement = "INSERT INTO {} ({}) VALUES ({})".format(
            self.table_name,
            ','.join(doc.keys()),
            ','.join(['?'] * len(doc))
        )
        cursor.execute(statement, list(doc.values()))
        self._conn.commit()

    def write_many(self, docs):
        list(map(self.write, docs))

    def read(self, query=None, limit=None):

        statement = "SELECT {} FROM {}".format(
            ','.join(self.fields), self.table_name)
        if query:
            query_str = self._format_condition(query)
            statement += " WHERE {}".format(query_str)

        if limit:
            statement += " ORDER BY ID DESC LIMIT {}".format(limit)

        cursor = self._conn.cursor()
        return cursor.execute(statement).fetchall()

    def read_latest(self, query):
        query_str = self._format_condition(query)
        statement = (
            "SELECT {fields} FROM {table} WHERE ID in" +
            "(SELECT MAX(ID) FROM {table} WHERE {con} GROUP BY code)"
        ).format(
            fields=','.join(self.fields),
            con=query_str,
            table=self.table_name)

        cursor = self._conn.cursor()
        return cursor.execute(statement).fetchall()

    def read_distinct(self, fields):
        cursor = self._conn.cursor()
        ret = cursor.execute("SELECT DISTINCT {} FROM {}".format(
                ','.join(fields), self.table_name))
        return ret

    @staticmethod
    def _format_assignment(doc):
        s = str(doc)
        formatted = s[2:-1].replace(
            "': ", '=').replace(", '", ',')
        return formatted

    @staticmethod
    def _format_condition(doc):
        if isinstance(doc, str):
            return doc
        s = str(doc)
        formatted = s[2:-1].replace(
            "': ", '=').replace(", '", ',').replace(',', ' and ')
        return formatted

    def update(self, query, document):
        cursor = self._conn.cursor()

        query_str = self._format_condition(query)
        document_str = self._format_assignment(document)

        statement = "UPDATE {} SET {} WHERE {}".format(
            self.table_name,
            document_str,
            query_str
        )

        cursor.execute(statement)
        self._conn.commit()

    def delete(self, query):
        raise NotImplementedError
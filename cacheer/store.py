# -*- coding: utf-8 -*-

import os
import datetime
import time

import collections
import contextlib
import functools

import pymongo
import sqlite3

import logging

import multiprocessing
import threading

import math

from cacheer.serializer import serializer
from cacheer.settings import conf
from cacheer.utils import timeit

LOG = logging.getLogger('cacheer.manager')


# TODO: try apache-ignite/apache-arrow

class LmdbStore:
    """
    Deprecated
    """

    def __init__(self):

        import lmdb

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
        # self._env = lmdb.open(db_path, map_size=map_size, readonly=True)

    @property
    def _env(self):
        pid = os.getpid()
        if pid not in self._envs:
            self._envs[pid] = lmdb.open(self.db_path,
                                        map_size=self.map_size,
                                        readonly=True)
        return self._envs[pid]

    def __enter__(self):
        pass

    def __exit__(self, *args, **kw):
        pass

    def write(self, key, value):
        # self._write(key, value, env=self._write_env)
        worker = multiprocessing.Process
        worker(target=self._write, args=(key, value)).start()

    def _write(self, key, value, env=None):
        env = env or lmdb.open(
            self.db_path, map_size=self.map_size)

        b_value = serializer.serialize(value)
        value_hash = serializer.gen_md5(b_value)
        with env.begin(write=True) as txn:
            txn.put(key.encode(), b_value)

        env.close()
        return value_hash

    def read(self, key):
        with self._env.begin() as txn:
            value = txn.get(key.encode())
        if value is not None:
            return serializer.deserialize(value)

    def delete(self, key):
        # self._delete(key, env=self._write_env)
        worker = multiprocessing.Process
        worker(target=self._delete, args=(key,)).start()

    def _delete(self, key, env=None):
        env = env or lmdb.open(
            self.db_path, map_size=self.map_size)

        with env.begin(write=True) as txn:
            txn.delete(key.encode())

        env.close()

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

        self._metadb_uris = conf['metadb-uris']

        self._status_coll_name = '__update_status'

        self._update_coll = '__update_history.status'
        self._api_map_coll = '__update_history.api_map'

        self._update_interval = 10
        self._update_time = time.time()
        self._update_status = {}
        self._update_status_first_loading = False

        self._api_map = {}
        self._api_map_first_loading = False

    def read_update_status(self):
        update_stats = {}
        for uri in self._metadb_uris:
            with pymongo.MongoClient(uri) as cl:
                coll = cl.get_database()[self._status_coll_name]
                docs = coll.find({}, {'_id': False})
                for doc in docs:
                    block_id, dt = doc['block_id'], doc['dt']
                    _should_update = (block_id not in update_stats or
                                      update_stats[block_id] < dt)
                    if _should_update:
                        update_stats[block_id] = dt
        self._update_status = update_stats

    def _refresh_update_status(self):
        now = time.time()
        _should_update = ((not self._update_status_first_loading) or
                          (now - self._update_time >= self._update_interval))
        if _should_update:
            self.read_update_status()
            self._update_status_first_loading = True
            self._update_time = now

    @contextlib.contextmanager
    def _open_mongo(self, ns):
        db_name, coll_name = ns.split('.', 1)
        client = pymongo.MongoClient(self._metadb_uris[0])
        coll = client[db_name][coll_name]
        yield coll
        client.close()

    def add_api(self, api_name, block_id):

        with self._open_mongo(self._api_map_coll) as coll:
            coll.update_one(
                filter={'api_name': api_name},
                update={'$set': {'block_id': block_id}},
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
        if not self._api_map_first_loading:
            self.load_api_map()
            self._api_map_first_loading = True

        api_tag = self._api_map[api_id]['block_id']
        glob_tag = self._api_map['*']['block_id']

        if api_id == '*':
            return glob_tag
        return glob_tag + ';' + api_tag

    def _split_block_id(self, block_id):
        return [i for i in block_id.split(';') if i != '']

    def get_latest_token(self, block_id):

        self._refresh_update_status()

        sub_block_ids = self._split_block_id(block_id)
        token = datetime.datetime(1970, 1, 1)
        for sub_id in sub_block_ids:
            dt = self._update_status.get(sub_id, None)

            # dt=0 would virtually disable cache
            if dt == 0:
                return None

            if dt is not None:
                if token < dt:
                    token = dt

        return token

    def update(self, block_id, meta, db_num=0, coll=None):

        def _update(coll):
            sub_block_ids = self._split_block_id(block_id)
            for sub_id in sub_block_ids:
                coll.update_one(
                    filter={'block_id': sub_id},
                    update={'$set': meta},
                    upsert=True)

        if coll is not None:
            _update(coll)
        else:
            with pymongo.MongoClient(self._metadb_uris[db_num]) as cl:
                coll = cl.get_database()[self._status_coll_name]
                _update(coll)



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

        self._max_length = 1000000000

        self.table_name = table_name
        self.db_name = db_name

        assert 'id' not in [f.lower() for f in fields]
        self.fields = fields

        self._indexed_fields = collections.OrderedDict()
        self._conns = {}

        self._db_initialized = False

    @property
    def _conn_id(self):
        return (os.getpid(), threading.get_ident())

    @property
    def _conn(self):

        if not self._db_initialized:
            self._db_initialized = True
            self.assure_table()

        conn_id = self._conn_id

        if conn_id not in self._conns:
            self._conns[conn_id] = conn = sqlite3.connect(
                self.db_name + '.db')

            def dict_factory(cursor, row):
                d = {}
                for idx, col in enumerate(cursor.description):
                    d[col[0]] = row[idx]
                return d

            conn.row_factory = dict_factory

        return self._conns[conn_id]

    def close(self):
        # self._conn.commit()
        self._conns.clear()

    def assure_table(self, name=None):
        if name is None:
            name = self.table_name

        with self._conn:
            try:
                self._conn.execute("SELECT * FROM {} LIMIT 1".format(name))
            except sqlite3.OperationalError:
                fields_str = ','.join(self.fields)
                fields_str = 'ID INTEGER PRIMARY KEY,' + fields_str
                self._conn.execute(
                    "CREATE TABLE {} ({})".format(name, fields_str))

        self.assure_index()

    def reset_table(self, name):
        with self._conn:
            fields_str = ','.join(self.fields)
            fields_str = 'ID INTEGER PRIMARY KEY,' + fields_str
            self._conn.execute(
                "CREATE TABLE {} ({})".format(name, fields_str))

        self.delete({})

    def reset_connection(self, exc=None):
            # reset conn if underlying sqlite gets deleted
            LOG.warning(str(exc) + '. Would reset connection')
            try:
                LOG.error('Try closing busy conn')
                self._conn.close()
            except:
                LOG.error('Close busy conn failed', exc_info=True)
            self._conns.pop(self._conn_id)
            self.assure_table()

    def add_index(self, field, unique=False):
        self._indexed_fields[field] = {'unique': unique}

    def assure_index(self):
        for key, meta in self._indexed_fields.items():
            self._add_index(key, unique=meta['unique'])

    def _add_index(self, key, unique):
        with self._conn:
            name = f'{key}_'
            stmt = (f"SELECT * FROM sqlite_master WHERE type ="
                    f" 'index' and tbl_name = '{self.table_name}'"
                    f" and name = '{name}'")

            if self._conn.execute(stmt).fetchone() is None:
                _unique = 'UNIQUE' if unique else '' 
                self._conn.execute(
                    f"CREATE {_unique} INDEX {name} ON "
                    f"{self.table_name}({key})")

    def write(self, doc):

        statement = "INSERT INTO {} ({}) VALUES ({})".format(
            self.table_name,
            ','.join(doc.keys()),
            ','.join(['?'] * len(doc))
        )

        def _write():
            with self._conn:
                    self._conn.execute(statement, tuple(doc.values()))

        try:
            _write()
        except sqlite3.OperationalError as e:
            self.reset_connection(exc=e)
            _write()

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

        def _read():
            with self._conn:
                ret = self._conn.execute(statement).fetchall()
            return ret

        try:
            return _read()
        except sqlite3.OperationalError as e:
            self.reset_connection(exc=e)
            return _read()

    def read_latest(self, query, by):
        query_str = self._format_condition(query)
        statement = (
            "SELECT {fields} FROM {table} WHERE ID in" +
            "(SELECT MAX(ID) FROM {table} WHERE {con} GROUP BY {by})"
        ).format(
            fields=','.join(self.fields),
            con=query_str,
            table=self.table_name,
            by=by)
        print(statement)
        with self._conn:
            ret = self._conn.execute(statement).fetchall()

        return ret

    def read_distinct(self, fields):
        with self._conn:
            ret = self._conn.execute("SELECT DISTINCT {} FROM {}".format(
                ','.join(fields), self.table_name)).fetchall()
        return ret

    @staticmethod
    def _format_assignment(doc):
        r = []
        for k in doc:
            r.append(f'{k}=?')
        return ','.join(r)

    @staticmethod
    def _format_condition(doc):
        if not doc:
            return 'TRUE'
        s = str(doc)
        formatted = s[2:-1].replace(
            "': ", ' = ').replace(
            ", '", ',').replace(
            "= {'$like =", 'like').replace(
            '}', '')
        return formatted

    def update(self, query, document):

        query_str = self._format_condition(query)
        document_str = self._format_assignment(document)

        statement = "UPDATE {} SET {} WHERE {}".format(
            self.table_name,
            document_str,
            query_str
        )

        with self._conn:
            self._conn.execute(statement, tuple(document.values()))

    def delete(self, query):
        query_str = self._format_condition(query)
        if query_str:
            query_str = f'WHERE {query_str} '
        with self._conn:
            self._conn.execute("DELETE FROM {} {}".format(
                self.table_name,
                query_str
            ))


class SqliteCacheStore(object):

    def __init__(self, db_path=None):
        self.db_path = db_path or conf['sqlite-uri']
        self._store = SqliteStore(
            self.db_path, 'lab_cache', ['key', 'value'])
        self._store.add_index('key', unique=True)
        self._cache_meta_prefix = '__cache_meta_'

    def read(self, key):
        res = self._store.read({'key': key}, limit=1)
        assert len(res) <= 1

        if len(res) == 0:
            return None

        b_value = res[0]['value']

        if isinstance(b_value, int):  # splited
            b_value = self._read_split_blob(key, b_value)

        return serializer.deserialize(b_value)

    def _write_or_update(self, key, value):
        try:
            self._store.write({'key': key, 'value': value})
        except sqlite3.IntegrityError as e:
            if str(e).startswith('UNIQUE constraint failed'):
                LOG.warning(f'Update already existing `{key}`')
                self._store.update(query={'key': key},
                                   document={'value': value})
            else:
                raise

    def write(self, key, value):
        b_value = serializer.serialize(value)
        value_len = len(b_value)
        if value_len > self._store._max_length:
            self._split_blob_and_save(value, value_len, key)
        else:
            self._write_or_update(key, b_value)

    def _split_blob_and_save(self, blob, length, key):
        number = math.ceil(length / self._store._max_length)
        step = math.ceil(length / number)
        for idx, i in enumerate(range(0, length, step)):
            sub = blob[i:i+step]
            sub_key = f'{key}_{idx}'
            self._write_or_update(sub_key, sub)

        self._write_or_update(key, number)

    def _read_split_blob(self, key, number):
        sub_keys = [f'{key}_{i}' for i in range(number)]

        def _get_sub(k):
            res = self._store.read({'key': k}, limit=1)
            return res[0]['value']

        blob = b''.join([_get_sub(k) for k in sub_keys])
        return blob

    def has_key(self, key):
        with self._store._conn:
            ret = self._store._conn.execute(
                f"SELECT key FROM lab_cache WHERE key"
                f" = '{key}' LIMIT 1").fetchone()
        return ret is not None

    def delete(self, key):
        self._store.delete({'key': key})

    def read_meta(self, key):
        meta_key = self._cache_meta_prefix + key
        return self.read(meta_key)

    def read_all_meta(self):
        res = self._store.read(
            query={'key': {'$like': '__cache_meta%'}})
        meta = {i['key']: serializer.deserialize(i['value']) for i in res}
        return meta

    def write_meta(self, key, meta):
        meta_key = self._cache_meta_prefix + key
        self.write(meta_key, meta)

    def delete_meta(self, key):
        meta_key = self._cache_meta_prefix + key
        self.delete(meta_key)

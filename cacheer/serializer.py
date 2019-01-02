# -*- coding: utf-8 -*-

import os
import logging.config

import yaml

import time
import functools

import hashlib
import __main__

import pickle
import pyarrow as pa
import pandas as pd
import numpy as np

from cacheer.utils import timeit, conf


class Serializer:

    def serialize(*args, **kw):
        raise NotImplementedError

    def deserialize(*args, **kw):
        raise NotImplementedError

    @classmethod
    
    def gen_md5(cls, b, value=False):
        bytes_ = b if isinstance(b, bytes) else cls.serialize(b)
        md5 = hashlib.md5(bytes_).hexdigest()
        if value:
            return md5, bytes_
        return md5


def _count_elements(df):
    return np.product(df.shape)


class Picklizer(Serializer):

    @staticmethod
    
    def serialize(obj):
        if isinstance(obj, bytes):
            return obj
        return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    
    def deserialize(b):
        return pickle.loads(b)


class Picklizer1(Serializer):
    """
    Provide a quick path for serializing DataFrame using pyarrow
    serialize_pandas/deserialize_pandas api
    """

    @staticmethod
    
    def serialize(obj):
        if isinstance(obj, bytes):
            return obj
        if isinstance(obj, pd.DataFrame) and np.product(obj.shape) > 30000:
            pa_buffer = pa.serialize_pandas(obj)
            return pa_buffer.to_pybytes()
        return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    
    def deserialize(b):
        try:
            obj = pickle.loads(b)
        except pickle.UnpicklingError:
            obj = pa.deserialize_pandas(b)
        return obj


class Picklizer2(Serializer):
    """
    Provide a quick path for serializing DataFrame using pyarrow Table
    as an intermediate
    """

    @staticmethod
    
    def to_table(df):
        return pa.Table.from_pandas(df)

    @staticmethod
    
    def to_dataframe(table):
        return table.to_pandas()

    @classmethod
    
    def serialize(cls, obj):
        if isinstance(obj, bytes):
            return obj
        if isinstance(obj, pd.DataFrame) and np.product(obj.shape) > 30000:
            obj = cls.to_table(obj)
        return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

    @classmethod
    
    def deserialize(cls, b):
        obj = pickle.loads(b)
        if isinstance(obj, pa.Table):
            obj = cls.to_dataframe(obj)
        return obj


class Picklizer3(Serializer):
    """
    Provide a quick path for serializing DataFrame using the conversion
    between `category` and `object` dtypes
    """

    @staticmethod
    
    def _is_categorized(df):
        for d in df.dtypes:
            if d.name == 'category':
                return True
        return False

    @classmethod
    
    def categorize(cls, df, copy=False):

        # TODO: use with metadata
        if cls._is_categorized(df):
            raise TypeError('Categorical are not supported by this serializer')

        df = df.copy() if copy else df
        # FIXME: multi column
        for name in df.columns:
            if df[name].dtype.name == 'object':
                df[name] = df[name].astype('category', copy=False)
        return df

    @staticmethod
    
    def decategorize(df):
        """
        experimental
        """
        # FIXME: have to be used with metadata
        for name in df.columns:
            if df[name].dtype.name == 'category':
                df[name] = df[name].astype('object', copy=False)
        return df

    @classmethod
    
    def serialize(cls, obj):
        if isinstance(obj, bytes):
            return obj
        if isinstance(obj, pd.DataFrame) and np.product(obj.shape) > 30000:
            obj = cls.categorize(obj, copy=True)
        return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize(cls, b):
        obj = pickle.loads(b)
        if isinstance(obj, pd.DataFrame):
            obj = cls.decategorize(obj)
        return obj


serializer_type = conf.get('serializer-type', 3)
serializer = {
    0: Picklizer,
    1: Picklizer1,
    2: Picklizer2,
    3: Picklizer3
}[serializer_type]


def benchmark_object(obj, number=5):
    import timeit

    ps = Picklizer(), Picklizer1(), Picklizer2(), Picklizer3()

    ser_res, deser_res = [], []
    for p in ps:

        ser_res.append(timeit.timeit(
            lambda: p.serialize(obj),
            number=number))

        serialized_obj = p.serialize(obj)
        deser_res.append(timeit.timeit(
            lambda: p.deserialize(serialized_obj),
            number=number))
        assert isinstance(p.deserialize(serialized_obj), pd.DataFrame)

    return [ser_res, deser_res]

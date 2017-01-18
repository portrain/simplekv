#!/usr/bin/env python
# -*- coding: utf-8 -*-

from io import BytesIO
from .. import KeyValueStore


class ConsulStore(KeyValueStore):
    """Uses a consul key-value store as the backend.

    :param consul: An instance of :py:class:`consul.Consul`.
    """

    def __init__(self, consul):
        self.consul = consul

    def _delete(self, key):
        self.consul.kv.delete(key)

    def keys(self):
        index, keys = self.consul.kv.get('', keys=True)
        return keys

    def iter_keys(self):
        return iter(self.keys())

    def _has_key(self, key):
        index, key = self.consul.kv.get(key, keys=True)
        return key is None

    def _get(self, key):
        index, val = self.consul.kv.get(key)

        if val is None:
            raise KeyError(key)
        return val['Value']

    def _get_file(self, key, file):
        file.write(self._get(key))

    def _open(self, key):
        return BytesIO(self._get(key))

    def _put(self, key, value):
        if not self.consul.kv.put(key, value):
            raise IOError('Failed to store data')
        return key

    def _put_file(self, key, file):
        self._put(key, file.read())
        return key


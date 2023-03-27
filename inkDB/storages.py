#!/usr/bin/env python3

## storage functionalities

from abc import ABCMeta, abstractmethod
import os

from .utils import with_metaclass


try:
    import ujson as json
except ImportError:
    import json


def touch(fname, times=None, create_dirs=False):
    if create_dirs:
        base_dir = os.path.dirname(fname)
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
    with open(fname, 'a'):
        os.utime(fname, times)


class Storage(with_metaclass(ABCMeta, object)):
    @abstractmethod
    def read(self):
        raise NotImplementedError('To be overridden!')

    @abstractmethod
    def write(self, data):
        raise NotImplementedError('To be overridden!')

    def close(self):
        pass


class JSONStorage(Storage):
    def __init__(self, path, create_dirs=False, **kwargs):
        super(JSONStorage, self).__init__()
        touch(path, create_dirs=create_dirs) 
        self.kwargs = kwargs
        self._handle = open(path, 'r+')

    def close(self):
        self._handle.close()

    def read(self):
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        if not size:
            return None
        else:
            self._handle.seek(0)
            return json.load(self._handle)

    def write(self, data):
        self._handle.seek(0)
        serialized = json.dumps(data, **self.kwargs)
        self._handle.write(serialized)
        self._handle.flush()
        self._handle.truncate()


class MemoryStorage(Storage):
    def __init__(self,*args, **kwargs):
        super(MemoryStorage, self).__init__()
        self.memory = None

    def read(self):
        return self.memory

    def write(self, data):
        self.memory = data
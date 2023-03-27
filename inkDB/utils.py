#!/usr/bin/env python3

## utils
from contextlib import contextmanager
import warnings

iteritems = getattr(dict, 'iteritems', dict.items)
itervalues = getattr(dict, 'itervalues', dict.values)


class LRUCache(dict):
    def __init__(self, *args, **kwargs):
        self.capacity = kwargs.pop('capacity', None) or float('nan')
        self.lru = []

        super(LRUCache, self).__init__(*args, **kwargs)

    def refresh(self, key):
        if key in self.lru:
            self.lru.remove(key)
        self.lru.append(key)

    def get(self, key, default=None):
        item = super(LRUCache, self).get(key, default)
        self.refresh(key)

        return item

    def __getitem__(self, key):
        item = super(LRUCache, self).__getitem__(key)
        self.refresh(key)

        return item

    def __setitem__(self, key, value):
        super(LRUCache, self).__setitem__(key, value)

        self.refresh(key)
        if len(self) > self.capacity:
            self.pop(self.lru.pop(0))

    def __delitem__(self, key):
        super(LRUCache, self).__delitem__(key)
        self.lru.remove(key)

    def clear(self):
        super(LRUCache, self).clear()
        del self.lru[:]


def with_metaclass(meta, *bases):
    class Metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)

    return Metaclass('temporary_class', None, {})


@contextmanager
def catch_warning(warning_cls):
    with warnings.catch_warnings():
        warnings.filterwarnings('error', category=warning_cls)

        yield


class FrozenDict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))

    def _immutable(self, *args, **kws):
        raise TypeError('object is immutable')

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    update = _immutable
    setdefault = _immutable
    pop = _immutable
    popitem = _immutable


def freeze(obj):
    if isinstance(obj, dict):
        return FrozenDict((k, freeze(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return tuple(freeze(el) for el in obj)
    elif isinstance(obj, set):
        return frozenset(obj)
    else:
        return obj
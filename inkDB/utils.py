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
    
    def __delitem__(self, key):
        super(LRUCache, self).__delitem__(key)
        self.lru.remove(key)
        
    def clear(self):
        super(LRUCache, self).clear()
        del self.lru[:]
        
class FrozenDict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))
    

def freeze(obj):
    if isinstance(obj, dict):
        return FrozenDict((key, freeze(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return tuple(freeze(el) for el in obj)
    elif isinstance(obj, set):
        return frozenset(obj)  
    else:
        return obj
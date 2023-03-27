#!/usr/bin/env python3
from . import JSONStorage, MemoryStorage
from .utils import LRUCache, iteritems, itervalues


class Element(dict):
    def __init__(self, value=None, id=None, **kwargs):
        super(Element, self).__init__(**kwargs)

        if value is not None:
            self.update(value)
            self.id = id


class StorageProxy(object):

    DEFAULT_ID_FIELD = 'id'

    def __init__(self, storage, table_name, **kwargs):
        self._storage = storage
        self._table_name = table_name
        self._id_field = kwargs.pop('id_field', StorageProxy.DEFAULT_ID_FIELD)

    def read(self):
        try:
            raw_data = (self._storage.read() or {})[self._table_name]
        except KeyError:
            self.write({})
            return {}

        data = {}
        for item in raw_data:
             id = item[self._id_field]
             data[id] = Element(item, id)

        return data

    def write(self, values):
        data = self._storage.read() or {}
        data[self._table_name] = values
        self._storage.write(data)

    def purge_table(self):
        try:
            data = self._storage.read() or {}
            del data[self._table_name]
            self._storage.write(data)
        except KeyError:
            pass

    @property
    def table_name(self):
        return self._table_name

    @property
    def id_field(self):
        return self._id_field or StorageProxy.DEFAULT_ID_FIELD



class inkDB(object):
    DEFAULT_STORAGE = JSONStorage

    def __init__(self, *args, **kwargs):

        storage = kwargs.pop('storage', inkDB.DEFAULT_STORAGE)
        cache = kwargs.pop('cache', None)
        self._opened = False

        self._storage =  cache if cache else storage(*args, **kwargs)

        self._opened = True

        self._tables = {}
        self._table = None 

    def table(self, name, **options):
        if not name:
            raise ValueError('Table name can not be None or empty.')

        if name in self._tables:
            return self._tables[name]

        table = self.table_class(StorageProxy(self._storage, name, **options), **options)

        self._tables[name] = table
        self._table = table

        table._read()

        return table

    def get(self, name):
        try:
            return self._tables[name]
        except KeyError:
            return None


    def tables(self):
        return set(self._storage.read())

    def all(self):
        return  self._storage.read()

    def purge_tables(self):
        self._storage.write({})
        self._tables.clear()

    def purge_table(self, name):
        if name in self._tables:
            del self._tables[name]

        proxy = StorageProxy(self._storage, name)
        proxy.purge_table()

    def close(self):
        self._opened = False
        self._storage.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._opened is True:
            self.close()

    def __len__(self):
        return len(self._table)

    def __iter__(self):
        return self._table.__iter__()



class Table(object):
    def __init__(self, storage, cache_size=10, **kwargs):

        self._storage = storage
        self._table_name = storage.table_name
        self._id_field = storage.id_field
        self._query_cache = LRUCache(capacity=cache_size)

        data = self._read()
        if data:
            self._last_id = max(i for i in data)
        else:
            self._last_id = 0

    def process_elements(self, func, cond=None, ids=None):
        data = self._read()
        updated_data = []

        if ids is not None:
            for id in ids:
                func(data, id)
                if id in data:
                    updated_data.append(data[id])

        else:
            ids = []

            for id in list(data):
                if cond(data[id]):
                    func(data, id)
                    ids.append(id)
                    if id in data:
                        updated_data.append(data[id])

        
        new_data = list(data.values())

        self._write(new_data)

        return ids, updated_data

    def clear_cache(self):
        self._query_cache.clear()

    def _get_next_id(self):
        current_id = self._last_id + 1
        self._last_id = current_id

        return current_id

    def _read(self):
        return self._storage.read()

    def _write(self, values):
        self._query_cache.clear()
        self._storage.write(values)

    def __len__(self):
        return len(self._read())

    def all(self):
        return  list(itervalues(self._read()))

    def __iter__(self):
        for value in itervalues(self._read()):
            yield value

    def insert(self, element):
        if not isinstance(self, Table):
            raise ValueError('Only table instance can support insert action.')

        id = self._get_next_id()

        if not isinstance(element, dict):
            raise ValueError('Element is not a dictionary')

        data = self._read()

        items = list(data.values())
        element[self._id_field] = id
        items.append(element)

        self._write(items)

        return element

    def insert_multiple(self, elements):
        if not isinstance(self, Table):
            raise ValueError('Only table instance can support insert action.')

        ids = []
        data = self._read()
        items = list(data.values())

        for element in elements:
            id = self._get_next_id()
            ids.append(id)
            element[self._id_field] = id
            items.append(element)

            # data[id] = element

        self._write(items)

        return elements

    def remove(self, cond=None, ids=None):
        return self.process_elements(lambda data, id: data.pop(id),
                                     cond, ids)

    def update(self, fields, cond=None, ids=None):
        if callable(fields):
            return self.process_elements(
                lambda data, id: fields(data[id]),
                cond, ids
            )
        else:
            return self.process_elements(
                lambda data, id: data[id].update(fields),
                cond, ids
            )

    def purge(self):
        self._write({})
        self._last_id = 0

    def search(self, cond):
        if cond in self._query_cache:
            return self._query_cache[cond][:]

        elements = [element for element in self.all() if cond(element)]
        self._query_cache[cond] = elements

        return elements[:]

    def get(self, cond=None, id=None):
        if id is not None:
            return self._read().get(id, None)

        for element in self.all():
            if cond(element):
                return element

    def count(self, cond):
        return len(self.search(cond))

    def contains(self, cond=None, ids=None):
        if ids is not None:
            return any(self.get(id=id) for id in ids)

        return self.get(cond) is not None

inkDB.table_class = Table
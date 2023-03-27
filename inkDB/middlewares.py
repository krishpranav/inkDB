from .database import inkDB


class Middleware(object):
    def __init__(self, storage_cls=inkDB.DEFAULT_STORAGE):
        self._storage_cls = storage_cls
        self.storage = None

    def __call__(self, *args, **kwargs):
        self.storage = self._storage_cls(*args, **kwargs)

        return self

    def __getattr__(self, name):
        return getattr(self.__dict__['storage'], name)


class CachingMiddleware(Middleware):
    WRITE_CACHE_SIZE = 1000

    def __init__(self, storage_cls=inkDB.DEFAULT_STORAGE):
        super(CachingMiddleware, self).__init__(storage_cls)

        self.cache = None
        self._cache_modified_count = 0

    def read(self):
        if self.cache is None:
            self.cache = self.storage.read()
        return self.cache

    def write(self, data):
        self.cache = data
        self._cache_modified_count += 1

        if self._cache_modified_count >= self.WRITE_CACHE_SIZE:
            self.flush()

    def flush(self):
        if self._cache_modified_count > 0:
            self.storage.write(self.cache)
            self._cache_modified_count = 0

    def close(self):
        self.flush()  
        self.storage.close()
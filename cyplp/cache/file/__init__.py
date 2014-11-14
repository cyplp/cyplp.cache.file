import os.path
import os
import hashlib

from dogpile.cache.api import CacheBackend, NO_VALUE
from dogpile.cache.api import CachedValue

class CacheFileBackend(CacheBackend):
    def __init__(self, arguments):
        self._root = arguments.get('root', '/tmp/cache')
        self._cache = {}

        if not os.path.isdir(self._root):
            # todo intermediaire
            os.mkdir(self._root)

    @staticmethod
    def _computeHash(key):
        return hashlib.md5(key).hexdigest()

    def get(self, key):
        cacheHash = self._computeHash(key)

        path = os.path.join(self._root, cacheHash)

        if not os.path.isfile(path):
            return NO_VALUE

        with open(path, 'rb') as tmp:
            metadata = self._cache[cacheHash]

            return CachedValue(tmp.read(), metadata)


    def set(self, key, value):
        cacheHash = self._computeHash(key)
        path = os.path.join(self._root, cacheHash)

        # import rpdb
        # rpdb.set_trace()

        with open(path, 'wb') as tmp:
            tmp.write(value.payload)

        self._cache[cacheHash] = value.metadata

    def delete(self, key):
        cacheHash = self._computeHash(key)
        path = os.path.join(self._root, cacheHash)
        os.remove(path)

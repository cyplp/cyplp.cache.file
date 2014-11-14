import os.path
import os

from dogpile.cache.api import CacheBackend, NO_VALUE


class CacheFileBackend(CacheBackend):
    def __init__(self, arguments):
        self._root = arguments['root']

        if not os.path.isdir(self._root):
            # todo intermediaire
            os.mkdir(self._root)

    def get(self, key):
        path = os.path.join(self._root, key)
        if not os.path.isfile(path):
            return NO_VALUE

        with open(path, 'rb') as tmp:
            return tmp

    def set(self, key, value):
        path = os.path.join(self._root, key)

        with open(path, 'wb') as tmp:
            tmp.write(value.read())


    def delete(self, key):
        path = os.path.join(self._root, key)
        os.remove(path)

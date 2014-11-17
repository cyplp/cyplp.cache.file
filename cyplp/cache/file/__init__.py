import os.path
import os
import hashlib
import json

import logging

from dogpile.cache.api import CacheBackend, NO_VALUE
from dogpile.cache.api import CachedValue

class CacheFileBackend(CacheBackend):
    def __init__(self, arguments):
        self._logger = logging.getLogger('cyplp_cache')
        self._logger.debug('init')

        self._root = arguments.get('root', '/tmp/cache')
        # self._cache = {}

        if not os.path.isdir(self._root):
            # todo intermediaire
            self._logger.debug('mkdir')
            os.mkdir(self._root)

    @staticmethod
    def _computeHash(key):
        return hashlib.md5(key).hexdigest()

    def get(self, key):
        cacheHash = self._computeHash(key)

        path = os.path.join(self._root, cacheHash)
        pathJson = os.path.join(self._root, cacheHash+'.json')

        if not os.path.isfile(path) or not os.path.isfile(pathJson):
            self._logger.info("miss !")
            return NO_VALUE

        self._logger.info("hit !")
        metadata = {}

        with open(pathJson, 'rb') as tmp:
            metadata = json.loads(tmp.read())

        with open(path, 'rb') as tmp:
            return CachedValue(tmp.read(), metadata)


    def set(self, key, value):
        cacheHash = self._computeHash(key)
        path = os.path.join(self._root, cacheHash)
        pathJson = os.path.join(self._root, cacheHash+'.json')
        # import rpdb
        # rpdb.set_trace()
        self._logger.info("caching ! %s in %s", key, cacheHash)

        with open(path, 'wb') as tmp:
            tmp.write(value.payload)

        with open(pathJson, 'wb') as tmp:
            tmp.write(json.dumps(value.metadata))


    def delete(self, key):

        cacheHash = self._computeHash(key)
        self._logger.info("deleting! %s in %s", key, cacheHash)
        path = os.path.join(self._root, cacheHash)
        pathJson = os.path.join(self._root, cacheHash+'.json')

        os.remove(path)
        os.remove(pathJson)

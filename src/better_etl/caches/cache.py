
class NoneCache:

    def get(self, k):
        return None

    def put(self, k, v):
        pass


class Cache:

    def __init__(self):
        self._cache = {}

    def get(self, k):
        return self._cache.get(k, None)

    def put(self, k, v):
        self._cache[k] = v

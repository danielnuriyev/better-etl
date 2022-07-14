
import json
import os.path

from better_etl.caches.cache import Cache

class PickledCache(Cache): # this is the most primitive way to persist a cache

    def __init__(self):
        super().__init__()
        self._file_name = ".cache"
        if os.path.exists(self._file_name):
            with open(self._file_name) as f:
                content = f.read()
                self._cache = json.loads(content)

    def put(self, k, v):
        super().put(k, v)
        content = json.dumps(self._cache)
        with open(self._file_name, "w") as f:
            f.write(content)

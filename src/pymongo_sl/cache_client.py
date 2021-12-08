from redis import Redis
from bson.objectid import ObjectId


class BaseCacheClient:
    def __init__(self):
        pass

    def get(self, key):
        pass

    def mget(self, *keys):
        pass

    def set(self, key, value):
        pass


class LocalCacheClient(BaseCacheClient):
    def __init__(self):
        self.mem = {}

    def get(self, key):
        if isinstance(key, ObjectId):
            key = str(key)
        return self.mem.get(key, None)

    def mget(self, *keys):
        return [self.get(x) for x in keys]

    def set(self, key, value):
        if isinstance(key, ObjectId):
            key = str(key)
        self.mem[key] = value


class CacheClient(BaseCacheClient):
    def __init__(self, **kwargs):
        self.__redis_client = None
        if "client" in kwargs:
            assert len(kwargs) == 1, "Only redis_client is needed for redis initialization"
            self.__redis_client = kwargs.pop("client")
        else:
            self.__redis_client = self._redis_init(**kwargs)

        if not isinstance(self.__redis_client, Redis):
            raise ValueError("redis_client should be instance of ~redis.Redis class")

    def _redis_init(self, **kwargs):
        return Redis(kwargs)

    def get(self, key):
        if isinstance(key, ObjectId):
            key = str(key)
        cached = self.__redis_client.get(key)
        if isinstance(cached, bytes):
            cached = cached.decode('utf-8')
        return cached

    def mget(self, *keys):
        preps = [str(x) if isinstance(x, ObjectId) else x for x in keys]
        out = self.__redis_client.mget(preps)
        return [x.decode('utf-8') if isinstance(x, bytes) else x for x in out]

    def set(self, key, value):
        if isinstance(key, ObjectId):
            key = str(key)
        self.__redis_client.set(key, value)

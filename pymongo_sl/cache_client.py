
from redis import Redis
from bson.objectid import ObjectId

class BaseCacheClient:
    def get(self, key):
        pass
    
    def set(self, key, value):
        pass

class CacheClient(BaseCacheClient):
    def __init__(self, *args, **kwargs):
        self.__redis_client: Redis = None
        
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
        # print(f"getting cache with {key = }")
        if isinstance(key, ObjectId):
            key = str(key)
        cached = self.__redis_client.get(key)
        if isinstance(cached, bytes):
            cached = cached.decode('utf-8')
        return cached
    
    def set(self, key, value):
        if isinstance(key, ObjectId):
            key = str(key)
        self.__redis_client.set(key, value)
        # print(f"setting cache with {key = }, {value = }")

from redis import Redis
from bson.objectid import ObjectId

class BaseCacheClient:
    def get(self, key):
        pass
    
    def get_string(self, key):
        pass
    
    # def set_string(self, key):
    #     pass
    
    def set(self, key, value):
        pass

class CacheClient(BaseCacheClient):
    def __init__(self, *args, **kwargs):
        self.__redis_client: Redis= kwargs.pop("redis_client", None)
        if not isinstance(self.__redis_client, Redis):
            raise ValueError("redis_client should be instance of ~redis.Redis class")

    def get_string(self, key):
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
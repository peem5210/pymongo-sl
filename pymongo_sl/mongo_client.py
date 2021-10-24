from redis import Redis
from pymongo.mongo_client import *
from pymongo_sl.cache_client import CacheClient

from pymongo_sl.database import DatabaseSL
from pymongo_sl.errors import (ConfigError)
REDIS_PREFIX = 'redis_'
class MongoClientSL(MongoClient):
    """pymongo MongoClient with region caching using Redis for SnapLogic
    This will be transparent to user and work just like the native ~pymongo.MongoClient
    
    REDIS:
        Please provide redis parameters(preferred) or pass the redis client as 'redis_client'
        For redis parameters please prefix with 'redis_'
        ex. host -> redis_host
    """
    def __init__(self, *args, **kwargs):
        redis_kwargs = dict([(kw.split(REDIS_PREFIX)[-1], kwargs.pop(kw)) for kw in kwargs.copy() if kw.startswith(REDIS_PREFIX)])
        redis_client = None        
        self.__client = MongoClient(*args, **kwargs)
        self.__dict__.update(self.__client.__dict__)
        
        if "client" in redis_kwargs:
            assert len(redis_kwargs) == 1, "Only redis_client is needed for redis initialization"
            redis_client = redis_kwargs["client"]
            if not isinstance(redis_client, Redis):
                raise ValueError("redis_client should be instance of ~redis.Redis class")
        else:
            redis_client = self._redis_init(**redis_kwargs)
        
        self.__cache_client = CacheClient(redis_client = redis_client)
        
    
    def _redis_init(self, **kwargs):
        return Redis(kwargs)
        
    def __getitem__(self, name):
        return DatabaseSL(self.__client, name, cache_client = self.__cache_client)

        
        
        


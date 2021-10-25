from pymongo.mongo_client import *

from pymongo_sl.cache_client import CacheClient
from pymongo_sl.database import DatabaseSL
from pymongo_sl.common import override

REDIS_PREFIX = 'redis_'
class MongoClientSL(MongoClient):
    """pymongo MongoClient with region caching using Redis for SnapLogic
    This will be transparent to user and work just like the native ~pymongo.MongoClient
    
    CacheClient:
        CacheClent uses redis client as a main caching service, in order to use this  
        please provide redis parameters or pass the redis client as 'redis_client'(preferred)
        For redis parameters please prefix with 'redis_'
        ex. host -> redis_host
    """
    
    @override
    def __init__(self, *args, **kwargs):
        redis_kwargs = dict([(kw.split(REDIS_PREFIX)[-1], kwargs.pop(kw)) for kw in kwargs.copy() if kw.startswith(REDIS_PREFIX)])   
        self.__cache_client = CacheClient(**redis_kwargs)
        self.__client = MongoClient(*args, **kwargs)
        self.__dict__.update(self.__client.__dict__)
    
    def get_cache_client(self):
        return self.__cache_client
    
    @override
    def __getitem__(self, name):
        return DatabaseSL(self.__client, name, cache_client = self.__cache_client)

        
        
        



class TestInit:
    def test_import(self):
        from pymongo_sl import MongoClientSL
        from pymongo_sl.cache_client import (CacheClient, LocalCacheClient, BaseCacheClient)
        from pymongo_sl.collection import CollectionSL
        from pymongo_sl.database import DatabaseSL
        assert True

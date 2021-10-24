from pymongo.collection import *
from pymongo_sl.cache_client import CacheClient
from pymongo_sl.errors import MissingArgsError
from pymongo_sl.common import *


class CollectionSL(Collection):
    """pymongo Collection for SnapLogic
     This will be transparent to user and work just like the native ~pymongo.collection.Collection
     with extended caching logic before delegating the actual query to the native class.
    """
    def __init__(self, *args, **kwargs):       
        self.__cache_client: CacheClient = kwargs.pop("cache_client", None)
        if self.__cache_client is None: 
            raise MissingArgsError("cache_client is not provided")
        self.__collection = Collection(*args, **kwargs)
        self.__dict__.update(self.__collection.__dict__)

    def __getitem__(self, name):
        return CollectionSL(self.__database,
                          _UJOIN % (self.__name, name),
                          False,
                          self.codec_options,
                          self.read_preference,
                          self.write_concern,
                          self.read_concern)
    
    def find(self, *args, **kwargs):
        "TODO: Add caching logic here"
        
        return self.__collection.find(*args, **kwargs)
    
    def find_one(self, *args, **kwargs):
        document = None
        filter = kwargs[_filter]
        if _id in filter and _region not in filter:
            region = self.__cache_client.get_string(filter[_id])
            if region:
                kwargs[_filter][_region] = region
                document = self.__collection.find_one(*args, **kwargs)
            else: #If _id is not in cache 
                document = self.__collection.find_one(*args, **kwargs)
                if document is not None:
                    if _project in kwargs:
                        "TODO: Add caching logic here"
                        pass
                    else:
                        self.__cache_client.set(document[_id], document[_region])
        else:
            document = self.__collection.find_one(*args, **kwargs)
        return document
    
    def update(self, *args, **kwargs):
        "TODO: Add caching logic here"

        return self.__collection.update(*args, **kwargs)
    
    def update_many(self, *args, **kwargs):
        "TODO: Add caching logic here"

        return self.__collection.update_many(*args, **kwargs)
        
    def update_one(self, *args, **kwargs):
        "TODO: Add caching logic here"

        return self.__collection.update_one(*args, **kwargs)
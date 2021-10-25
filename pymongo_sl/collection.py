from pymongo.collection import *

from pymongo_sl.cache_client import CacheClient
from pymongo_sl.errors import MissingArgsError
from pymongo_sl.common import override
from pymongo_sl.keywords import KW


class CollectionSL(Collection):
    """pymongo Collection for SnapLogic
     This will be transparent to user and work just like the native ~pymongo.collection.Collection
     with extended caching logic before delegating the actual query to the native class.
    """
    
    @override
    def __init__(self, *args, **kwargs):       
        self.__cache_client: CacheClient = kwargs.pop("cache_client", None)
        if self.__cache_client is None: 
            raise MissingArgsError("cache_client is not provided")
        self.__collection = Collection(*args, **kwargs)
        self.__dict__.update(self.__collection.__dict__)

    @override
    def __getitem__(self, name):
        return CollectionSL(self.__database,
                          _UJOIN % (self.__name, name),
                          False,
                          self.codec_options,
                          self.read_preference,
                          self.write_concern,
                          self.read_concern)
    @override
    def find(self, *args, **kwargs):
        "TODO: Add caching logic here"
        
        return self.__collection.find(*args, **kwargs)
    
    def _find_one_with_region(self, filter=None, projection=None, *args, **kwargs):
        document = kwargs.pop(KW.document, None)
        remove_region = False
        region = self.__cache_client.get(filter[KW._id])
        if region is not None:
            filter[KW.region] = region
            document = self.__collection.find_one(filter=filter, projection=projection, *args, **kwargs)
        else:
            if isinstance(projection, dict) and projection:
                if KW.region not in projection:
                    remove_region = True
                if next(iter(projection.values())):
                    projection[KW.region] = True
            document = self.__collection.find_one(filter=filter, projection=projection, *args, **kwargs)
            if document is not None:
                if KW._id in document and KW.region in document:
                    self.__cache_client.set(document[KW._id], document[KW.region])
                else:
                    pass
        if remove_region:
            document.pop(KW.region)
        return document
    
    @override
    def find_one(self, filter, *args, **kwargs):
        document = None        
        if KW._id in filter and KW.region not in filter and "TODO: Implement the schema validation that ensure the region field":
            kwargs[KW.document] = None
            document = self._find_one_with_region(filter, *args, **kwargs)
        else:
            document = self.__collection.find_one(filter, *args, **kwargs)
        return document
    
    @override
    def update(self, *args, **kwargs):
        "TODO: Add caching logic here"

        return self.__collection.update(*args, **kwargs)
    
    @override
    def update_many(self, *args, **kwargs):
        "TODO: Add caching logic here"

        return self.__collection.update_many(*args, **kwargs)
    
    @override  
    def update_one(self, *args, **kwargs):
        "TODO: Add caching logic here"

        return self.__collection.update_one(*args, **kwargs)
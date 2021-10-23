from pymongo.mongo_client import *

from pymongo_sl.database import DatabaseSL

class MongoClientSL(MongoClient):
    "pymongo MongoClient Snaplogic Extended"
    def __init__(self, *args, **kwargs):
        self.__client = MongoClient(*args, **kwargs)
        self.__dict__.update(self.__client.__dict__)
        
    def __getattr__(self, name):
        return self.__getitem__(name)
    
    def __getitem__(self, name):
        return DatabaseSL(self.__client, name)

        
        
        


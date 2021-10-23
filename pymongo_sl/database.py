from pymongo.database import *

from pymongo_sl.collection import CollectionSL

class DatabaseSL(Database):
    "pymongo Database Snaplogic Extended"
    def __init__(self, *args, **kwargs):
        self.__database = Database(*args, **kwargs)
        self.__dict__.update(self.__database.__dict__)
        
    def __getattr__(self, name):
        return self.__getitem__(name)
    
    def __getitem__(self, name):
        return CollectionSL(self.__database, name)
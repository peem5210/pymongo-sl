from pymongo.collection import *

class CollectionSL(Collection):
    "pymongo Collection Snaplogic Extended"
    def __init__(self, *args, **kwargs):
        
        self.__collection = Collection(*args, **kwargs)
        self.__dict__.update(self.__collection.__dict__)

    def __getattr__(self, name):
        return self.__getitem__(name)
        
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
        "TODO: Add caching logic here"

        return self.__collection.find_one(*args, **kwargs)
    
    def update(self, *args, **kwargs):
        "TODO: Add caching logic here"

        return self.__collection.update(*args, **kwargs)
    
    def update_many(self, *args, **kwargs):
        "TODO: Add caching logic here"

        return self.__collection.update_many(*args, **kwargs)
        
    def update_one(self, *args, **kwargs):
        "TODO: Add caching logic here"

        return self.__collection.update_one(*args, **kwargs)
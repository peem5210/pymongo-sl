from pymongo.database import Database

from pymongo_sl.collection import CollectionSL
from pymongo_sl.errors import MissingArgsError
from pymongo_sl.common import override


class DatabaseSL(Database):
    """pymongo Database for SnapLogic
     This will be transparent to user and work just like the native :class:`~pymongo.database.Database`
    """

    @override
    def __init__(self, *args, **kwargs):
        self.__cache_client = kwargs.pop("cache_client", None)
        if self.__cache_client is None:
            raise MissingArgsError("cache_client is not provided")
        self.__database = Database(*args, **kwargs)
        self.__dict__.update(self.__database.__dict__)

    @override
    def __getitem__(self, name):
        return CollectionSL(self.__database, name, cache_client=self.__cache_client)

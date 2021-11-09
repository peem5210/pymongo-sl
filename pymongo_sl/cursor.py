from pymongo.cursor import *
from pymongo.cursor import Cursor

from pymongo_sl.common import override
from pymongo_sl.errors import MissingArgsError
from pymongo_sl.keywords import KW


class CursorSL(Cursor):
    """pymongo Cursor for SnapLogic
     This will be transparent to user and work just like the native :class:`~pymongo.cursor.Cursor`
    """

    @override
    def __init__(self, *args, **kwargs):
        self.__cache_client = kwargs.pop("cache_client", None)
        self.__forced_projecting = kwargs.pop("forced_projecting", False)
        self.__no_cache = kwargs.pop("no_cache", False)
        if self.__cache_client is None:
            raise MissingArgsError("cache_client is not provided")
        self.__cursor = Cursor(*args, **kwargs)
        self.__dict__.update(self.__cursor.__dict__)

    @override
    def next(self):
        result = self.__cursor.next()
        if (not self.__no_cache) and self.__forced_projecting and (KW.region in result):
            result.pop(KW.region)
        return result
    __next__ = next

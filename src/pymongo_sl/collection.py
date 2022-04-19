from bson import ObjectId
from pymongo.collection import Collection, _UJOIN
from pymongo.cursor import Cursor
from pymongo_sl.cursor import CursorSL
from pymongo_sl.errors import MissingArgsError
from pymongo_sl.common import override
from pymongo_sl.keywords import KW


class CollectionSL(Collection):
    """pymongo Collection for SnapLogic
     This will be transparent to user and work just like the native :class:`~pymongo.collection.Collection`
     with extended caching logic before delegating the actual query to the native class.
    """

    @override
    def __init__(self, *args, **kwargs):
        self.__cache_client = kwargs.pop("cache_client", None)
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

    def _ensure_region(self, filter, projection, same_region=False):
        """Construct particular filter and projection that ensure the existence of field `region`
        when queried to be used to store in cache using `~pymongo_sl.cache_client.CacheClient`.

        :Parameters:
          - `filter`: Same as parameter `filter` used in :meth:`~pymongo.collection.Collection.find`.
          - `projection`: Same as parameter `filter` used in :meth:`~pymongo.collection.Collection.find`.

        :Returns:
          - `An instance of tuple structured as tuple[dict, dict, bool]`
            fist 2 dict is modified filter and projection respectively
            last is boolean indicating whether field `region` was forced
            to project or not
        """
        forced_projection = False
        if filter and KW.region not in filter and KW.id in filter:
            region = None
            id_expr = filter[KW.id]
            if isinstance(id_expr, ObjectId):
                region = self.__cache_client.get(filter[KW.id])
            elif isinstance(id_expr, dict) and same_region and isinstance(id_expr.get('$in', False), list):
                for id in id_expr['$in']:
                    region = self.__cache_client.get(id)
                    break  # For now, looking at region from head only
            if region is not None:
                filter[KW.region] = region
            else:
                if isinstance(projection, dict) and projection:
                    if KW.region not in projection and next(iter(projection.values())):
                        forced_projection = True
                        projection[KW.region] = True
        return {KW.filter: filter,
                KW.projection: projection,
                KW.forced_projection: forced_projection}

    @override
    def find(self, filter=None, projection=None, same_region=False, *args, **kwargs):
        enable_cache = kwargs.pop('enable_cache', False)
        if enable_cache:
            updated_kwargs = self._ensure_region(filter, projection, same_region=same_region)
            kwargs.update(updated_kwargs)
            return CursorSL(self, *args, cache_client=self.__cache_client, **kwargs)
        else:
            return Cursor(self, filter=filter, projection=projection, *args, **kwargs)

    def _find_one_with_region(self, filter=None, projection=None, *args, **kwargs):
        updated_kwargs = self._ensure_region(filter, projection)
        document = self.__collection.find_one(filter=updated_kwargs[KW.filter],
                                              projection=updated_kwargs[KW.projection],
                                              *args, **kwargs)
        if document is not None:
            if KW.id in document and KW.region in document:
                self.__cache_client.set(document[KW.id], document[KW.region])
            else:
                pass
        if updated_kwargs[KW.forced_projection]:
            document.pop(KW.region)
        return document

    @override
    def find_one(self, filter=None, projection=None, *args, **kwargs):
        enable_cache = kwargs.pop('enable_cache', False)
        if enable_cache and filter and KW.id in filter and KW.region not in filter \
                and """TODO: Implement the schema validation that ensure the region field
                        of the queried collection, so we won't have a miss force projection to
                        collection that doesn't have `region` field
                    """:
            document = self._find_one_with_region(filter, projection, *args, **kwargs)
        else:
            document = self.__collection.find_one(filter, projection, *args, **kwargs)
        return document

    @override
    def update(self, *args, **kwargs):
        """TODO: Add caching logic here"""

        return self.__collection.update(*args, **kwargs)

    @override
    def update_many(self, filter, update, *args, **kwargs):
        enable_cache = kwargs.pop('enable_cache', False)
        if enable_cache:
            ensured = self._ensure_region(filter, None)
            return self.__collection.update_many(ensured[KW.filter], update, *args, **kwargs)
        else:
            return self.__collection.update_many(filter, update, *args, **kwargs)

    @override
    def update_one(self, filter, update, *args, **kwargs):
        enable_cache = kwargs.pop('enable_cache', False)
        if enable_cache:
            ensured = self._ensure_region(filter, None)
            return self.__collection.update_one(ensured[KW.filter], update, *args, **kwargs)
        else:
            return self.__collection.update_one(filter, update, *args, **kwargs)

    @override
    def find_and_modify(self, query={}, update=None,
                        upsert=False, sort=None, full_response=False,
                        manipulate=False, **kwargs):
        enable_cache = kwargs.pop('enable_cache', False)
        if enable_cache:
            ensured = self._ensure_region(query, kwargs[KW.fields] if KW.fields in kwargs else None)
            if KW.fields in kwargs:
                kwargs.pop(KW.fields)
            updated = self.__collection.find_and_modify(ensured[KW.filter], update,
                                                        upsert, sort, full_response, manipulate,
                                                        fields=ensured[KW.projection],
                                                        **kwargs)
            if isinstance(updated, dict):
                if KW.id in updated and KW.region in updated:
                    self.__cache_client.set(updated[KW.id], updated[KW.region])
                if ensured[KW.forced_projection]:
                    updated.pop(KW.region)
            return updated
        else:
            return self.__collection.find_and_modify(query, update,
                                              upsert, sort, full_response, manipulate,
                                              **kwargs)

    # TODO: remove key from cache
    @override
    def remove(self, spec_or_id=None, multi=True, **kwargs):
        enable_cache = kwargs.pop('enable_cache', False)
        if enable_cache and isinstance(spec_or_id, dict) and "_id" in spec_or_id:
            if isinstance(spec_or_id["_id"], dict) and "$in" in spec_or_id["_id"] and isinstance(spec_or_id["_id"]["$in"], list):
                for region in self.__cache_client.mget(*spec_or_id["_id"]["$in"]):
                    if region is not None:
                        spec_or_id["region"] = region
                        break
            else:
                region = self.__cache_client.get(spec_or_id["_id"])
                if region is not None:
                    spec_or_id["region"] = region

        return self.__collection.remove(spec_or_id, multi, **kwargs)

    @override
    def insert_one(self, document, bypass_document_validation=False,
                   session=None, **kwargs):
        enable_cache = kwargs.pop('enable_cache', False)
        if enable_cache and isinstance(document, dict) and "_id" in document and "region" in document:
            self.__cache_client.set(document["_id"], document["region"])
        return self.__collection.insert_one(document, bypass_document_validation, session)

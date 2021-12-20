from e2e_tests.validator import validate_document, validate_args_list, args


def validate_find_and_modify(timing, document, collection_nt, collection_sl):
    validate_args_list(timing, validate_document, collection_nt.find_and_modify, collection_sl.find_and_modify, [
        args(query={"_id": document["_id"]}, fields={"_id": True, "read": True, "region": True}, update={"$set": {"hello": "world"}}),
        args(query={"_id": document["_id"]}, fields={"_id": True, "read": True, "region": True}, update={"$set": {"hello": "world"}}, enable_cache=True),
        args({"_id": document["_id"]}, {"$set": {"hello": "world"}},fields={"_id": True, "read": True, "region": True}),
        args({"_id": document["_id"]}, {"$set": {"hello": "world"}}, fields={"_id": True, "read": True}),
        args({"_id": document["_id"]}, {"$set": {"hello": "world"}}, fields={}),
        args({"_id": document["_id"]}, {"$set": {"hello": "world"}}, fields=[]),
        args(query={"_id": document["_id"]}, fields={"_id": False}, update={"$set": {"hello": "world"}}),
        args(query={"_id": document["_id"]}, fields={"_id": True, "read": True, "region": True},update={"$set": {"hello": "world"}}),
        args(query={"_id": document["_id"]}, fields={"_id": True, "read": True}, update={"$set": {"hello": "world"}}),
        args(query={"_id": document["_id"]}, fields={}, update={"$set": {"hello": "world"}}),
        args(query={"_id": document["_id"]}, fields=[], update={"$set": {"hello": "world"}}),
        args(query={"_id": document["_id"]}, fields={"_id": False}, update={"$set": {"hello": "world"}}),
    ])

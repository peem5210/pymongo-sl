from e2e_tests.validator import validate_result


def validate_update_many(timing, _, collection_nt, collection_sl):
    undo_func = collection_nt.update_one
    undo_kwargs = {"filter": {"group": 2}, "update": {'$set': {"read": False}}}
    validate_result(timing, collection_nt.update_many, collection_sl.update_many, filter={"group": 2}, update={'$set': {"read": True}}, undo_func=undo_func, undo_kwargs=undo_kwargs)

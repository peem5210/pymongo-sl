from e2e.validator import validate_result


def validate_update_many(report, _, collection_nt, collection_sl):
    undo_func = collection_nt.update_one
    undo_kwargs = {"filter": {"group": 2}, "update": {'$set': {"read": False}}}
    validate_result(report, collection_nt.update_many, collection_sl.update_many, filter={"group": 2}, update={'$set': {"read": True}}, undo_func=undo_func, undo_kwargs=undo_kwargs)
    validate_result(report, collection_nt.update_many, collection_sl.update_many, {"group": 2}, update={'$set': {"read": True}}, undo_func=undo_func, undo_kwargs=undo_kwargs)
    validate_result(report, collection_nt.update_many, collection_sl.update_many, {"group": 2}, {'$set': {"read": True}}, undo_func=undo_func, undo_kwargs=undo_kwargs)

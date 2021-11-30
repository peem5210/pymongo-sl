from e2e_tests.validator import validate_result


def validate_update_many(timing, _, collection_nt, collection_sl):
    validate_result(timing, collection_nt.update_many, collection_sl.update_many, filter={"group": 2},update={'$set': {"read": True}})

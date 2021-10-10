import random
import bson
import json

from rcache.rcache import Rcache

REGIONS = ['ASIA', 'EURO', 'OREG']
NUMBER_RANDOMIZED_OBJ = 10_000_000

def random_object_id():
    for _ in range(NUMBER_RANDOMIZED_OBJ):
        yield Rcache(
        _id = bson.objectid.ObjectId(), 
        region = REGIONS[random.randint(0,len(REGIONS)-1)]
        ).to_dict()

with open("../random_object_id.txt", "w") as f:
    for i in random_object_id():
        f.write(json.dumps(i))
        f.write('\n')


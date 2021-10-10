from dataclasses import dataclass
from typing import Any

from bson import ObjectId
@dataclass(frozen=True)
class Rcache():
    region: str
    _id: ObjectId = ObjectId()
    
    def to_dict(self):
        return {"_id:":str(self._id), "region":self.region}
    
    def __post_init__(self):
        assert type(self._id)==ObjectId, f"_id should be ObjectId: {type(self._id)}"
        assert type(self.region)==str, f"region should be string: {type(self.region)}"
from FileOrg import FileOrg
from typing import List


class StaticHash(FileOrg):

    def __init__(self, relation_name, bucket_range):
        super().__init__(relation_name)
        self.bucket_range = bucket_range
        pass

    def __hashing(self, key: int) -> hash:

    def insert(self, records: List[dict]) -> None:
        for record in records:
            address = __hashing(record.key)
            if(address not in self.bucket):
        data = self.bucket[address]
        wanted_data = {}
        if(type(data) == List):
            for e in range(len(data)):
                if(data[e].id == key):
                    wanted_data = data[e]
        else:
            wanted_data = data
        return wanted_data

    def select_many(self, list_of_keys: List[int]) -> List[dict]:
        selected = []
        for key in list_of_keys:
            selected.append[self.select_one(key)]
        return selected

    def select_range(self, starting_key: int, ending_key: int):
        keys_to_retrieve = [x for x in range(starting_key+1, ending_key)]
        retrieved_keys = select_many(keys_to_retrieve)
        return retrieved_keys

    def delete(self, key: int) -> None:
        # del select_one(key)
        """ address = __hashing(key);
        data = self.bucket[address]
        if (type(data) == List):
            for i in range(len(data)):
                if(data[i].id == key):
                    del data[i]
        else:
            del data;
     """

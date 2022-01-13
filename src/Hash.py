from FileOrg import FileOrg
from typing import List
from Block import Block


class Hash(FileOrg):

    def __init__(self, relation_name, schema_header, bucket_size):
        super().__init__(relation_name, schema_header)
        self.bucket_size = bucket_size

    def __hashing(self, key: int) -> hash:
        return f"{int(key/self.bucket_size)}|{key % self.bucket_size}"

    def insert(self, records: List[dict]) -> None:
        for record in records:
            bucket, offset = self.__hashing(record.key).split('|')
            if len(self.empty_list) > 0:
                ix, offset = self.empty_list[-1]
                self.write_record(ix, offset, record)
                self.empty_list.pop()
            else:
                new_block = Block()
                new_block.append(record)
                self.append_block(new_block)
                self.empty_list.append((self.block_count, record.size))
            wanted_data = {}
            if(type(data) == List):
                for e in range(len(data)):
                    if(data[e].id == record.key):
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
        retrieved_keys = self.select_many(keys_to_retrieve)
        return retrieved_keys

    def delete(self, key: int) -> None:
        pass

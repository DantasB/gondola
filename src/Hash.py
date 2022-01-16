from FileOrg import FileOrg
from typing import List


class Hash(FileOrg):

    def __init__(self, relation_name, schema_header, bucket_size):
        super().__init__(relation_name, schema_header)
        self.bucket_size = bucket_size

    def hashing(self, key: int) -> hash:
        return f"{int(key/self.bucket_size)}|{key % self.bucket_size}"

    def go_to_location(self, bucket, offset):
        self.data_file.seek(int(bucket)*self.bucket_size + offset + self.HEADER_SIZE)

    def insert(self, record) -> None:
        bucket, offset = self.hashing(record.id).split('|')
        breakpoint()
        offset = int(offset)
        self.go_to_location(bucket, offset)
        self.data_file.write(record.to_string() + '\n')

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

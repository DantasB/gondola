from FileOrg import FileOrg
from Schema import Schema
from Block import Block


class Heap(FileOrg):
    def __init__(self, file_path, metadata_path):
        super().__init__(file_path, metadata_path)

    def insert(self, record):
        if len(self.empty_list) > 0:
            ix, offset = self.empty_list[0]
            block = super().read_block(ix)
            block[offset] = record
        else:
            new_block = Block()
            new_block.append(record)
            super().append_block()

    def select(self, filter):
        r = []
        ix = 0
        while True:
            block_len, block = super().read_block(ix)
            if block_len <= 0:
                break
            for record in block.records:
                if filter(record):
                    r.append(record)
            ix += 1
        return r

    def delete(self):
        pass

    def reorganize(self):
        pass

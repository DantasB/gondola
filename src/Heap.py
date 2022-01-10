from FileOrg import FileOrg
from Schema import Schema
from Block import Block


class Heap(FileOrg):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.empty_list = self.metadata_file.readline()

    def insert(self, record):
        if len(self.empty_list) > 0:
            ix, offset = self.empty_list[0]
            block = super().read_block(ix)
            block[offset] = record
            super().write_block(block, ix)
            self.empty_list.pop()
        else:
            new_block = Block()
            new_block.records.append(record)
            last_ix = super().append_block(new_block)
            for i in range(1, RECORDS_IN_A_BLOCK):
                self.empty_list.append((last_ix, i))

    def select(self, filter):
        r = []
        ix = 0
        while True:
            block = super().read_block(ix)
            if len(block) <= 0:
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

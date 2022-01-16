from hashlib import new
from FileOrg import FileOrg
from Schema import Schema
from Block import Block

# TODO empty_list precisa ser um list, e não string


class Heap(FileOrg):
    def __init__(self, relation_name, schema_header=None):
        super().__init__(relation_name, schema_header)

    def insert(self, record):
        if len(self.empty_list) > 0:
            ix, offset = self.empty_list.pop()
            block = self.read_block(ix)
            new_empty = block.write(offset, record)
            self.write_block(block, ix)
            if new_empty != None:
                self.empty_list.append((ix, new_empty[0]))
        else:
            new_block = Block()
            new_block.write(0, record)
            self.append_block(new_block)
            self.empty_list.append((self.block_count-1, record.size))
        self.record_count += 1

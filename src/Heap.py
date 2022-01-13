from FileOrg import FileOrg
from Schema import Schema
from Block import Block

# TODO empty_list precisa ser um list, e não string


class Heap(FileOrg):
    def __init__(self, relation_name, schema_header, one_file=False):
        super().__init__(relation_name, schema_header)
        if not one_file:
            self.empty_list = self.metadata_file.readline().split(',')
            self.block_count = int(self.metadata_file.readline())
            self.record_count = int(self.metadata_file.readline())

    def insert(self, record):
        if len(self.empty_list) > 0:
            ix, offset = self.empty_list[-1]
            block = self.read_block(ix)
            block.write(offset, record)
            self.write_block(block, ix)
            self.empty_list.pop()
        else:
            new_block = Block()
            new_block.records.append(record)
            self.append_block(new_block)
            self.empty_list.append((self.block_count, record.size))
        self.record_count += 1

    def persist(self):
        lines = []
        lines.append(self.empty_list + "\n")
        lines.append(self.block_count + "\n")
        lines.append(self.record_count + '\n')
        self.metadata_file.seek(0)
        self.metadata_file.writelines(lines)

    def reorganize(self):
        pass

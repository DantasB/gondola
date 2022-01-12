from FileOrg import FileOrg
from Schema import Schema
from Block import Block

# TODO empty_list precisa ser um list, e não string


class Heap(FileOrg):
    def __init__(self, relation_name, one_file=False):
        super().__init__(relation_name)
        if not one_file:
            self.empty_list = self.metadata_file.readline()
            self.block_count = self.metadata_file.readline()
            self.record_count = self.metadata_file.readline()

    def insert(self, record):
        if len(self.empty_list) > 0:
            ix, offset = self.empty_list[-1]
            block = self.readBlock(ix)
            block.write(offset, record)
            self.writeBlock(block, ix)
            self.empty_list.pop()
        else:
            new_block = Block()
            new_block.append(record)
            self.append_block(new_block)
            for i in range(1, RECORDS_IN_A_BLOCK):
                self.empty_list.append((self.block_count, i))
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

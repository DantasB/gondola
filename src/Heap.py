from FileOrg import FileOrg
from Schema import Schema
from Block import Block


class Heap(FileOrg):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.empty_list = self.metadata_file.readline()
        self.block_count = self.metadata_file.readline()

    def insert(self, record):
        if len(self.empty_list) > 0:
            ix, offset = self.empty_list[-1]
            block = super().read_block(ix)
            block.write(offset, record)
            super().write_block(block, ix)
            self.empty_list.pop()
        else:
            new_block = Block()
            new_block.append(record)
            last_ix = super().append_block(new_block)
            self.block_count += 1
            for i in range(1, RECORDS_IN_A_BLOCK):
                self.empty_list.append((last_ix, i))

    def updateEmptyList(self):
        lines = self.metadata_file.readlines()
        lines[0] = "".join(self.empty_list) + "\n"
        self.metadata_file.seek(0)
        self.metadata_file.writelines(lines)

    def updateBlockCount(self):
        lines = self.metadata_file.readlines()
        lines[1] = "".join(self.block_count) + "\n"
        self.metadata_file.seek(0)
        self.metadata_file.writelines(lines)

    def persist(self):
        self.updateEmptyList()
        self.updateBlockCount()

    def reorganize(self):
        pass

from FileOrg import FileOrg
from Block import Block
# Variable attribute length Heap


class VLHeap(FileOrg):
    def __init__(self, relation_name, schema_header):
        super().__init__(relation_name, schema_header)
        self.empty_list = self.metadata_file.readline()
        self.block_count = self.metadata_file.readline()
        self.record_count = self.metadata_file.readline()

    def __findEmpty(self, size):
        for i, (ix, offset, r_size) in enumerate(self.empty_list):
            if r_size >= size:
                del self.empty_list[i]
                return (ix, offset)
        raise Exception('[ERROR] Cant find fitting empty slot')

    def insert(self, record):
        try:
            ix, offset = self.__findEmpty(record.size)
            self.writeRecord(ix, offset, record)
        except:
            new_block = Block()
            new_block.append(record)
            self.append_block(new_block)
            for i in range(1, RECORDS_IN_A_BLOCK):
                self.empty_list.append((self.block_count, i, record.size))
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

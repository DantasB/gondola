from FileOrg import FileOrg
from Block import Block
# Variable attribute length Heap


class VLHeap(FileOrg):
    def __init__(self, relation_name):
        super().__init__(relation_name)

    def __findEmpty(self, size):
        for i, (ix, offset, r_size) in enumerate(self.empty_list):
            if r_size >= size:
                del self.empty_list[i]
                return (ix, offset)
        raise Exception('[ERROR] Cant find fitting empty slot')

    def insert(self, record):
        try:
            ix, offset = self.__findEmpty(record.size)
            offset, size = self.write_record(ix, offset, record)
            self.empty_list.append(ix, offset, size)
        except:
            new_block = Block()
            new_block.append(record)
            self.append_block(new_block)
            self.empty_list.append(
                (self.block_count, record.size, self.BLOCK_SIZE - record.size)
            )
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

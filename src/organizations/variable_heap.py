from src.structures.file_org import FileOrg
from src.structures.block import Block

# Variable attribute length Heap


class VLHeap(FileOrg):
    def __init__(self, relation_name, schema_header):
        super().__init__(relation_name, schema_header)

    def __findEmpty(self, size):
        for i, (ix, offset, r_size) in enumerate(self.empty_list):
            if r_size >= size:
                del self.empty_list[i]
                return (ix, offset)

        raise Exception("[WARN] Cant find fitting empty slot")

    def insert(self, record):
        try:
            ix, offset = self.__findEmpty(record.size)
            new_empty = self.write_record(ix, offset, record)
            if new_empty:
                offset, size = new_empty
                self.empty_list.append((ix, offset, size))
        except Exception as e:
            print(e)
            new_block = Block()
            new_block.write(0, record)
            self.append_block(new_block)
            self.empty_list.append(
                (self.block_count - 1, record.size, self.BLOCK_SIZE - record.size)
            )
        self.record_count += 1

    def reorganize(self):
        pass

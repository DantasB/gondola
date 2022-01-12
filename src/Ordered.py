from FileOrg import FileOrg
from Block import Block


class Ordered(FileOrg):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.empty_list = self.metadata_file.readline()
        self.block_count = self.metadata_file.readline()
        self.record_count = self.metadata_file.readline()
        self.extension_block_count = self.metadata_file.readline()

        extension_path = f"./{relation_name}_ext.cbd"
        self.extension_file = open(self.extension_path, "r+")

        self.need_reorganize = False

    def searchIndex(self, index):
        if self.need_reorganize:
            self.reorganize()
        record_block_ix = int(index/RECORDS_IN_A_BLOCK)
        record_block_offset = index % RECORDS_IN_A_BLOCK
        block = super().read_block(record_block_ix)
        return block[record_block_offset]

    def binary_search(self, id, start, end):
        if end - start < 1:
            for i in range(start, end+1):

        p = int((end - start)/2 + start)
        p_record = self.searchIndex(p)
        p_id = p_record.id
        if id == p_id:
            return p_record
        elif id < p_id:
            return self.binary_search(id, start, p-1)
        else:
            return self.binary_search(id, p+1, end)

    def select_range(self, first_id, last_id):
        r = []
        first_ix = int(first_id/RECORDS_IN_A_BLOCK)
        last_ix = int(last_id/RECORDS_IN_A_BLOCK)

        for ix in range(first_ix, last_ix + 1):
            block = super().read_block(ix)
            for record in block.records:
                if record.id >= first_id and record.id <= last_id:
                    r.append(record)

    def select_list(self, filter_list):
        pass

    def insert(self, record):
        last_block = super.read_block(
            self.extension_file[self.extension_block_count-1])
        last_register_index = last_block[-1]


pass


def reorganize(self):
    # retirar todos os records marcados como deletados
    # fazer um merge sort com os registros que tenham sobrado
    pass

from FileOrg import FileOrg
from Block import Block
from Heap import Heap


class Ordered(FileOrg):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.empty_list = self.metadata_file.readline()
        self.block_count = self.metadata_file.readline()
        self.record_count = self.metadata_file.readline()
        self.heap = Heap(relation_name + '_extension', one_file=True)

        self.need_reorganize = False

    def searchIndex(self, index):
        if self.need_reorganize:
            self.reorganize()
        record_block_ix = int(index/RECORDS_IN_A_BLOCK)
        record_block_offset = index % RECORDS_IN_A_BLOCK
        block = super().readBlock(record_block_ix)
        return block[record_block_offset]

    def binarySearch(self, id, start=0, end=None):
        if end == None:
            end = self.record_count-1
        if end - start < 1:
            for i in range(start, end+1):
                p_record = self.searchIndex(i)
                p_id = p_record.id
                if id == p_record.id:
                    return p_record
            raise Exception('[ERROR] Couldnt find record')

        p = int((end - start)/2 + start)
        p_record = self.searchIndex(p)
        p_id = p_record.id
        if id == p_id:
            return p_record
        elif id < p_id:
            return self.binarySearch(id, start, p-1)
        else:
            return self.binarySearch(id, p+1, end)

    def selectRange(self, first_id, last_id):
        if self.need_reorganize:
            self.reorganize()
        r = []
        if first_id > self.record_count-1:
            first_id = self.record_count
        if last_id > self.record_count-1:
            last_id = self.record_count
        first = self.binarySearch(first_id)
        last = self.binarySearch(last_id)

        for ix in range(first.ix, last.ix + 1):
            block = super().readBlock(ix)
            for record in block.records:
                if record.id >= first_id and record.id <= last_id:
                    r.append(record)

    def selectId(self, id):
        if self.need_reorganize:
            self.reorganize()
        return self.binarySearch(id)

    def selectList(self, filter_list):
        r = []
        for l in filter_list:
            try:
                record = self.selectId(l)
                r.append(record)
            except:
                pass
        return r

    def insert(self, record):
        # # # O WRITE NESSE CASO DEVE SER FEITO NO ARQUIVO DE EXTENSÃO!!!
        self.heap.insert(record)
        self.record_count += 1


def reorganize(self):
    # for i in range(1, self.extension_block_count-1, BLOCK_SIZE):
    # ler as entradas no arquivo de extensão
    # retirar todos os records marcados como deletados
    # fazer um merge sort com os registros que tenham sobrado
    pass

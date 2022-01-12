from FileOrg import FileOrg
from Block import Block
from Heap import Heap


class Ordered(FileOrg):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.empty_list = self.metadata_file.readline()
        self.block_count = self.metadata_file.readline()
        self.record_count = self.metadata_file.readline()
        self.heap = Heap(relation_name + ' extension', one_file=True)

        self.need_reorganize = False

    def searchIndex(self, index):
        if self.need_reorganize:
            self.reorganize()
        record_block_ix = int(index/RECORDS_IN_A_BLOCK)
        record_block_offset = index % RECORDS_IN_A_BLOCK
        block = super().read_block(record_block_ix)
        return block[record_block_offset]

    def binarySearch(self, id, start, end):
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
        first = self.binarySearch(first_id, 0, self.record_count-1)
        last = self.binarySearch(last_id, 0, self.record_count-1)

        for ix in range(first.ix, last.ix + 1):
            block = super().read_block(ix)
            for record in block.records:
                if record.id >= first_id and record.id <= last_id:
                    r.append(record)

    def selectSingle(self, id):
        if self.need_reorganize:
            self.reorganize()
        return self.binarySearch(id, 0, self.record_count-1)

    def selectList(self, filter_list):
        r = []
        for l in filter_list:
            try:
                record = self.selectSingle(l)
                r.append(record)
            except:
                pass
        return r

    def insert(self, record):
        # # # O WRITE NESSE CASO DEVE SER FEITO NO ARQUIVO DE EXTENSÃO!!!

        # se o bloco ainda tiver espaço
        if((offset + REGISTER_SIZE) < BLOCK_SIZE):
            block = super().read_block(ix)
            block.append(record)
            super().write_block(block, ix)
        else:
            new_block = Block()
            new_block.append(record)
            super().write_block(new_block, ix)
            self.extension_block_count += 1

            # ler último bloco registrado no arquivo de extensão -> last_block
            # ver se ele tem espaço (como o tamanho do registro é fixo, é mais fácil de ver isso)
            # caso tenha, alocar nele, caso não, num novo


def reorganize(self):
    # for i in range(1, self.extension_block_count-1, BLOCK_SIZE):
    # ler as entradas no arquivo de extensão
    # retirar todos os records marcados como deletados
    # fazer um merge sort com os registros que tenham sobrado
    pass

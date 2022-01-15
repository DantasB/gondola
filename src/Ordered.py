from FileOrg import FileOrg
from Block import Block
from Heap import Heap
from copy import deepcopy


class Ordered(FileOrg):
    def __init__(self, relation_name, schema_header):
        super().__init__(relation_name, schema_header)
        self.heap = Heap(relation_name + '_extension', schema_header)

        self.need_reorganize = False

    def search_index(self, index):
        if self.need_reorganize:
            self.reorganize()
        block_ix = int(index/self.RECORDS_IN_A_BLOCK)
        block_offset = (index % self.RECORDS_IN_A_BLOCK) * self.RECORD_SIZE
        block = super().read_block(block_ix)
        return block_ix, block.read(block_offset)

    def __binary_search(self, id, start=0, end=None):
        if end == None:
            end = self.record_count-1
        if end - start < 1:
            for i in range(start, end+1):
                ix, p_record = self.search_index(i)
                p_id = p_record.id
                if id == p_record.id:
                    return ix, p_record
            raise Exception('[ERROR] Couldnt find record')

        p = int((end - start)/2 + start)
        p_record = self.search_index(p)
        p_id = p_record.id
        if id == p_id:
            return p_record
        elif id < p_id:
            return self.__binary_search(id, start, p-1)
        else:
            return self.__binary_search(id, p+1, end)

    def select_range(self, first_id, last_id):
        if self.need_reorganize:
            self.reorganize()
        r = []
        if first_id > self.record_count-1:
            first_id = self.record_count
        if last_id > self.record_count-1:
            last_id = self.record_count
        first_ix, _ = self.__binary_search(first_id)
        last_ix, _ = self.__binary_search(last_id)

        for ix in range(first_ix, last_ix + 1):
            block = super().read_block(ix)
            for record in block.records:
                if record.id >= first_id and record.id <= last_id:
                    r.append(record)

    def select_id(self, id):
        if self.need_reorganize:
            self.reorganize()
        return self.__binary_search(id)[1]

    def select_list(self, filter_list):
        r = []
        for l in filter_list:
            try:
                record = self.select_id(l)
                r.append(record)
            except:
                pass
        return r

    def insert(self, record):
        # adições serão feitas de maneira indiscriminada (não serão reorganizadas no momento chave)
        self.heap.insert(record)

    def __merge_sort(self, records, order_field):
        if(len(records) == 1):
            return records[0]
        else:
            print('foi')

    def reorganize(self):
        new_records = []
        # ler as entradas no arquivo de extensão
        new_records = [new_rec for new_rec in self.heap.data_file.readlines()]
        # apagar tudo desse arquivo após ter recuperado a informação
        self.heap.reset()

        # pega os registros atuais (no arquivo original), e filtra pelos que não estão marcados como deletados)
        records = self.select(lambda record: not record.isEmpty)

        # id é apenas um exemplo de possibilidade de campo pelo qual ordenar
        sorted_records = self.__merge_sort(
            new_records + filtered_excluded_records, "id")

        # retirar todos os records marcados como deletados
        # fazer um merge sort com os registros que tenham sobrado
        pass

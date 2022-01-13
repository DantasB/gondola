from FileOrg import FileOrg
from Block import Block
from Heap import Heap
from copy import deepcopy


class Ordered(FileOrg):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.empty_list = self.metadata_file.readline()
        self.block_count = self.metadata_file.readline()
        self.record_count = self.metadata_file.readline()
        self.heap = Heap(relation_name + '_extension', one_file=True)

        self.need_reorganize = False

    def search_index(self, index):
        if self.need_reorganize:
            self.reorganize()
        block_ix = int(index/RECORDS_IN_A_BLOCK)
        block_offset = (index % RECORDS_IN_A_BLOCK) * RECORD_SIZE
        block = super().read_block(block_ix)
        return block.read(block_offset)

    def __binary_search(self, id, start=0, end=None):
        if end == None:
            end = self.record_count-1
        if end - start < 1:
            for i in range(start, end+1):
                p_record = self.search_index(i)
                p_id = p_record.id
                if id == p_record.id:
                    return p_record
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
        first = self.__binary_search(first_id)
        last = self.__binary_search(last_id)

        for ix in range(first.ix, last.ix + 1):
            block = super().read_block(ix)
            for record in block.records:
                if record.id >= first_id and record.id <= last_id:
                    r.append(record)

    def select_id(self, id):
        if self.need_reorganize:
            self.reorganize()
        return self.__binary_search(id)

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
        self.heap.record_count += 1

    def __merge_sort(self, records, order_field):
        if(len(records) == 1):
            return records[0]
        else:

        pass

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

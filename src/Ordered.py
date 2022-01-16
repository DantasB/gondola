from FileOrg import FileOrg
from Heap import Heap
from Record import Record


class Ordered(FileOrg):
    def __init__(self, relation_name, schema_header):
        super().__init__(relation_name, schema_header)
        self.heap = Heap(relation_name + '_extension', schema_header)
        if not self.already_existed:
            f = open(self.metadata_path, 'a')
            f.write('\n')
            f.close()
        f = open(self.metadata_path, 'r')
        f_lines = f.readlines()
        id_list = f_lines[-1].strip('\n').split('|')
        if id_list[0] != '':
            self.id_list = [int(id) for id in id_list]
        else:
            self.id_list = []
        f.close()

    def delete(self, id):
        self.need_reorganize = True
        if filter:
            super().delete(lambda r: r.id == id)
        else:
            self.id_list.remove(id)
        self.reorganize()

    def __search_index(self, index):
        if self.need_reorganize:
            self.reorganize()
        block_ix = int(index/self.RECORDS_IN_A_BLOCK)
        block_offset = (index % self.RECORDS_IN_A_BLOCK) * self.RECORD_SIZE
        return block_ix, block_offset

    def __binary_search(self, id, start=0, end=None):
        # https://www.geeksforgeeks.org/python-program-for-binary-search/
        if end == None:
            end = len(self.id_list) - 1

        # Check base case
        if end >= start:

            mid = (end + start) // 2

            # If element is present at the middle itself
            if self.id_list[mid] == id:
                return self.__search_index(mid)

            # If element is smaller than mid, then it can only
            # be present in left subarray
            elif self.id_list[mid] > id:
                return self.__binary_search(id, start, mid - 1)

            # Else the element can only be present in right subarray
            else:
                return self.__binary_search(id, mid + 1, end)

        else:
            # Element is not present in the array
            raise Exception('[ERROR] Couldnt find ID')

    def select_range(self, first_id, last_id):
        # como fazer??
        pass

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
        self.need_reorganize = True
        # adições serão feitas de maneira indiscriminada (não serão reorganizadas no momento chave)
        self.heap.insert(record)
        self.id_list.append(record.id)
        self.id_list.sort()

    def persist(self):
        self.heap.persist()
        metadata_file = open(self.metadata_path, 'w')
        lines = []
        lines.append(self.empty_list_to_str())
        lines.append(str(self.block_count) + "\n")
        lines.append(str(self.record_count) + '\n')
        lines.append(str(self.need_reorganize) + '\n')
        metadata_file.seek(0)
        metadata_file.writelines(lines)
        metadata_file.close()
        f = open(self.metadata_path, 'a')
        stringfied = [str(id) for id in self.id_list]
        f.write('|'.join(stringfied) + '\n')

    def __merge_sort(self, records, start=0, ending=None):
        if ending == None:
            ending = len(records)
        if(ending - start > 1):
            mid = (ending+start)//2

            self.__merge_sort(records, start, mid)
            self.__merge_sort(records, mid, ending)
            self.__merge(records, start, mid, ending)
        return records

    def __merge(self, records, start, mid, ending):
        left = records[start:mid]
        right = records[mid:ending]
        i, j = 0, 0
        for k in range(start, ending):
            if(i >= len(left)):
                records[k] = right[j]
                j += 1
            elif(j >= len(right)):
                records[k] = left[i]
                i += 1
            elif(left[i].id < right[j].id):
                records[k] = left[i]
                i += 1
            else:
                records[k] = right[j]
                j += 1

    def reorganize(self):
        self.need_reorganize = False
        # ler as entradas no arquivo de extensão
        new_records = self.heap.select(lambda r: True)
        # apagar tudo desse arquivo após ter recuperado a informação
        self.heap.reset()
        # pega os registros atuais (no arquivo original)
        main_records = self.select(lambda r: True)
        self.data_clear()
        # id é apenas um exemplo de possibilidade de campo pelo qual ordenar
        sorted_records = self.__merge_sort(
            main_records + new_records)
        for record in sorted_records:
            self.data_file.write(record.to_string()+'\n')

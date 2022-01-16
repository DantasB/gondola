from src.structures.record import Record
from src.base import Loader


class Block(Loader):
    def __init__(self, buffer=""):
        self.records = []
        offset = 0
        size = 0
        for record in buffer.split("\n")[:-1]:
            new_rec = Record(record, offset)
            self.__append(new_rec)
            size += new_rec.size
            offset += size
        if size < self.BLOCK_SIZE:
            self.__append(Record(offset=offset, size=self.BLOCK_SIZE - size))

    def __offset_to_ix(self, offset):
        """
        Retorna o indice do registro na lista de acordo como o offset no bloco
        Retorna len(self.records) se offset for igual ao ultimo byte do bloco
        """
        sum = 0
        for i, record in enumerate(self.records):
            sum += record.size
            if sum - 1 >= offset:
                return i
        raise Exception("[ERROR] Invalid Offset")

    def clear(self, offset):
        """
        Substitui o record no offset por um outro vazio de mesmo tamanho
        """
        ix = self.__offset_to_ix(offset)
        old = self.records[ix]
        self.records[ix] = Record(offset=offset, size=old.size)

    def to_string(self):
        return "\n".join([record.to_string() for record in self.records]) + "\n"

    def write(self, offset, record):
        """
        Chama update para substituir record no offset por outro
        """
        record.offset = offset
        return self.update(offset, record)

    def read(self, offset):
        """
        lê o record no offset
        """
        ix = self.__offset_to_ix(offset)
        return self.records[ix]

    def __append(self, record):
        self.records.append(record)

    def insert(self, record):
        for r in self.records:
            if r.is_empty and r.size >= record.size:
                self.write(r.offset, record)
                return
        raise Exception('Cant fit inside block')

    def update(self, offset, record):
        """ "
        Substitui um record no offset por outro
        Se o novo record for menor que o anterior, cria um record vazio
        com o tamanho do buraco que ficou no arquivo
        Retorna o offset e o tamanho do buraco
        """
        ix = self.__offset_to_ix(offset)
        old = self.records[ix]
        self.records[ix] = record
        if old.size > record.size:
            self.records.insert(
                ix + 1,
                Record(offset=old.offset + record.size, size=old.size - record.size),
            )
            return old.offset + record.size, old.size - record.size
        elif old.size < record.size:
            raise Exception("[ERROR] Cant exceed block size")

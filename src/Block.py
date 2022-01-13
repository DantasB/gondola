from Record import Record


class Block:
    def __init__(self, buffer=""):
        self.records = [Record(content=record, size=len(record))
                        for record in buffer.split("\n")[:-1]]

    def __offset_to_ix(self, offset):
        """
        Retorna o indice do registro na lista de acordo como o offset no bloco
        Retorna len(self.records) se offset for igual ao ultimo byte do bloco
        """
        sum = 0
        for i, record in enumerate(self.records):
            sum += record.size
            if sum - 1 >= offset:
                return i-1
        if sum == offset:
            return i + 1
        raise Exception('[ERROR] Invalid Offset')

    def clear(self, offset):
        """
            Substitui o record no offset por um outro vazio de mesmo tamanho
        """
        ix = self.__offset_to_ix(offset)
        old = self.records[ix]
        self.records[ix] = Record(size=old.size)

    def to_string(self):
        return '\n'.join([record.to_string() for record in self.records]) + '\n'

    def write(self, offset, record):
        """
            Chama update para substituir record no offset por outro
        """
        ix = self.__offset_to_ix(offset)
        if len(self.record) == ix:
            # se offset for igual ao ultimo byte tentamos append
            self.__append(record)
        else:
            return self.update(offset, record)
        return 'No new empty'

    def read(self, offset):
        """
        lê o record no offset
        """
        ix = self.__offset_to_ix(offset)
        return self.records[ix]

    def __append(self, record):
        if self.size + record.size > BLOCK_SIZE:
            raise Exception('[ERROR] Block size exceeded')
        self.records.append(record)

    def update(self, offset, record):
        """"
        Substitui um record no offset por outro
        Se o novo record for menor que o anterior, cria um record vazio
        com o tamanho do buraco que ficou no arquivo
        Retorna o offset e o tamanho do buraco
        """
        ix = self.__offset_to_ix(offset)
        old = self.records[ix]
        self.records[ix] = record
        if old.size > record.size:
            if len(self.records) == ix - 1:
                self.__append(
                    Record(size=old.size-record.size))
            self.records[ix + 1] = Record(size=old.size-record.size)
            return offset + old.size, old.size-record.size
        return 'No new empty'

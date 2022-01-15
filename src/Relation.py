from Heap import Heap
from Hash import Hash
from Ordered import Ordered
from Schema import Schema
from Record import Record
from VLHeap import VLHeap

fileOrgs = {"VLHeap": VLHeap, "Heap": Heap, "Hash": Hash, "Ordered": Ordered}


class Relation:
    def __init__(self, fileOrg: str, name: str, column_defs: dict):
        """
        Recebe como argumento um fileOrg (str) e, as defs das colunas da Relation e seu nome
        Exemplo:
            Relation('Heap', 'Alunos', { "id":
                                          {
                                            "type": "int",
                                            "size": 10 }
                                          },
                                          "nome":
                                          {
                                              "type": "varchar",
                                              "size": 50
                                          } )
        """
        self.schema = Schema(relation_name=name, column_defs=column_defs)
        self.fileOrg = fileOrgs[fileOrg](
            relation_name=name, schema_header=self.schema.metadata())

    def insert(self, data):
        self.fileOrg.insert(Record(data))

    def select(self, filter=["all", "in", "between"]):
        return self.fileOrg.select(filter)

    def delete(self, filter=["where", "all"]):
        self.fileOrg.delete(filter)

    def reorganize(self):
        self.fileOrg.reorganize()

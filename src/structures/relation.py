from src.organizations.heap import Heap
from src.organizations.hash import Hash
from src.organizations.ordered_archive import Ordered
from src.structures.schema import Schema
from src.structures.record import Record
from src.organizations.variable_heap import VLHeap

fileOrgs = {"VLHeap": VLHeap, "Heap": Heap, "Hash": Hash, "Ordered": Ordered}


class Relation:
    def __init__(self, fileOrg: str, name: str, column_defs: dict, **kwargs):
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
            relation_name=name, schema_header=self.schema.metadata(), **kwargs
        )

    def insert(self, data):
        self.fileOrg.insert(Record(data))

    def select(self, filter=["all", "in", "between"]):
        return self.fileOrg.select(filter)

    def delete(self, filter=["where", "all"]):
        self.fileOrg.delete(filter)

    def reorganize(self):
        self.fileOrg.reorganize()

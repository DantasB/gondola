from Heap import Heap
from Schema import Schema


fileOrgs = {"Heap": Heap}


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
        self.fileOrg = fileOrgs[fileOrg](relation_name=name)

    def insert(self):
        """
        inserir linha numa tabela
        """
        pass

    def select(self, filter=["all", "in", "between"]):
        pass

    def delete(self, filter=["where", "all"]):
        pass

    def reorganize(self):
        pass

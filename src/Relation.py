from Heap import Heap
from Schema import Schema


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

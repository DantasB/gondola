from src.structures.relation import Relation
from random import randint

data_file = open('./input/data.csv', 'r')
file_lines = data_file.readlines()


def populate_relation(relation: Relation):
    for file_line in file_lines:
        """
        id 1 até 2022
        """
        data = file_line.strip('\n').split('|')
        data[0] = data[0].rjust(20, '0')
        data[1] = data[1].ljust(20, ' ')
        data[2] = data[2].ljust(20, ' ')
        relation.insert('|'.join(data))


def populate_relation_vlheap(relation: Relation):
    for file_line in file_lines[:100]:
        relation.insert(file_line.strip('\n'))


def validate_heap():
    print("Validando Heap")
    name = "ZodiacoHeap"
    fileOrg = "Heap"
    column_defs = {
        "id": {
            "type": "int",
            "size": 20
        },
        "nome": {
            "type": "varchar",
            "size": 20
        },
        "zodiaco": {
            "type": "varchar",
            "size": 20
        }
    }

    relation = Relation(fileOrg, name, column_defs)
    populate_relation(relation)
    relation.fileOrg.reorganize()
    relation.fileOrg.persist()
    for _ in range(5):
        random_number = randint(1, 2022)
        relation.select(lambda r: int(r.id) == random_number)


def validate_vlheap():
    print("Validando VLHeap")
    name = "ZodiacoVL"
    fileOrg = "VLHeap"
    column_defs = {
        "id": {
            "type": "int",
            "size": 20
        },
        "nome": {
            "type": "varchar",
            "size": 20
        },
        "zodiaco": {
            "type": "varchar",
            "size": 20
        }
    }
    relation = Relation(fileOrg, name, column_defs)
    populate_relation_vlheap(relation)
    relation.fileOrg.persist()
    for _ in range(5):
        random_number = randint(1, 2022)
        relation.select(lambda r: int(r.id) == random_number)


def validate_ordered():
    print("Validando Ordered")
    name = "ZodiacoOrdered"
    fileOrg = "Ordered"
    column_defs = {
        "id": {
            "type": "int",
            "size": 20
        },
        "nome": {
            "type": "varchar",
            "size": 20
        },
        "zodiaco": {
            "type": "varchar",
            "size": 20
        }
    }
    relation = Relation(fileOrg, name, column_defs)
    populate_relation(relation)
    relation.fileOrg.persist()
    for _ in range(5):
        random_number = randint(1, 2022)
        relation.select(lambda r: int(r.id) == random_number)


def validate_hash():
    print("Validando Hash")
    name = "ZodiacoHash"
    fileOrg = "Hash"
    column_defs = {
        "id": {
            "type": "int",
            "size": 20
        },
        "nome": {
            "type": "varchar",
            "size": 20
        },
        "zodiaco": {
            "type": "varchar",
            "size": 20
        }
    }

    relation = Relation(fileOrg, name, column_defs)
    populate_relation(relation)
    relation.delete(1983)
    relation.fileOrg.persist()
    for _ in range(5):
        random_number = randint(1, 2022)
        relation.select(lambda r: int(r.id) == random_number)


def main():
    validate_heap()
    validate_vlheap()
    validate_ordered()
    validate_hash()
    return


if __name__ == "__main__":
    main()

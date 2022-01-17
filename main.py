from src.structures.relation import Relation

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
    relation.delete(lambda r: int(r.id) == 423)
    relation.fileOrg.reorganize()
    relation.fileOrg.persist()


def validate_vlheap():
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
    relation.delete(lambda r: int(r.id) == 9)
    relation.fileOrg.persist()


def validate_ordered():
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
    relation.delete(lambda r: int(r.id) == 423)
    relation.fileOrg.persist()


def validate_hash():
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
    relation.delete(lambda r: int(r.id) == 2)
    relation.fileOrg.persist()


def main():
    validate_heap()
    validate_vlheap()
    validate_ordered()
    validate_hash()
    return


if __name__ == "__main__":
    main()

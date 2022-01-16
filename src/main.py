from Relation import Relation


def validate_heap():
    name = "Amigos"
    fileOrg = "Heap"
    column_defs = {
        "id": {
            "type": "int",
            "size": 10
        },
        "nome": {
            "type": "varchar",
            "size": 255
        }
    }
    relation = Relation(fileOrg, name, column_defs)
    record_data = ('222|"bernado"')
    relation.insert(record_data)
    record_data = ('423|"bauzudo"')
    relation.insert(record_data)
    record_data = ('122|"Joao"')
    relation.insert(record_data)
    relation.delete(lambda r: r.id == 423)
    relation.fileOrg.persist()

def validate_vlheap():
    name = "AmigosVL"
    fileOrg = "VLHeap"
    column_defs = {
        "id": {
            "type": "int",
            "size": 10
        },
        "nome": {
            "type": "varchar",
            "size": 255
        }
    }
    relation = Relation(fileOrg, name, column_defs)
    record_data = ('222|"bernado"')
    relation.insert(record_data)
    record_data = ('423|"bauzudo"')
    relation.insert(record_data)
    record_data = ('122|"Joao"')
    relation.insert(record_data)
    relation.delete(lambda r: r.id == 423)
    relation.fileOrg.persist()


def validate_ordered():
    name = "AmigosOrdered"
    fileOrg = "Ordered"
    column_defs = {
        "id": {
            "type": "int",
            "size": 10
        },
        "nome": {
            "type": "varchar",
            "size": 255
        }
    }
    relation = Relation(fileOrg, name, column_defs)
    record_data = ('222|"bernado"')
    relation.insert(record_data)
    record_data = ('423|"bauzudo"')
    relation.insert(record_data)
    record_data = ('122|"Joao"')
    relation.insert(record_data)
    relation.delete(lambda r: r.id == 423)
    relation.fileOrg.persist()


def main():
    validate_heap()
    validate_vlheap()
    validate_ordered()

    return


if __name__ == "__main__":
    main()

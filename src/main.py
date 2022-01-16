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
    record_data = ('423|"bauzudo"')
    relation.insert(record_data)
    #relation.delete(lambda r: r.id == 223)
    #a, b = relation.fileOrg.select_id(323)

    relation.fileOrg.persist()
    # relation.fileOrg.heap.persist()

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
    record_data = ('423|"bauzudo"')
    relation.insert(record_data)
    #relation.delete(lambda r: r.id == 223)
    #a, b = relation.fileOrg.select_id(323)

    relation.fileOrg.persist()
    # relation.fileOrg.heap.persist()


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
    record_data = ('423|"bauzudo"')
    relation.insert(record_data)
    #relation.delete(lambda r: r.id == 223)
    #a, b = relation.fileOrg.select_id(323)

    relation.fileOrg.persist()
    # relation.fileOrg.heap.persist()


def main():
    validate_heap()
    validate_vlheap()
    validate_ordered()
    return


if __name__ == "__main__":
    main()

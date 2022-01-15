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
    record_data = ('123|"bauzudo"')
    relation.insert(record_data)

    relation.fileOrg.persist()
    # relation.fileOrg.heap.persist()


def main():
    validate_heap()
    return


if __name__ == "__main__":
    main()

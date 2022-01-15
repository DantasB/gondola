from Relation import Relation


def validate_heap():
    name = "Amigos"
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
    #record_data = ('423|"bauzudo"')
    # relation.insert(record_data)
    a, b = relation.fileOrg.select_id(423)

    relation.fileOrg.persist()
    relation.fileOrg.heap.persist()


def main():
    validate_heap()
    return


if __name__ == "__main__":
    main()

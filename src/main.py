from Relation import Relation


def validate_heap():
    name = "Amigos"
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
    record_data = ('123|"bauzudo"')
    relation.insert(record_data)
    relation.delete(lambda r: r[0] == '221')

    relation.fileOrg.persist()


def main():
    validate_heap()
    return


if __name__ == "__main__":
    main()

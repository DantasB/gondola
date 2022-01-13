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
    relation.insert(
        ('555, "lauzudo"')
    )


def main():
    validate_heap()
    return


if __name__ == "__main__":
    main()

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
    relation.fileOrg.reorganize()
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


def validate_hash():
    name = "AmigosHash"
    fileOrg = "Hash"
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
    relation = Relation(fileOrg, name, column_defs, bucket_size=3)
    record_data = ('1|"bernado"')
    relation.insert(record_data)
    record_data = ('4|"asdasd"')
    relation.insert(record_data)
    record_data = ('5|"lima"')
    relation.insert(record_data)
    record_data = ('10|"bergmonta"')
    record_data = ('2|"bauzudo"')
    relation.insert(record_data)
    record_data = ('3|"Joao"')
    relation.insert(record_data)
    relation.delete(lambda r: r.id == 2)
    relation.fileOrg.persist()


def main():
    validate_heap()
    # validate_vlheap()
    # validate_ordered()
    # validate_hash()
    return


if __name__ == "__main__":
    main()

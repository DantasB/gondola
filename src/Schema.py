from Column import Column


class Schema:
    def __init__(self, relation_name, column_defs: dict) -> None:
        self.relation_name = relation_name
        self.columns = []
        for column_key in column_defs.keys():
            column_info = column_defs[column_key]
            self.columns.append(
                Column(
                    name=column_key, column_type=column_info["type"], size=column_info["size"]
                )
            )

        size = 0
        for column in self.columns:
            size += column.size
        self.record_size = size

    def metadata(self):
        meta = "$".join([column.metadata() for column in self.columns])
        meta = f"{self.record_size}${meta}"
        return meta

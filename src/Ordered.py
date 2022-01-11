from FileOrg import FileOrg


class Ordered(FileOrg):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.empty_list = self.metadata_file.readline()
        self.block_count = self.metadata_file.readline()

    def insert(self):
        pass

    def select(self, filter):
        r = []
        ix = 0
        while True:
            block = super().read_block(ix)
            if len(block) <= 0:
                break
            for record in block.records:
                if filter(record):
                    r.append(record)
            ix += 1
        return r

    def delete(self):
        pass

    def reorganize(self):
        pass

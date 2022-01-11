from FileOrg import FileOrg


class Ordered(FileOrg):
    def __init__(self, relation_name):
        super().__init__(relation_name)
        self.empty_list = self.metadata_file.readline()
        self.block_count = self.metadata_file.readline()

    def insert(self):
        pass

    def reorganize(self):
        pass

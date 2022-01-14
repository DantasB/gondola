import os


class Loader:
    BLOCK_SIZE = int(os.getenv('BLOCK_SIZE'))
    HEADER_SIZE = int(os.getenv('HEADER_SIZE'))
    RECORDS_IN_A_BLOCK = int(os.getenv('RECORDS_IN_A_BLOCK'))
    RECORD_SIZE = int(os.getenv('RECORD_SIZE'))

    def load_file(self, file_path):
        if not os.path.exists(file_path):
            temp = open(file_path, 'x')
            temp.close()
        f = open(file_path, "r+")
        return f

    def load_empty_list(self, file):
        tuples = file.readline().rstrip('\n').split('|')
        if tuples[0] != '':
            return [tuple(map(int, t.split(','))) for t in tuples]
        else:
            return []

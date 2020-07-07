import json
import zstd
import os

class Archive:
    def __init__(self, out_dir, name='OS'):
        self.out_dir = out_dir
        self.name = name
        self.data = []
        self.i = 0
        if os.path.exists(out_dir) and len(os.listdir(out_dir)) > 0:
            self.i = max(map(lambda x: int(x.split('_')[1].split('.')[0]), os.listdir(out_dir))) + 1

    def add_data(self, data):
        self.data.append(data)

    def commit(self):
        compression_level = 3
        cdata = zstd.compress(json.dumps(self.data).encode('UTF-8'), compression_level)
        with open(self.out_dir + '/' + self.name + '_' + str(self.i) + '.json.zst', 'wb') as fh:
            fh.write(cdata)
        self.i += 1
        self.data = []
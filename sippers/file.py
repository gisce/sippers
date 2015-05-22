from __future__ import absolute_import
import os

from sippers.configs.parser import get_parser
from sippers.utils import naturalsize


class SipsFileStats(object):
    def __init__(self, stat):
        self.stat = stat
        self.read = 0

    @property
    def size(self):
        return naturalsize(self.stat.st_size)

    @property
    def progress(self):
        return '{}%'.format(
            int((float(self.read) / float(self.stat.st_size)) * 100)
        )


class SipsFile(object):
    def __init__(self, path):
        self.path = path
        self.stats = SipsFileStats(os.stat(path))
        self.fd = open(path, 'r')
        self.parser = get_parser(self.path)

    def __iter__(self):
        return self

    def next(self):
        for line in self.fd:
            self.stats.read += len(line)
            return line
        raise StopIteration()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fd.close()

    def close(self):
        self.fd.close()

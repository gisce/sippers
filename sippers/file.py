from __future__ import absolute_import
import codecs
import os
import zipfile

from sippers.configs.parser import get_parser
from sippers.utils import naturalsize
from sippers import logger


class SipsFileStats(object):
    def __init__(self, size):
        self.st_size = size
        self.read = 0

    @property
    def size(self):
        return naturalsize(self.st_size)

    @property
    def progress(self):
        return '{}%'.format(
            int((float(self.read) / float(self.st_size)) * 100)
        )


class PackedSipsFileStats(SipsFileStats):
    def __init__(self, size, n_files):
        super(PackedSipsFileStats, self).__init__(size)
        self.n_files = n_files
        self.idx_file = 0

    @property
    def progress(self):
        return '{} ({}/{})'.format(
            super(PackedSipsFileStats, self).progress,
            self.idx_file, self.n_files
        )


class PackedSipsFile(object):
    def __init__(self, path):
        self.path = path
        self.parser = get_parser(self.path)()
        if not zipfile.is_zipfile(self.path):
            logger.error("File %s is not a zip file", self.path)
            raise zipfile.BadZipfile
        self.fd = zipfile.ZipFile(self.path)
        self.parser = get_parser(self.path)()
        self.files = iter(self.fd.namelist())
        self.stats = PackedSipsFileStats(
            os.stat(path).st_size, len(self.fd.namelist())
        )

    def __iter__(self):
        return self

    def next(self):
        for filename in self.files:
            self.stats.idx_file += 1
            stats = SipsFileStats(self.fd.getinfo(filename).file_size)
            sips_fd = self.fd.open(filename)
            sf = SipsFile(filename, fd=sips_fd, parser=self.parser)
            sf.stats = stats
            self.stats.read += self.fd.getinfo(filename).compress_size
            return sf
        self.stats.read = self.stats.st_size
        raise StopIteration()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.fd.close()


class SipsFile(object):
    def __init__(self, path, fd=None, parser=None):
        self.path = path
        if parser is None:
            self.parser = get_parser(self.path)()
        else:
            self.parser = parser
        if fd is None:
            self.fd = open(path, 'r')
            self.stats = SipsFileStats(os.fstat(self.fd.fileno()).st_size)
        else:
            self.fd = fd
        self.parser.load_config()

    def __iter__(self):
        return self

    def next(self):
        for line in self.fd:
            self.stats.read += len(line)
            return self.parser.parse_line(line)
        raise StopIteration()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.fd.close()

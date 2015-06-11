from __future__ import absolute_import
from datetime import datetime
import os
import zipfile

from sippers.parsers.parser import get_parser
from sippers.utils import naturalsize
from sippers import logger


class SipsFileStats(object):
    """ Stats for a SIPS file
    """
    def __init__(self, size):
        """
        :param size: Size of file
        """
        self.st_size = size
        self.read = 0
        self.start = datetime.now()
        self.line_number = 0
        self.errors = []

    @property
    def size(self):
        """Human readeable size of the SIPS file.
        """
        return naturalsize(self.st_size)

    @property
    def progress(self):
        """Progress in % of read content.
        """
        return '{}%'.format(
            int((float(self.read) / float(self.st_size)) * 100)
        )

    @property
    def elapsed_time(self):
        """Elapsed time processing the file.
        """
        return '{}'.format(datetime.now() - self.start)

    @property
    def speed(self):
        elapsed = (datetime.now() - self.start).total_seconds()
        if elapsed:
            return '{}/s'.format(naturalsize(self.read / elapsed))
        return '--'

    @property
    def number_of_errors(self):
        return len(self.errors)

    @property
    def rate_error(self):
        return (self.number_of_errors * 1.0 / self.line_number) * 100


class PackedSipsFileStats(SipsFileStats):
    """Stats for a Packed SIPS file (with zip).
    """
    def __init__(self, size, n_files):
        """
        :param size: Size of the file
        :param n_files: Number of files packed
        """
        super(PackedSipsFileStats, self).__init__(size)
        self.n_files = n_files
        self.idx_file = 0

    @property
    def progress(self):
        """Progress in % and the number of files read.

         Format example: 80% (8/10)
        """
        return '{} ({}/{})'.format(
            super(PackedSipsFileStats, self).progress,
            self.idx_file, self.n_files
        )


class PackedSipsFile(object):
    """Packed SIPS file.

    Process of content of zip file is processed with iterators to keep the
    minimal memory footprint.

    Example::

        with PackedSipsFile('/tmp/PACKED.SIPS.zip') as packed:
            for sips_file in packed:
                for line in sips_file:
                    print sips_file.stats.progress
                    print line
                print packed.stats.progress

    """
    def __init__(self, path, strict=False):
        """
        :param path: Packed SIPS file path
        """
        self.path = path
        self.parser = get_parser(self.path)(strict=strict)
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
        """Close the file descriptor.
        """
        self.fd.close()


class SipsFile(object):
    """SIPS file.

    Process of content of file is processed with iterators to keep the
    minimal memory footprint.

    Example::

        with SipsFile('/tmp/SIPS.TXT') as sips_file:
            for line in sips_file:
                print sips_file.stats.progress
                print line

    :param path: Path of SIPS file
    :param fd: File descriptor (use this if you have already opened the file)
    :param parser: Force to use a parser
    :param strict: Strict schema validation
    :param `SipsFileStats` resume: Resume an imported file passing a SipsFileStats
    """
    def __init__(self, path, fd=None, parser=None, strict=False, resume=None):
        self.path = path
        self.resume_line_number = 0
        if parser is None:
            self.parser = get_parser(self.path)(strict=strict)
        else:
            self.parser = parser
        if fd is None:
            self.fd = open(path, 'r')
            self.stats = SipsFileStats(os.fstat(self.fd.fileno()).st_size)
        else:
            self.fd = fd
        if resume:
            assert isinstance(resume, SipsFileStats)
            self.stats = resume
            self.resume_line_number = self.stats.line_number
            self.stats.line_number = 0

    def __iter__(self):
        return self

    def next(self):
        for line in self.fd:
            self.stats.line_number += 1
            if self.stats.line_number <= self.resume_line_number:
                continue
            self.stats.read += len(line)
            try:
                data, errors = self.parser.parse_line(line)
                if errors:
                    self.stats.errors.append(
                        (self.stats.line_number, errors)
                    )
                return data
            except Exception as exc:
                logger.critical(
                    "Error in %s L:%s: %s %s", self.path, self.stats.line_number,
                    str(exc), line, exc_info=True
                )
                self.stats.errors.append(
                    (self.stats.line_number, str(exc))
                )
                if self.parser.adapter.strict:
                    raise
        raise StopIteration()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.fd.close()

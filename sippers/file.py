from __future__ import absolute_import

from sippers.configs.parser import get_parser


class SipsFileStats(object):
    def __init__(self, n_lines):
        self.n_lines = n_lines
        self.r_lines = 0
        self.line = 0
        self.state = 0


class SipsFile(object):
    def __init__(self, path):
        self.stats = SipsFileStats()
        self.path = path
        self.parser = get_parser(self.path)

    def readline(self):
        yield 'Foo'
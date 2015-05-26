import codecs
import unittest

from sippers.file import SipsFile, PackedSipsFile
from sippers.utils import naturalsize
from . import SipsTestCaseBase, get_data


class PackedSipsFileTest(SipsTestCaseBase):

    def test_iterator_file(self):
        for dso in self.SIPS_PACKED_DATA:
            sips_file = self.SIPS_PACKED_DATA[dso]['file']
            psf = PackedSipsFile(sips_file)
            for sf in psf:
                for _ in sf:
                    pass
                self.assertEqual(sf.stats.progress, '100%')
            self.assertEqual(psf.stats.progress, '100% (1/1)')


class SipsFileTest(SipsTestCaseBase):

    def test_iterator_file(self):
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            sf = SipsFile(sips_file)
            with codecs.open(self.SIPS_DATA[dso]['file'],
                             encoding=sf.parser.encoding) as orig:
                orig_content = orig.read()
            content = ''
            for line in sf:
                content += line['orig']
            self.assertEqual(orig_content, content)
            sf.close()

    def test_stats(self):
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            path = get_data(sips_file)
            sf = SipsFile(path)
            for _ in sf:
                read = sf.stats.read
                progress = sf.stats.progress
            self.assertEqual(progress, '100%')
            self.assertEqual(naturalsize(read), sf.stats.size)
            sf.close()

    def test_with_statement(self):
        sips_file = self.SIPS_DATA.values()[0]['file']
        path = get_data(sips_file)
        with SipsFile(path) as sf:
            for _ in sf:
                pass
        self.assertIs(sf.fd.closed, True)

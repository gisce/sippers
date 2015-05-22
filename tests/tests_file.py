from sippers.file import SipsFile
from sippers.utils import naturalsize
from . import SipsTestCaseBase, get_data


class SipsFileTest(SipsTestCaseBase):

    def test_iterator_file(self):
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            path = get_data(sips_file)
            orig_content = self.SIPS_DATA[dso]['content']
            sf = SipsFile(path)
            content = ''
            for line in sf:
                content += line
            self.assertEqual(orig_content, content)
            sf.close()

    def test_stats(self):
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            path = get_data(sips_file)
            sf = SipsFile(path)
            print sf.stats.size
            for _ in sf:
                read = sf.stats.read
                progress = sf.stats.progress
            self.assertEqual(progress, '100%')
            self.assertEqual(naturalsize(read), sf.stats.size)
            sf.close()

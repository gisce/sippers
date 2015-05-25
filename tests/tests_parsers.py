from . import SipsTestCaseBase, get_data
from sippers.file import SipsFile


class TestRegisteredClasses(SipsTestCaseBase):
    def test_registered_class(self):
        from sippers.configs.parser import _PARSERS

        self.assertItemsEqual(_PARSERS.keys(), [
            'sippers.configs.iberdrola.Iberdrola',
            'sippers.configs.endesa.Endesa',
            'sippers.configs.endesa.EndesaCons'
        ])


class TestParser(SipsTestCaseBase):
    def test_parse_ps(self):
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            lines = []
            with SipsFile(sips_file) as sf:
                for line in sf:
                    lines.append(line['ps'])
            self.assertEqual(len(lines), 10)
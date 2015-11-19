from . import SipsTestCaseBase
from sippers.file import SipsFile, PackedSipsFile


class TestRegisteredClasses(SipsTestCaseBase):
    def test_registered_class(self):
        from sippers.parsers.parser import _PARSERS

        self.assertItemsEqual(_PARSERS.keys(), [
            'sippers.parsers.iberdrola.Iberdrola',
            'sippers.parsers.endesa.Endesa',
            'sippers.parsers.endesa.EndesaCons',
            'sippers.parsers.hidrocantabrico.Hidrocantabrico',
            'sippers.parsers.hidrocantabrico.HidrocantabricoMeasures',
            'sippers.parsers.cnmc.Cnmc',
            'sippers.parsers.cnmc.CnmcCons',
        ])


class TestParser(SipsTestCaseBase):
    def test_parse_ps(self):
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            lines = []
            line_numbers = len(open(sips_file, 'r').readlines())
            with SipsFile(sips_file, strict=True) as sf:
                for line in sf:
                    self.assertIn('ps', line)
                    self.assertIn('measures', line)
                    self.assertIn('orig', line)
                    lines.append(line['ps'])
            self.assertEqual(len(lines), line_numbers)
        for dso in self.SIPS_PACKED_DATA:
            sips_file = self.SIPS_PACKED_DATA[dso]['file']
            with PackedSipsFile(sips_file, strict=True) as psf:
                for sf in psf:
                    lines = []
                    for line in sf:
                        self.assertIn('ps', line)
                        self.assertIn('measures', line)
                        self.assertIn('orig', line)
                        lines.append(line['ps'])
from __future__ import unicode_literals
from tests import SipsTestCaseBase
from sippers.file import SipsFile, PackedSipsFile


class TestRegisteredClasses(SipsTestCaseBase):
    def test_registered_class(self):
        from sippers.parsers.parser import _PARSERS
        self.maxDiff = None
        self.assertEqual(list(sorted(list(_PARSERS.keys()))), list(sorted([
            "sippers.parsers.cnmc.Cnmc",
            "sippers.parsers.cnmc.CnmcCons",
            "sippers.parsers.endesa.EndesaCons",
            "sippers.parsers.cnmc_v2.CnmcV2Cons",
            "sippers.parsers.hidrocantabrico.HidrocantabricoMeasures",
            "sippers.parsers.cnmc_v2.CnmcV2",
            "sippers.parsers.cnmc_gas.CnmcGas",
            "sippers.parsers.iberdrola.Iberdrola",
            "sippers.parsers.cnmc_gas.CnmcGasCons",
            "sippers.parsers.endesa.Endesa",
            "sippers.parsers.hidrocantabrico.Hidrocantabrico"
        ])))


class TestParser(SipsTestCaseBase):
    def test_parse_ps(self):
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            lines = []
            with open(sips_file, 'rb') as _f:
                line_numbers = len(_f.readlines())
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
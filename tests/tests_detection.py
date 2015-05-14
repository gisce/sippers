from . import SipsTestCaseBase
from sippers.configs.parser import get_parser


COMPANY_PARSERS = {
    "IBERDROLA": "sippers.configs.iberdrola.Iberdrola"
}


class TestSipsDetection(SipsTestCaseBase):
    def test_name_pattern(self):
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            parser = get_parser(sips_file)
            path = '.'.join([parser.__module__, parser.__name__])
            self.assertEquals(
                COMPANY_PARSERS[dso],
                path
            )

    def test_assign_parser(self):
        from sippers.fitxer_sips import FitxerSips
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            sips = FitxerSips(sips_file)
            parser = sips.parser.__class__
            path = '.'.join([parser.__module__, parser.__name__])
            self.assertEquals(
                COMPANY_PARSERS[dso],
                path
            )
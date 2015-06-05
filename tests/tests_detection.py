from . import SipsTestCaseBase
from sippers.parsers.parser import get_parser
from sippers.file import SipsFile, PackedSipsFile


COMPANY_PARSERS = {
    "IBERDROLA": "sippers.parsers.iberdrola.Iberdrola",
    "HIDROCANTABRICO_PS": "sippers.parsers.hidrocantabrico.Hidrocantabrico",
    "HIDROCANTABRICO_CO": "sippers.parsers.hidrocantabrico.HidrocantabricoMeasures",
    "ENDESA_PS": "sippers.parsers.endesa.Endesa",
    "ENDESA_MEASURES": "sippers.parsers.endesa.EndesaCons",
    "HIDROCANTABRICO_PACKED_PS": "sippers.parsers.hidrocantabrico.Hidrocantabrico"
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
        for dso in self.SIPS_DATA:
            sips_file = self.SIPS_DATA[dso]['file']
            sips = SipsFile(sips_file)
            parser = sips.parser.__class__
            path = '.'.join([parser.__module__, parser.__name__])
            self.assertEquals(
                COMPANY_PARSERS[dso],
                path
            )
            sips.close()
        for dso in self.SIPS_PACKED_DATA:
            sips_file = self.SIPS_PACKED_DATA[dso]['file']
            sips = PackedSipsFile(sips_file)
            parser = sips.parser.__class__
            path = '.'.join([parser.__module__, parser.__name__])
            self.assertEquals(
                COMPANY_PARSERS[dso],
                path
            )
            sips.close()
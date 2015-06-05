import os
from unittest import TestCase


_ROOT = os.path.abspath(os.path.dirname(__file__))


def get_data(path):
    return os.path.join(_ROOT, 'data', path)


class SipsTestCaseBase(TestCase):
    """Base class to inherit test cases for SIPS.

    - Loads samples data from SIPS.
    """

    @staticmethod
    def load_sips(sips_file):
        return {'file': get_data(sips_file)}

    @classmethod
    def setUpClass(cls):
        cls.SIPS_DATA = {
            'IBERDROLA': cls.load_sips('HGSBKA_TXT2.TXT'),
            'HIDROCANTABRICO_PS': cls.load_sips('HIDROCANTABRICO_PS_50290_3-1A.TXT'),
            'HIDROCANTABRICO_CO': cls.load_sips('HIDROCANTABRICO_CO_50290_3-1A.TXT')
        }
        cls.SIPS_PACKED_DATA = {
            'ENDESA_PS': {'file': get_data('ERZ.INF.SEG01.ZIP')},
            'ENDESA_MEASURES': {'file': get_data('ERZ.INF2.SEG01.ZIP')},
            'HIDROCANTABRICO_PACKED_PS': {'file': get_data('HIDROCANTABRICO_PS.zip')}
        }

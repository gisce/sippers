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
        }

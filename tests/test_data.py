import unittest

from sippers import get_data


class TestDataExists(unittest.TestCase):

    def test_hc_poblacions(self):
        with open(get_data('hc_poblacions.json'), 'r') as _:
            pass

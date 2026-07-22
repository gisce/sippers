from __future__ import unicode_literals
import unittest

from sippers import get_data


class TestDataExists(unittest.TestCase):

    def test_hc_poblacions(self):
        with open(get_data('hc_poblacions.json'), 'rb') as _:
            pass

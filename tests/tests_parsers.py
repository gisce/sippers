from . import SipsTestCaseBase


class TestRegisteredClasses(SipsTestCaseBase):
    def test_registered_class(self):
        from sippers.configs.parser import _PARSERS

        self.assertEquals(_PARSERS.keys(), [
            'sippers.configs.iberdrola.Iberdrola'
        ])
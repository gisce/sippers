from . import SipsTestCaseBase


class TestRegisteredClasses(SipsTestCaseBase):
    def test_registered_class(self):
        from sippers.configs.parser import _PARSERS

        self.assertItemsEqual(_PARSERS.keys(), [
            'sippers.configs.iberdrola.Iberdrola',
            'sippers.configs.endesa.Endesa',
            'sippers.configs.endesa.EndesaCons'
        ])
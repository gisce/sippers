from . import SipsTestCaseBase
from sippers.file import SipsFile
from sippers.models import Document
from sippers.adapters import SipsAdapter, pre_insert
from sippers.backends import BaseBackend


class FakeAdapter(SipsAdapter):

    @pre_insert
    def replace_name(self, data):
        assert isinstance(self.backend, BaseBackend)
        data['name'] = '$' + data['name']
        return data


class FakeBackend(BaseBackend):
    pass


class TestAdapters(SipsTestCaseBase):

    def test_make_object_correct_adapter(self):
        for sips_file in self.SIPS_DATA.values():
            with SipsFile(sips_file['file']) as sf:
                for line in sf:
                    ps = line.get('ps')
                    if not ps:
                        continue
                    self.assertIsInstance(ps, Document)
                    self.assertIsInstance(
                        ps.adapter, sf.parser.ps_adapter.__class__
                    )
                    measures = line.get('measures')
                    if not measures:
                        continue
                    for measure in measures:
                        self.assertIsInstance(measure, Document)
                        self.assertIsInstance(
                            measure.adapter,
                            sf.parser.measures_adapter.__class__
                        )

    def test_pre_insert_decorator(self):
        sips_file = self.SIPS_DATA['HIDROCANTABRICO_PS']
        with SipsFile(sips_file['file']) as sf:
            for line in sf:
                ps = line.get('ps')
                if not ps:
                    continue
                ps.adapter = FakeAdapter()
                ps.adapter.backend = FakeBackend(None)
                cups = '$' + ps.data['name']
                data = ps.adapter._invoke_processors(
                    'pre_insert', False, ps.data, False
                )
                self.assertEqual(cups, data['name'])
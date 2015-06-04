from marshmallow import Schema
from marshmallow.decorators import tag_processor
from sippers.models import Document


def pre_insert(fn=None, raw=False):
    return tag_processor('pre_insert', fn, raw)


class SipsAdapter(Schema):

    def make_object(self, data):
        return Document(data, adapter=self)


class MeasuresAdapter(Schema):
    def make_object(self, data):
        return Document(data, adapter=self)

from marshmallow import Schema
from sippers.models import SipsSchema, MeasuresSchema


class SipsAdapter(Schema):

    def make_object(self, data):
        return data


class MeasuresAdapter(Schema):
    def make_object(self, data):
        return data

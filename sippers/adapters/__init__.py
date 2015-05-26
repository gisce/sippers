from marshmallow import Schema
from sippers import logger
from sippers.models import SipsSchema


class Adapter(Schema):

    def make_object(self, data):
        return SipsSchema().load(data).data

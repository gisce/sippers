from marshmallow import Schema, post_load
from marshmallow.decorators import tag_processor
from sippers.models import Document


def pre_insert(fn=None, raw=False):
    """Filter to use before inserting the document to the database.

    Useful when you must to do some operation between the parsed data and the
    already inserted data.

    Example for Hidrocantabrico when the measures file doesn't have the CUPS
    code and have the internal contract code::

        @pre_insert
        def fix_name(self, data):
            backend = self.backend
            result = backend.get(self.backend.ps_collection, {
                'ref': data['name'], 'cod_distri': '0026'}
            )
            if result:
                data['name'] = result[0]['name']
            return data
    """
    return tag_processor('pre_insert', fn, raw)


class SipsAdapter(Schema):
    """Base SIPS Adapter.
    """

    @post_load
    def make_document(self, data):
        return Document(data, adapter=self)


class MeasuresAdapter(Schema):
    """Base Measures Adapter.
    """

    @post_load
    def make_document(self, data):
        return Document(data, adapter=self)

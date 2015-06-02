from sippers.adapters import SipsAdapter, MeasuresAdapter
from sippers.models import SipsSchema, MeasuresSchema
from sippers.models.iberdrola import TARIFFS_OCSUM
from marshmallow import pre_load, fields


class HidrocantabricoSipsAdapter(SipsAdapter, SipsSchema):

    @pre_load
    def fix_floats(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.Float):
                if not data.get(attr):
                    data[attr] = '0'
                data[attr] = data[attr].replace(',', '.')
        return data
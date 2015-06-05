from sippers.adapters import SipsAdapter, MeasuresAdapter
from sippers.models import SipsSchema, MeasuresSchema
from sippers.models.iberdrola import TARIFFS_OCSUM
from marshmallow import pre_load, fields


class IberdrolaSipsAdapter(SipsAdapter, SipsSchema):

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                data[attr] += 'T00:00:00'
        return data

    @pre_load
    def adapt_distribuidora(self, data):
        data['distri'] = 'IBERDROLA'
        data['cod_distri'] = '0021'
        return data

    @pre_load
    def fix_floats(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.Float):
                if not data.get(attr):
                    data[attr] = 0
                if isinstance(data[attr], basestring):
                    data[attr] = data[attr].replace(',', '.')
        return data

    @pre_load
    def adapt_indicatiu_icp(self, data):
        if data.get('indicatiu_icp') == '2':
            data['indicatiu_icp'] = '1'
        return data

    @pre_load
    def adapt_persona_fisica_juridica(self, data):
        persona = data.get('persona_fj')
        persona_map = {'PJ': '1', 'PF': '0'}
        if persona and persona in persona_map:
            data['persona_fj'] = persona_map[persona]
        return data

    @pre_load
    def adapt_tarifa(self, data):
        tarifa = data.get('tarifa')
        if tarifa and tarifa in TARIFFS_OCSUM:
            data['tarifa'] = TARIFFS_OCSUM[tarifa]
        return data

    @pre_load
    def adapt_tensio(self, data):
        tensio = data.get('tensio')
        try:
            tensio = int(tensio)
        except ValueError:
            pass
        data['tensio'] = str(tensio)
        return data

    @pre_load
    def adapt_primera_vivenda(self, data):
        pv_map = {u'N': '0', u'S': '1'}
        pv = data.get('primera_vivenda')
        if pv and pv in pv_map:
            data['primera_vivenda'] = pv_map[pv]
        return data

class IberdrolaMeasuresAdapter(MeasuresAdapter, MeasuresSchema):

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                data[attr] += 'T00:00:00'
        return data
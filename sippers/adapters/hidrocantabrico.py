import json

from sippers import get_data
from sippers.adapters import SipsAdapter, MeasuresAdapter
from sippers.models import Document, SipsSchema, MeasuresSchema
from sippers.models.iberdrola import TARIFFS_OCSUM
from marshmallow import pre_load, fields


class HidrocantabricoSipsAdapter(SipsAdapter, SipsSchema):

    with open(get_data('hc_poblacions.json'), 'r') as f:
        poblacions = json.load(f, encoding="utf-8")

    @pre_load
    def fix_floats(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.Float):
                if not data.get(attr):
                    data[attr] = '0'
                data[attr] = data[attr].replace(',', '.')
        return data

    @pre_load
    def fix_poblacio(self, data):
        data['poblacio'] = self.poblacions.get(data['poblacio'], None)
        return data

    @pre_load
    def fix_primera_vivenda(self, data):
        mapping = {'N': '0', 'S': '1'}
        pv = data['primera_vivenda']
        data['primera_vivenda'] = mapping.get(pv)
        return data

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                orig = data[attr]
                if orig not in ('', '0', '00000000'):
                    data[attr] = '{}-{}-{}T00:00:00'.format(
                        orig[0:4], orig[4:6], orig[6:8]
                    )
                else:
                    data[attr] = None
        return data

    @pre_load
    def fix_perfil_consum(self, data):
        pc = data['perfil_consum']
        if pc:
            data['perfil_consum'] = 'P' + pc.lower()
        else:
            data['perfil_consum'] = None
        return data

    @pre_load
    def fix_indicatiu_icp(self, data):
        icp = data['indicatiu_icp']
        mapping = {'S': '1', 'N': '0'}
        data['indicatiu_icp'] = mapping.get(icp)
        return data

    @pre_load
    def fix_propietat_equip_mesura(self, data):
        mapping = {'D': '0', 'T': '1'}
        propietat = data['propietat_equip_mesura']
        data['propietat_equip_mesura'] = mapping.get(propietat)
        return data

    @pre_load
    def fix_persona_fj(self, data):
        mapping = {'F': '0', 'J': '1'}
        fj = data['persona_fj']
        data['persona_fj'] = mapping.get(fj)
        return data


class HidrocantabricoMeasuresAdapter(MeasuresAdapter, MeasuresSchema):

    @pre_load
    def fix_numbers(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.Integer):
                if not data[attr]:
                    data[attr] = 0
                else:
                    data[attr] = float(data[attr].replace(',', '.'))
        return data

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                orig = data[attr]
                if orig not in ('', '0', '00000000'):
                    data[attr] = '{}-{}-{}T00:00:00'.format(
                        orig[0:4], orig[4:6], orig[6:8]
                    )
                else:
                    data[attr] = None
        return data

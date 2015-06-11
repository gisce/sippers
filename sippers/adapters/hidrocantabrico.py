import json

from sippers import get_data
from sippers.adapters import SipsAdapter, MeasuresAdapter, pre_insert
from sippers.models import SipsSchema, MeasuresSchema, TARIFFS as BASE_TARIFFS
from marshmallow import pre_load, fields


TARIFFS = {
    '20A': '2.0A',
    '20DHA': '2.0DHA',
    '20DHS': '2.0DHS',
    '21A': '2.1A',
    '21DHA': '2.1DHA',
    '21DHS': '2.1DHS',
    '30A': '3.0A',
    '31A': '3.1A',
    '61': '6.1',
    '61A': '6.1A',
    '61B': '6.1B',
    '62': '6.2',
    '63': '6.3',
    '64': '6.4',
    '65': '6.5'
}


class HidrocantabricoSipsAdapter(SipsAdapter, SipsSchema):

    with open(get_data('hc_poblacions.json'), 'r') as f:
        poblacions = json.load(f, encoding="utf-8")

    @pre_load
    def adapt_tarifa(self, data):
        tarifa = data.get('tarifa')
        if tarifa not in BASE_TARIFFS:
            tarifa = TARIFFS.get(tarifa, tarifa)
        data['tarifa'] = tarifa
        return data

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
        pv = data.get('primera_vivenda')
        data['primera_vivenda'] = mapping.get(pv)
        return data

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                orig = data.get(attr)
                if orig not in ('', '0', '00000000'):
                    data[attr] = '{}-{}-{}T00:00:00'.format(
                        orig[0:4], orig[4:6], orig[6:8]
                    )
                else:
                    data[attr] = None
        return data

    @pre_load
    def fix_perfil_consum(self, data):
        pc = data.get('perfil_consum')
        if pc:
            data['perfil_consum'] = 'P' + pc.lower()
        else:
            data['perfil_consum'] = None
        return data

    @pre_load
    def fix_indicatiu_icp(self, data):
        icp = data.get('indicatiu_icp')
        mapping = {'S': '1', 'N': '0'}
        data['indicatiu_icp'] = mapping.get(icp)
        return data

    @pre_load
    def fix_propietat_equip_mesura(self, data):
        mapping = {'D': '0', 'T': '1'}
        propietat = data.get('propietat_equip_mesura')
        data['propietat_equip_mesura'] = mapping.get(propietat)
        return data

    @pre_load
    def fix_persona_fj(self, data):
        mapping = {'F': '0', 'J': '1'}
        fj = data.get('persona_fj')
        data['persona_fj'] = mapping.get(fj)
        return data


class HidrocantabricoMeasuresAdapter(MeasuresAdapter, MeasuresSchema):

    @pre_load
    def fix_numbers(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, (fields.Integer, fields.Float)):
                if not data.get(attr):
                    data[attr] = 0
                else:
                    data[attr] = float(data.get(attr).replace(',', '.'))
        return data

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                orig = data.get(attr)
                if orig not in ('', '0', '00000000'):
                    data[attr] = '{}-{}-{}T00:00:00'.format(
                        orig[0:4], orig[4:6], orig[6:8]
                    )
                else:
                    data[attr] = None
        return data

    @pre_insert
    def fix_name(self, data):
        backend = self.backend
        result = backend.get(self.backend.ps_collection, {
            'ref': data['name'], 'cod_distri': '0026'}
        )
        if result:
            data['name'] = result[0]['name']
        return data

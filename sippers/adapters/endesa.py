from sippers import logger
from sippers.adapters import SipsAdapter, MeasuresAdapter
from sippers.models import SipsSchema, MeasuresSchema, TARIFFS as BASE_TARIFFS
from marshmallow import Schema, fields, pre_load


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
"""Mapping of ENDESA tariffs codes."""


class EndesaBaseAdapter(Schema):
    """Endesa SIPS Adapter
    """

    @pre_load
    def fix_dates(self, data):
        """Fix the dates in the SIPS file from ENDESA.

        In the endesa sips file dates are ``YYYYMMDD`` or ``0`` or ``00000000``.
        With this all ``fields.DateTime`` fields are caugth and parsed to a
        correct format ``YYYY-MM-DDT00:00:00``.
        """
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                orig = data.get(attr)
                if orig and orig not in ('0', '00000000'):
                    data[attr] = '{}-{}-{}T00:00:00'.format(
                        orig[0:4], orig[4:6], orig[6:8]
                    )
                else:
                    data[attr] = None
        return data

    @pre_load
    def fix_numbers(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.Integer):
                if not data.get(attr):
                    data[attr] = 0
        return data

    @pre_load
    def fix_floats(self, data):
        """Fix floats numbers.

        Replace ``,`` to ``.``
        """
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.Float):
                if not data.get(attr):
                    data[attr] = 0
                if isinstance(data[attr], basestring):
                    data[attr] = data[attr].replace(',', '.')
        return data

class EndesaSipsAdapter(EndesaBaseAdapter, SipsAdapter, SipsSchema):
    """Endesa SIPS Adapter.
    """

    @pre_load
    def adapt_tarifa(self, data):
        """Fix the ATR Tariff code

        Using :attr:`TARIFFS`
        """
        tarifa = data.get('tarifa')
        if tarifa not in BASE_TARIFFS:
            tarifa = TARIFFS.get(tarifa, tarifa)
        data['tarifa'] = tarifa
        return data

    @pre_load
    def adapt_indicatiu_icp(self, data):
        icp = data.get('indicatiu_icp')
        if icp and icp == 'ICP INSTALADO':
            data['indicatiu_icp'] = '1'
        else:
            data['indicatiu_icp'] = '0'
        return data

    @pre_load
    def adapt_propietat_equip_mesura(self, data):
        propietat = data.get('propietat_equip_mesura')
        if propietat and propietat == 'EMPRESA DISTRIBUIDORA':
            data['propietat_equip_mesura'] = '1'
        else:
            data['propietat_equip_mesura'] = '0'
        return data

    @pre_load
    def adapt_perfil_consum(self, data):
        perfil = data.get('perfil_consum')
        if perfil:
            data['perfil_consum'] = data['perfil_consum'].title()
        else:
            data['perfil_consum'] = None
        return data

    @pre_load
    def adapt_primera_vivenda(self, data):
        vivenda = data.get('primera_vivenda')
        values = {'S': '1', 'N': '0'}
        if vivenda and vivenda in values:
            data['primera_vivenda'] = values[vivenda]
        else:
            data['primera_vivenda'] = None
        return data

    @pre_load
    def adapt_tipo_pm(self, data):
        pm = data.get('tipo_pm')
        values = {
            'TIPO I': '01',
            'TIPO II': '02',
            'TIPO III': '03',
            'TIPO IV': '04',
            'TIPO V': '05'
        }
        if pm and pm in values:
            data['tipo_pm'] = values[pm]
        else:
            data['tipo_pm'] = None
        return data

    @pre_load
    def adapt_persona_fj(self, data):
        persona = data.get('persona_fj')
        values = {'J': '1', 'F': '0'}
        if persona and persona in values:
            data['persona_fj'] = values[persona]
        else:
            data['persona_fj'] = None
        return data


class EndesaMeasuresAdapter(EndesaBaseAdapter, MeasuresSchema, MeasuresAdapter):
    pass

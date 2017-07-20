from sippers.adapters import SipsAdapter, MeasuresAdapter
from sippers.models import SipsSchema, MeasuresSchema
from sippers.models.iberdrola import TARIFFS_OCSUM
from sippers.models import TENSIO_CNMC
from sippers.models import TELEGESTIO_CNMC
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
    def adapt_propietat_icp(self, data):
        if data.get('propietat_icp') == '2':
            data['propietat_icp'] = '1'
        else:
            data['propietat_icp'] = '0'
        return data

    @pre_load
    def adapt_propietat_edm(self, data):
        if data.get('propietat_equip_mesura') == '2':
            data['propietat_equip_mesura'] = '1'
        else:
            data['propietat_equip_mesura'] = '0'
        return data

    @pre_load
    def adapt_persona_fisica_juridica(self, data):
        # segons taula 6 CNMC
        persona = data.get('persona_fj')
        persona_map = {
            'CI': '1',
            'CT': '0',
            'DN': '0',
            'NI': '0',
            'NV': '0',
            'OT': '0',
            'PS': '0',
            'NE': '0',
            'PF': '0',
            'PJ': '1',
        }
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
        if tensio and tensio in TENSIO_CNMC:
            data['tensio'] = TENSIO_CNMC[tensio]
        return data

    @pre_load
    def adapt_primera_vivenda(self, data):
        pv_map = {u'0': '1', u'1': '0'}
        pv = data.get('primera_vivenda')
        if pv and pv in pv_map:
            data['primera_vivenda'] = pv_map[pv]
        else:
            data['primera_vivenda'] = None
        return data

    @pre_load
    def adapt_telegesio(self, data):
        telegestion = data.get('telegestion')
        if telegestion and telegestion in TELEGESTIO_CNMC:
            data['telegestio'] = '0'
            if telegestion in ('01', '02'):
                data['telegestio'] = '1'
        return data

    @pre_load
    def adapt_potcttada_w_to_kw(self, data):
        measures = [
            'pot_cont_p1',
            'pot_cont_p2',
            'pot_cont_p3',
            'pot_cont_p4',
            'pot_cont_p5',
            'pot_cont_p6',
        ]
        for measure in measures:
            m_data = data.get(measure)
            if int(m_data) > 0:
                data[measure] = int(m_data) / 1000
        return data

IberdrolaSipsAdapter()

class IberdrolaMeasuresAdapter(MeasuresAdapter, MeasuresSchema):

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                data[attr] += 'T00:00:00'
        return data

    @pre_load
    def measures_w_to_kw(self, data):
        measures = [
            'activa_1',
            'activa_2',
            'activa_3',
            'activa_4',
            'activa_5',
            'activa_6',
            'reactiva_1',
            'reactiva_2',
            'reactiva_3',
            'reactiva_4',
            'reactiva_5',
            'reactiva_6',
            'potencia_1',
            'potencia_2',
            'potencia_3',
            'potencia_4',
            'potencia_5',
            'potencia_6',
        ]
        for measure in measures:
            m_data = data.get(measure)
            if int(m_data) > 0:
                data[measure] = int(m_data) / 1000
        return data

IberdrolaMeasuresAdapter()

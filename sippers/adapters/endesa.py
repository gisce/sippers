from sippers.adapters import Adapter
from sippers.models import SipsSchema
from marshmallow import pre_load


class EndesaSipsAdapter(Adapter, SipsSchema):

    @pre_load
    def fix_dates(self, data):
        for d in ('data_alta', 'data_ulti_mov', 'data_ult_canv', 'data_lim_exten'):
            orig = data[d]
            if orig != '0':
                data[d] = '{}-{}-{}'.format(orig[0:4], orig[4:6], orig[6:8])
            else:
                data[d] = None
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

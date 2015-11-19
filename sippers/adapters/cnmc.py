from sippers.adapters import SipsAdapter, MeasuresAdapter
from sippers.models import SipsSchema, MeasuresSchema
from marshmallow import pre_load, fields


class CnmcSipsAdapter(SipsAdapter, SipsSchema):
    '''A self.fields tenim els camps per defecte, els de SipsSchema base'''

    @pre_load
    def adapt_nom_cognoms(self, data):
        nom_complet = data.get('nom_complet')
        data['cognom'] = nom_complet
        return data

    @pre_load
    def add_distri_description(self, data):
        cod_distri = data.get('cod_distri')
        if cod_distri == '059':
            data['distri'] = 'GRUPO ELECTRIFICACION RURAL BINEFAR'
        return data

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                # si ve alguna data, assumim que ve correcta
                if data[attr] == u'':   data[attr] = None
                else:                   data[attr] += 'T00:00:00'
        return data

    @pre_load
    def separate_potencies_contractades(self, data):
        potencies_contractades = data.get('potencies_contractades')

        potencies = potencies_contractades.split(';')

        while len(potencies) < 6:
            potencies.append('0') # omplim minim fins 6 periodes

        if len(potencies) == 7:
            [pot_cont_p1, pot_cont_p2, pot_cont_p3, pot_cont_p4, pot_cont_p5,
             pot_cont_p6, pot_cont_p7] = potencies
        else: # 6 periodes
            [pot_cont_p1, pot_cont_p2, pot_cont_p3, pot_cont_p4, pot_cont_p5,
             pot_cont_p6] = potencies

        data['pot_cont_p1'] = int(pot_cont_p1)
        data['pot_cont_p2'] = int(pot_cont_p2)
        data['pot_cont_p3'] = int(pot_cont_p3)
        data['pot_cont_p4'] = int(pot_cont_p4)
        data['pot_cont_p5'] = int(pot_cont_p5)
        data['pot_cont_p6'] = int(pot_cont_p6)
        if 'pot_cont_p7' in locals(): data['pot_cont_p7'] = int(pot_cont_p7)

        return data

    @pre_load
    def adapt_propietat_equip_mesura(self, data):

        propietat = data.get('propietat_equip_mesura')

        if propietat and propietat == u'E':
            data['propietat_equip_mesura'] = '1'
        else:
            data['propietat_equip_mesura'] = '0'

        return data

    @pre_load
    def adapt_consumption_profile(self, data):

        cp = data.get('perfil_consum')

        if cp == u'Pa':     cp = 'Pa'
        elif cp == u'Pb':   cp = 'Pb'
        elif cp == u'Pc':   cp = 'Pc'
        elif cp == u'Pd':   cp = 'Pd'
        else:
            cp = None

        data['perfil_consum'] = cp

        return data

    @pre_load
    def adapt_tipo_pm(self, data):

        tipo_pm = data.get('tipo_pm')

        if tipo_pm == u'1':     tipo_pm = '01'
        elif tipo_pm == u'2':   tipo_pm = '02'
        elif tipo_pm == u'3':   tipo_pm = '03'
        elif tipo_pm == u'4':   tipo_pm = '04'
        elif tipo_pm == u'5':   tipo_pm = '05'
        else:
            tipo_pm = None

        data['tipo_pm'] = tipo_pm

        return data

class CnmcMeasuresAdapter(MeasuresAdapter, MeasuresSchema):

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                # si ve alguna dada, assumim que ve correcta
                if data[attr] == u'':   data[attr] = None
                else:                   data[attr] += 'T00:00:00'

        return data

    @pre_load
    def separate_activa(self, data):

        activa = data.get('activa') # actives separades per pt i coma
        actives = activa.split(';')

        while len(actives) < 6:
            actives.append('0') # omplim minim fins 6 periodes

        if len(actives) == 7:
            [activa_1, activa_2, activa_3, activa_4, activa_5, activa_6,
             activa_7] = actives
        else: # 6 periodes
            [activa_1, activa_2, activa_3, activa_4, activa_5,
             activa_6] = actives

        data['activa_1'] = int(float(activa_1))
        data['activa_2'] = int(float(activa_2))
        data['activa_3'] = int(float(activa_3))
        data['activa_4'] = int(float(activa_4))
        data['activa_5'] = int(float(activa_5))
        data['activa_6'] = int(float(activa_6))
        if 'activa_7' in locals(): data['activa_7'] = int(float(activa_7))

        return data

    @pre_load
    def separate_reactiva(self, data):

        reactiva = data.get('reactiva') # reactives separades per pt i coma
        reactives = reactiva.split(';')

        while len(reactives) < 6:
            reactives.append('0') # omplim minim fins 6 periodes

        if len(reactives) == 7:
            [reactiva_1, reactiva_2, reactiva_3, reactiva_4, reactiva_5,
             reactiva_6, reactiva_7] = reactives
        else: # 6 periodes
            [reactiva_1, reactiva_2, reactiva_3, reactiva_4, reactiva_5,
             reactiva_6] = reactives

        data['reactiva_1'] = int(float(reactiva_1))
        data['reactiva_2'] = int(float(reactiva_2))
        data['reactiva_3'] = int(float(reactiva_3))
        data['reactiva_4'] = int(float(reactiva_4))
        data['reactiva_5'] = int(float(reactiva_5))
        data['reactiva_6'] = int(float(reactiva_6))
        if 'reactiva_7' in locals(): data['reactiva_7'] = int(float(reactiva_7))

        return data

    @pre_load
    def separate_potencia(self, data):

        potencia = data.get('potencia') # reactives separades per pt i coma
        potencies = potencia.split(';')

        while len(potencies) < 6:
            potencies.append('0') # omplim minim fins 6 periodes

        if len(potencies) == 7:
            [potencia_1, potencia_2, potencia_3, potencia_4, potencia_5,
             potencia_6, potencia_7] = potencies
        else: # 6 periodes
            [potencia_1, potencia_2, potencia_3, potencia_4, potencia_5,
             potencia_6] = potencies

        data['potencia_1'] = int(float(potencia_1))
        data['potencia_2'] = int(float(potencia_2))
        data['potencia_3'] = int(float(potencia_3))
        data['potencia_4'] = int(float(potencia_4))
        data['potencia_5'] = int(float(potencia_5))
        data['potencia_6'] = int(float(potencia_6))
        if 'potencia_7' in locals(): data['potencia_7'] = int(float(potencia_7))

        return data

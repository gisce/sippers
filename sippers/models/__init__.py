from marshmallow import Schema, fields

TARIFFS = [
    '2.0A',
    '2.0DHA',
    '2.0DHS',
    '2.1A',
    '2.1DHA',
    '2.1DHS',
    '3.0A',
    '3.1A',
    '6.1A',
    '6.1B',
    '6.2',
    '6.3',
    '6.4',
    '6.5'
]

TYPE_PS = ['01', '02', '03', '04', '05']

CONSUMPTION_PROFILE = ['Pa', 'Pb', 'Pc', 'Pd']


class SipsSchema(Schema):
    name = fields.String(required=True)
    distri = fields.String()
    cod_distri = fields.String()
    poblacio = fields.String()
    direccio = fields.String()
    codi_postal = fields.String()
    provincia = fields.String()
    data_alta = fields.Date(allow_none=True)
    nom = fields.String()
    cognom = fields.String()
    direccio_titular = fields.String()
    pot_max_puesta = fields.Float()
    pot_max_bie = fields.Float()
    tarifa = fields.Select(choices=TARIFFS),
    des_tarifa = fields.String()
    tensio = fields.String()
    tipo_pm = fields.Select(choices=TYPE_PS)
    indicatiu_icp = fields.Select(choices=['0', '1'])
    perfil_consum = fields.Select(choices=CONSUMPTION_PROFILE, allow_none=True)
    der_extensio = fields.Float()
    der_acces_llano = fields.Float()
    der_acces_valle = fields.Float()
    propietat_equip_mesura = fields.Select(choices=['0', '1'])
    propietat_icp = fields.Select(choices=['0', '1']),
    pot_cont_p1 = fields.Float()
    pot_cont_p2 = fields.Float()
    pot_cont_p3 = fields.Float()
    pot_cont_p4 = fields.Float()
    pot_cont_p5 = fields.Float()
    pot_cont_p6 = fields.Float()
    data_ulti_mov = fields.Date(allow_none=True)
    data_ult_canv = fields.Date(allow_none=True)
    data_lim_exten = fields.Date(allow_none=True)
    persona_fj = fields.Select(choices=['0', '1'], allow_none=True)
    primera_vivenda = fields.Select(choices=['0', '1'], allow_none=True)
    fianza = fields.Float()


class MeasuresSchema(Schema):
    name = fields.String(required=True)
    data_final = fields.Date(allow_none=True)
    real_estimada = fields.String()
    activa_1 = fields.Integer()
    tipo_a1 = fields.String()
    activa_2 = fields.Integer()
    tipo_a2 = fields.String()
    activa_3 = fields.Integer()
    tipo_a3 = fields.String()
    activa_4 = fields.Integer()
    tipo_a4 = fields.String()
    activa_5 = fields.Integer()
    tipo_a5 = fields.String()
    activa_6 = fields.Integer()
    tipo_a6 = fields.String()
    reactiva_1 = fields.Integer()
    tipo_r1 = fields.String()
    reactiva_2 = fields.Integer()
    tipo_r2 = fields.String()
    reactiva_3 = fields.Integer()
    tipo_r3 = fields.String()
    reactiva_4 = fields.Integer()
    tipo_r4 = fields.String()
    reactiva_5 = fields.Integer()
    tipo_r5 = fields.String()
    reactiva_6 = fields.Integer()
    tipo_r6 = fields.String()
    potencia_1 = fields.Integer()
    tipo_p1 = fields.String()
    potencia_2 = fields.Integer()
    tipo_p2 = fields.String()
    potencia_3 = fields.Integer()
    tipo_p3 = fields.String()
    potencia_4 = fields.Integer()
    tipo_p4 = fields.String()
    potencia_5 = fields.Integer()
    tipo_p5 = fields.String()
    potencia_6 = fields.Integer()
    tipo_p6 = fields.String()
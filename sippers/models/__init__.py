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
    data_alta = fields.Date()
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
    perfil_consum = fields.Select(choices=CONSUMPTION_PROFILE)
    der_extensio = fields.Float()
    der_acces_lalno = fields.Float()
    der_access_valle = fields.Float()
    propietat_equip_mesura = fields.Select(choices=['0', '1'])
    propietat_icp = fields.Select(choices=['0', '1']),
    pot_cont_p1 = fields.Float()
    pot_cont_p2 = fields.Float()
    pot_cont_p3 = fields.Float()
    pot_cont_p4 = fields.Float()
    pot_cont_p5 = fields.Float()
    pot_cont_p6 = fields.Float()
    data_ulti_mov = fields.Date()
    data_ult_canv = fields.Date()
    data_lim_exten = fields.Date()
    persona_fj = fields.Select(choices=['0', '1'])
    primera_vivenda = fields.Select(choices=['0', '1'])
    fianza = fields.Integer()
    consum_any_p1 = fields.Integer()
    consum_any_p2 = fields.Integer()
    consum_any_p3 = fields.Integer()
    consum_any_p4 = fields.Integer()
    consum_any_p5 = fields.Integer()
    consum_any_p6 = fields.Integer()
    consum_any_p7 = fields.Integer()
    max_maximetre_p1 = fields.Float()
    max_maximetre_p2 = fields.Float()
    max_maximetre_p3 = fields.Float()
    max_maximetre_p4 = fields.Float()
    max_maximetre_p5 = fields.Float()
    max_maximetre_p6 = fields.Float()
    max_maximetre_p7 = fields.Float()

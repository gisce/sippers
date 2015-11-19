from marshmallow import Schema, fields
from marshmallow.validate import OneOf

from sippers.models import TYPE_PS, CONSUMPTION_PROFILE


TARIFFS_OCSUM = {
    '001': "2.0A",
    '003': "3.0A",
    '004': "2.0DHA",
    '005': "2.1A",
    '006': "2.1DHA",
    '007': "2.0DHS",
    '008': "2.1DHS",
    '011': "3.1A",
    '012': "6.1",
    '013': "6.2",
    '014': "6.3",
    '015': "6.4",
    '016': "6.5"
}

class IberdrolaSipsSchema(Schema):

    name = fields.String(position=0, length=22)
    direccio = fields.String(position=1, length=150)
    poblacio = fields.String(position=2, length=45)
    codi_postal = fields.String(position=3, length=10)
    provincia = fields.String(position=4, length=45)
    persona_fj = fields.String(
        validate=OneOf(['PF', 'PJ']), position=5, length=2
    )
    cognom = fields.String(position=6, length=50)
    direccio_titular = fields.String(position=7, length=80)
    municipi_titular = fields.String(position=8, length=45)
    codi_postal_titular = fields.String(position=9, length=10)
    provincia_titular = fields.String(position=10, length=45)
    data_alta = fields.Date(position=11, length=10)
    tarifa = fields.String(
        validate=OneOf(TARIFFS_OCSUM.keys()), position=12, length=3
    )
    tensio = fields.Integer(position=13, length=9)
    pot_max_bie = fields.Float(position=14, length=12)
    pot_max_puesta = fields.Float(position=15, length=12)
    tipo_pm = fields.String(validate=OneOf(TYPE_PS), position=16, length=2)
    indicatiu_icp = fields.String(position=17, length=1)
    perfil_consum = fields.String(
        validate=OneOf(CONSUMPTION_PROFILE), position=18, length=2
    )
    der_acces_reconocido = fields.Float(position=19, length=12)
    der_extensio = fields.Float(position=20, length=12)
    propietat_equip_mesura = fields.String(position=21, length=1)
    propietat_icp = fields.String(position=22, length=1)
    pot_cont_p1 = fields.Float(position=23, length=12)
    pot_cont_p2 = fields.Float(position=24, length=12)
    pot_cont_p3 = fields.Float(position=25, length=12)
    pot_cont_p4 = fields.Float(position=26, length=12)
    pot_cont_p5 = fields.Float(position=27, length=12)
    pot_cont_p6 = fields.Float(position=28, length=12)
    pot_cont_p7 = fields.Float(position=29, length=12)
    pot_cont_p8 = fields.Float(position=30, length=12)
    pot_cont_p9 = fields.Float(position=31, length=12)
    pot_cont_p10 = fields.Float(position=32, length=12)
    data_ult_canv = fields.Date(position=33, length=10)
    data_ulti_mov = fields.Date(position=34, length=10)
    data_lim_exten = fields.Date(position=35, length=10)
    data_ult_lect = fields.Date(position=36, length=10)
    pot_disp_caixa = fields.Float(position=37, length=12)
    impago = fields.String(position=38, length=11)
    diposit_garantia = fields.String(
        validate=OneOf(['1', '2']), position=39, length=1
    )
    import_diposit = fields.Float(position=40, length=11)
    primera_vivenda = fields.String(
        validate=OneOf(['S', 'N']), position=41, length=1
    )
    telegest_actiu = fields.String(position=42, length=2)
    any_sub = fields.Integer(position=43, length=4)
    trimestre_sub = fields.Integer(position=44, length=1)


class IberdrolaMeasuresSchema(Schema):
    any_consum = fields.Integer(position=0, length=4)
    facturacio_consum = fields.String(position=1, length=4)
    data_anterior = fields.Date(position=2, length=10)
    data_final = fields.Date(position=3, length=10)
    tarifa_consums = fields.String(position=4, length=4)
    DH = fields.String(position=5, length=3)
    activa_1 = fields.Integer(position=6, length=14)
    activa_2 = fields.Integer(position=7, length=14)
    activa_3 = fields.Integer(position=8, length=14)
    activa_4 = fields.Integer(position=9, length=14)
    activa_5 = fields.Integer(position=10, length=14)
    activa_6 = fields.Integer(position=11, length=14)
    activa_7 = fields.Integer(position=12, length=14)
    reactiva_1 = fields.Integer(position=13, length=14)
    reactiva_2 = fields.Integer(position=14, length=14)
    reactiva_3 = fields.Integer(position=15, length=14)
    reactiva_4 = fields.Integer(position=16, length=14)
    reactiva_5 = fields.Integer(position=17, length=14)
    reactiva_6 = fields.Integer(position=18, length=14)
    reactiva_7 = fields.Integer(position=19, length=14)
    potencia_1 = fields.Integer(position=20, length=11)
    potencia_2 = fields.Integer(position=21, length=11)
    potencia_3 = fields.Integer(position=22, length=11)
    potencia_4 = fields.Integer(position=23, length=11)
    potencia_5 = fields.Integer(position=24, length=11)
    potencia_6 = fields.Integer(position=25, length=11)
    potencia_7 = fields.Integer(position=26, length=11)

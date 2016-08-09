from marshmallow import Schema, fields
from marshmallow.validate import OneOf

from sippers.models import TYPE_PS, CONSUMPTION_PROFILE
from sippers.models import TENSIO_CNMC, TELEGESTIO_CNMC


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
    '016': "6.5",
    '017': "6.1B",
}

class IberdrolaSipsSchema(Schema):

    name = fields.String(position=0, length=22)
    codi_postal = fields.String(position=1, length=5)
    municipio = fields.String(position=2, length=5)
    provincia = fields.String(position=3, length=2)
    persona_fj = fields.String(position=4,length=2)
    data_alta = fields.Date(position=5, length=10)
    tarifa = fields.String(
        validate=OneOf(TARIFFS_OCSUM.keys()), position=6, length=3
    )
    tensio = fields.String(
        validate=OneOf(TENSIO_CNMC.keys()), position=7, length=2
    )
    pot_max_bie = fields.Integer(position=8, length=11)
    pot_max_puesta = fields.Integer(position=9, length=11)
    tipo_pm = fields.String(validate=OneOf(TYPE_PS), position=10, length=2)
    indicatiu_icp = fields.String(position=11, length=1)
    perfil_consum = fields.String(
        validate=OneOf(CONSUMPTION_PROFILE), position=12, length=2
    )
    der_extensio = fields.Integer(position=13, length=11)
    der_acces_reconocido = fields.Integer(position=14, length=11)
    propietat_equip_mesura = fields.String(position=15, length=1)
    propietat_icp = fields.String(position=16, length=1)
    pot_cont_p1 = fields.Integer(position=17, length=14)
    pot_cont_p2 = fields.Integer(position=18, length=14)
    pot_cont_p3 = fields.Integer(position=19, length=14)
    pot_cont_p4 = fields.Integer(position=20, length=14)
    pot_cont_p5 = fields.Integer(position=21, length=14)
    pot_cont_p6 = fields.Integer(position=22, length=14)
    data_ulti_mov = fields.Date(position=23, length=10)
    data_ult_canv = fields.Date(position=24, length=10)
    data_lim_exten = fields.Date(position=25, length=10)
    data_ult_lect = fields.Date(position=26, length=10)
    impago = fields.Integer(position=27, length=11)
    import_diposit = fields.Integer(position=28, length=9)
    primera_vivenda = fields.String(position=29, length=1)
    telegestion = fields.String(
        validate=OneOf(TELEGESTIO_CNMC.keys()), position=30, length=2)
    fases_edm = fields.String(
        validate=OneOf(['M', 'T']), position=31, length=1)
    autoconsumo = fields.String(position=32, length=2)
    transf_intensidad = fields.String(position=33, length=15)
    cnae = fields.String(position=34, length=4)
    modo_control_potencia = fields.Integer(position=35, length=1)
    contratable = fields.String(
        validate=OneOf(['0', '1']), position=36, length=1
    )
    motivo_estado_ps = fields.String(position=37, length=2)
    clase_expediente = fields.String(position=38, length=1)
    motivo_expediente = fields.String(position=39, length=2)

IberdrolaSipsSchema()

class IberdrolaMeasuresSchema(Schema):
    data_inicial = fields.Date(position=0, length=10)
    data_final = fields.Date(position=1, length=10)
    tarifa = fields.String(
        validate=OneOf(TARIFFS_OCSUM.keys()), position=2, length=3)
    activa_1 = fields.Integer(position=3, length=14)
    activa_2 = fields.Integer(position=4, length=14)
    activa_3 = fields.Integer(position=5, length=14)
    activa_4 = fields.Integer(position=6, length=14)
    activa_5 = fields.Integer(position=7, length=14)
    activa_6 = fields.Integer(position=8, length=14)
    reactiva_1 = fields.Integer(position=9, length=14)
    reactiva_2 = fields.Integer(position=10, length=14)
    reactiva_3 = fields.Integer(position=11, length=14)
    reactiva_4 = fields.Integer(position=12, length=14)
    reactiva_5 = fields.Integer(position=13, length=14)
    reactiva_6 = fields.Integer(position=14, length=14)
    potencia_1 = fields.Integer(position=15, length=14)
    potencia_2 = fields.Integer(position=16, length=14)
    potencia_3 = fields.Integer(position=17, length=14)
    potencia_4 = fields.Integer(position=18, length=14)
    potencia_5 = fields.Integer(position=19, length=14)
    potencia_6 = fields.Integer(position=20, length=14)

IberdrolaMeasuresSchema()
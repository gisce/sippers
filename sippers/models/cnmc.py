# -*- coding: utf-8 -*-
from marshmallow import Schema, fields

class CnmcMeasuresSchema(Schema):

    name = fields.String(position=0)
    data_inici = fields.Date(position=1)
    data_final = fields.Date(position=2)

    # activa, reactiva i potencia segons cnmc han de venir una dada per
    # periode, ordre ascendent, separats per ';'.
    # Caldrà adaptar-los per separar els periodes
    activa = fields.String(position=3)
    reactiva = fields.String(position=4)
    potencia = fields.String(position=5)

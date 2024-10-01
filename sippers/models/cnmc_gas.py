# -*- coding: utf-8 -*-
from marshmallow import Schema, fields
from marshmallow.validate import OneOf

from sippers.utils import TAULA_PROPIEDAD_CONTADOR_CORRECTOR, TAULA_RESULTADO_INSPECCION, TAULA_PERFIL_CONSUMO, TAULA_TIPO_PEAJE, TAULA_TIPO_LECTURA_GAS


class CnmcGasSipsSchema(Schema):
    """Base model for SIPS
    """
    apellido1Titular = fields.String(allow_none=True)
    apellido2Titular = fields.String(allow_none=True)
    calibreContador = fields.String(allow_none=True)
    caudalHorarioEnWh = fields.String(allow_none=True)
    caudalMaximoDiarioEnWh = fields.String(allow_none=True)
    cnae = fields.String(allow_none=True)
    codigoAccesibilidadContador = fields.String(allow_none=True)
    codigoContador = fields.String(allow_none=True)
    codigoEmpresaDistribuidora = fields.String(allow_none=True)
    codigoPeajeEnVigor = fields.String(validate=OneOf(TAULA_TIPO_PEAJE),allow_none=True)
    codigoPostalPS = fields.String(allow_none=True)
    codigoPostalTitular = fields.String(allow_none=True)
    codigoPresion = fields.String(allow_none=True)
    codigoProvinciaPS = fields.String(allow_none=True)
    codigoProvinciaTitular = fields.String(allow_none=True)
    codigoResultadoInspeccion = fields.String(validate=OneOf(TAULA_RESULTADO_INSPECCION), allow_none=True)
    codigoTelemedida = fields.String(allow_none=True)
    conectadoPlantaSatelite = fields.String(allow_none=True)
    cups = fields.String(allow_none=True)
    derechoTUR = fields.String(allow_none=True)
    desMunicipioPS = fields.String(allow_none=True)
    desMunicipioTitular = fields.String(allow_none=True)
    desProvinciaPS = fields.String(allow_none=True)
    desProvinciaTitular = fields.String(allow_none=True)
    esViviendaHabitual = fields.String(allow_none=True)
    escaleraPS = fields.String(allow_none=False)
    escaleraTitular = fields.String(allow_none=True)
    fechaUltimaInspeccion = fields.DateTime(allow_none=True)
    fechaUltimoCambioComercializador = fields.DateTime(allow_none=True)
    fechaUltimoMovimientoContrato = fields.DateTime(allow_none=True)
    idTipoTitular = fields.String(allow_none=True)
    idTitular = fields.String(allow_none=True)
    informacionImpagos = fields.String(allow_none=True)
    municipioPS = fields.String(allow_none=True)
    municipioTitular = fields.String(allow_none=True)
    nombreEmpresaDistribuidora = fields.String(allow_none=True)
    nombreTitular = fields.String(allow_none=True)
    numFincaPS = fields.String(allow_none=True)
    numFincaTitular = fields.String(allow_none=True)
    pctd = fields.String(allow_none=True)
    pisoPS = fields.String(allow_none=True)
    pisoTitular = fields.String(allow_none=True)
    portalPS = fields.String(allow_none=True)
    portalTitular = fields.String(allow_none=True)
    presionMedida = fields.String(allow_none=False)
    propiedadEquipoMedida = fields.String(validate=OneOf(TAULA_PROPIEDAD_CONTADOR_CORRECTOR) ,allow_none=True)
    puertaPS = fields.String(allow_none=True)
    puertaTitular = fields.String(allow_none=True)
    tipoContador = fields.String(allow_none=True)
    tipoCorrector = fields.String(allow_none=True)
    tipoPerfilConsumo = fields.String(OneOf(TAULA_PERFIL_CONSUMO), allow_none=True)
    tipoViaPS = fields.String(allow_none=True)
    tipoViaTitular = fields.String(allow_none=True)
    viaPS = fields.String(allow_none=True)
    viaTitular = fields.String(allow_none=True)

class CnmcGasMeasuresSchema(Schema):

    cups = fields.String(position=0, required=True, allow_none=False)
    fechaInicioMesConsumo = fields.DateTime(required=True, position=1, allow_none=False)
    fechaFinMesConsumo = fields.DateTime(required=True, position=2, allow_none=False)
    codigoTarifaPeaje = fields.String(validate=OneOf(TAULA_TIPO_PEAJE), position=3, required=False, allow_none=True)
    consumoEnWhP1 = fields.String(position=4, required=False, allow_none=True)
    consumoEnWhP2 = fields.String(position=5, required=False, allow_none=True)
    caudalMedioEnWhdia = fields.String(position=6, required=False, allow_none=True)
    caudaMinimoDiario = fields.String(position=7, required=False, allow_none=True)
    caudaMaximoDiario = fields.String(position=8, required=False, allow_none=True)
    porcentajeConsumoNocturno = fields.String(position=9, required=False, allow_none=True)
    codigoTipoLectura = fields.String(validate=OneOf(TAULA_TIPO_LECTURA_GAS), position=10, required=False, allow_none=True)
# -*- coding: utf-8 -*-
from marshmallow import Schema, fields
from sippers.utils import TABLA_6, TABLA_9, TABLA_17, TABLA_30, TABLA_32, TABLA_35, TABLA_62, TABLA_64, TABLA_108, TABLA_111
from marshmallow.validate import OneOf


class CnmcV2SipsSchema(Schema):
    """Base model for SIPS
    """
    CNAE = fields.String(allow_none=True)
    aplicacionBonoSocial = fields.String(allow_none=True)
    codigoAPM = fields.String(allow_none=True)
    codigoAccesibilidadContador = fields.String(allow_none=True)
    codigoAutoconsumo = fields.String(allow_none=True)
    codigoBIE = fields.String(allow_none=True)
    codigoClaseExpediente = fields.String(allow_none=True)
    codigoClasificacionPS = fields.String(validate=OneOf(TABLA_30), allow_none=True)
    codigoComercializadora = fields.String(allow_none=True)
    codigoDHEquipoDeMedida = fields.String(validate=OneOf(TABLA_35), allow_none=True)
    codigoDisponibilidadICP = fields.String(allow_none=True)
    codigoEmpresaDistribuidora = fields.String(allow_none=True)
    codigoFasesEquipoMedida = fields.String(allow_none=True)
    codigoModoControlPotencia = fields.String(allow_none=True)
    codigoMotivoExpediente = fields.String(allow_none=True)
    codigoPSContratable = fields.String(allow_none=True)
    codigoPeriodicidadFacturacion = fields.String(validate=OneOf(TABLA_108), allow_none=True)
    codigoPostalPS = fields.String(allow_none=True)
    codigoPropiedadEquipoMedida = fields.String(validate=OneOf(TABLA_32), allow_none=True)
    codigoPropiedadICP = fields.String(validate=OneOf(TABLA_32), allow_none=True)
    codigoProvinciaPS = fields.String(allow_none=True)
    codigoTarifaATREnVigor = fields.String(validate=OneOf(TABLA_17), allow_none=True)
    codigoTelegestion = fields.String(validate=OneOf(TABLA_111), allow_none=True)
    codigoTensionMedida = fields.String(validate=OneOf(TABLA_64), allow_none=True)
    codigoTensionV = fields.String(validate=OneOf(TABLA_64), allow_none=True)
    codigoTipoContrato = fields.String(validate=OneOf(TABLA_9), allow_none=True)
    codigoTipoSuministro = fields.String(validate=OneOf(TABLA_62), allow_none=True)
    cups = fields.String(required=True, allow_none=False)
    esViviendaHabitual = fields.String(allow_none=True)
    fechaAltaSuministro = fields.DateTime(allow_none=True)
    fechaCaducidadAPM = fields.DateTime(allow_none=True)
    fechaCaducidadBIE = fields.DateTime(allow_none=True)
    fechaEmisionAPM = fields.DateTime(allow_none=True)
    fechaEmisionBIE = fields.DateTime(allow_none=True)
    fechaLimiteDerechosReconocidos = fields.DateTime(allow_none=True)
    fechaUltimaLectura = fields.DateTime(allow_none=True)
    fechaUltimoCambioComercializador = fields.DateTime(allow_none=True)
    fechaUltimoMovimientoContrato = fields.DateTime(allow_none=True)
    importeDepositoGarantiaEuros = fields.String(allow_none=True)
    informacionImpagos = fields.String(allow_none=True)
    motivoEstadoNoContratable = fields.String(allow_none=True)
    municipioPS = fields.String(allow_none=True)
    nombreEmpresaDistribuidora = fields.String(allow_none=True)
    potenciaCGPW = fields.String(allow_none=True)
    potenciaMaximaAPMW = fields.String(allow_none=True)
    potenciaMaximaBIEW = fields.String(allow_none=True)
    potenciasContratadasEnWP1 = fields.String(allow_none=True)
    potenciasContratadasEnWP2 = fields.String(allow_none=True)
    potenciasContratadasEnWP3 = fields.String(allow_none=True)
    potenciasContratadasEnWP4 = fields.String(allow_none=True)
    potenciasContratadasEnWP5 = fields.String(allow_none=True)
    potenciasContratadasEnWP6 = fields.String(allow_none=True)
    relacionTransformacionIntensidad = fields.String(allow_none=True)
    tipoIdTitular = fields.String(validate=OneOf(TABLA_6), allow_none=True)
    tipoPerfilConsumo = fields.String(allow_none=True)
    valorDerechosAccesoW = fields.String(allow_none=True)
    valorDerechosExtensionW = fields.String(allow_none=True)

class CnmcV2MeasuresSchema(Schema):

    cups = fields.String(position=0, required=True, allow_none=False)
    codigoDHEquipoDeMedida = fields.String(position=22, allow_none=True)
    codigoTarifaATR = fields.String(position=3, allow_none=True)
    codigoTipoLectura = fields.String(position=23, allow_none=True)
    consumoEnergiaActivaEnWhP1 = fields.String(position=4)
    consumoEnergiaActivaEnWhP2 = fields.String(position=5)
    consumoEnergiaActivaEnWhP3 = fields.String(position=6)
    consumoEnergiaActivaEnWhP4 = fields.String(position=7)
    consumoEnergiaActivaEnWhP5 = fields.String(position=8)
    consumoEnergiaActivaEnWhP6 = fields.String(position=9)
    consumoEnergiaReactivaEnVArhP1 = fields.String(position=10)
    consumoEnergiaReactivaEnVArhP2 = fields.String(position=11)
    consumoEnergiaReactivaEnVArhP3 = fields.String(position=12)
    consumoEnergiaReactivaEnVArhP4 = fields.String(position=13)
    consumoEnergiaReactivaEnVArhP5 = fields.String(position=14)
    consumoEnergiaReactivaEnVArhP6 = fields.String(position=15)
    fechaFinMesConsumo = fields.DateTime(required=True, position=2, allow_none=False)
    fechaInicioMesConsumo = fields.DateTime(required=True, position=1, allow_none=False)
    potenciaDemandadaEnWP1 = fields.String(position=16)
    potenciaDemandadaEnWP2 = fields.String(position=17)
    potenciaDemandadaEnWP3 = fields.String(position=18)
    potenciaDemandadaEnWP4 = fields.String(position=19)
    potenciaDemandadaEnWP5 = fields.String(position=20)
    potenciaDemandadaEnWP6 = fields.String(position=21)

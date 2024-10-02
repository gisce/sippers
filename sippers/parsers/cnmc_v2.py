from __future__ import absolute_import
from __future__ import unicode_literals

import csv
from io import StringIO

import pymongo

from sippers import logger
from sippers.utils import build_dict
from sippers.parsers.parser import Parser, register
from sippers.adapters.cnmc_v2 import CnmcV2SipsAdapter, CnmcV2MeasuresAdapter
from sippers.models.cnmc_v2 import CnmcV2MeasuresSchema

class CnmcV2(Parser):

    # En aquest cas els llegim amb el csv.DictReader en comptes de fer-ho amb
    # un Schema de marshmallow, ja que el csv pot contenir comes dins d'un
    # camp si van wrapped entre cometes i el marshmallow aixo no ho contempla.

    # amb csv.DictReader obtindrem un diccionari amb els headers_ps
    # que li indiquem aqui, en comptes d'anar-los a buscar a l'Schema.
    # El diccionari el podem utilitzar per passar-li al
    # self.adapter.load igual que el resultat de l'esquema

    pattern = '[0-9]{6}_SIPS2_PS_ELECTRICIDAD_[a-z]*_?[a-z]*\.csv'
    encoding = "UTF-8"
    collection = 'cnmc_sips'
    collection_index = 'cups'
    index_unic = True

    def __init__(self, strict=False):

        # l'ordre dels camps segons format cnmc
        self.headers_ps = [
            'codigoEmpresaDistribuidora',
            'cups',
            'nombreEmpresaDistribuidora',
            'codigoPostalPS',
            'municipioPS',
            'codigoProvinciaPS',
            'fechaAltaSuministro',
            'codigoTarifaATREnVigor',
            'codigoTensionV',
            'potenciaMaximaBIEW',
            'potenciaMaximaAPMW',
            'codigoClasificacionPS',
            'codigoDisponibilidadICP',
            'tipoPerfilConsumo',
            'valorDerechosExtensionW',
            'valorDerechosAccesoW',
            'codigoPropiedadEquipoMedida',
            'codigoPropiedadICP',
            'potenciasContratadasEnWP1',
            'potenciasContratadasEnWP2',
            'potenciasContratadasEnWP3',
            'potenciasContratadasEnWP4',
            'potenciasContratadasEnWP5',
            'potenciasContratadasEnWP6',
            'fechaUltimoMovimientoContrato',
            'fechaUltimoCambioComercializador',
            'fechaLimiteDerechosReconocidos',
            'fechaUltimaLectura',
            'informacionImpagos',
            'importeDepositoGarantiaEuros',
            'tipoIdTitular',
            'esViviendaHabitual',
            'codigoComercializadora',
            'codigoTelegestion',
            'codigoFasesEquipoMedida',
            'codigoAutoconsumo',
            'codigoTipoContrato',
            'codigoPeriodicidadFacturacion',
            'codigoBIE',
            'fechaEmisionBIE',
            'fechaCaducidadBIE',
            'codigoAPM',
            'fechaEmisionAPM',
            'fechaCaducidadAPM',
            'relacionTransformacionIntensidad',
            'CNAE',
            'codigoModoControlPotencia',
            'potenciaCGPW',
            'codigoDHEquipoDeMedida',
            'codigoAccesibilidadContador',
            'codigoPSContratable',
            'motivoEstadoNoContratable',
            'codigoTensionMedida',
            'codigoClaseExpediente',
            'codigoMotivoExpediente',
            'codigoTipoSuministro',
            'aplicacionBonoSocial'
        ]
        self.adapter = CnmcV2SipsAdapter(strict=strict)

    def parse_line(self, line):

        # passar previament la linia pel csv reader
        # per que agafi be els camps tot i les comes dins del camp direccio
        # per fer-ho cal passar-la a StringIO
        _l = StringIO(line)
        reader = csv.DictReader(_l, fieldnames=self.headers_ps, delimiter=',')
        linia = next(reader)  # nomes n'hi ha una

        parsed = {'ps': {}, 'orig': line, 'collection': self.collection}
        result, errors = self.adapter.load(linia)

        if errors:
            logger.error(errors)
        parsed['ps'] = result
        return parsed, errors

register(CnmcV2)

class CnmcV2Cons(Parser):

    # En el cas de les mesures, usem Schema per mantenir el format i
    # perque no hi trobarem mes comes que les delimiters
    pattern = '[0-9]{6}_SIPS2_CONSUMOS_ELECTRICIDAD_[a-z]*_?[a-z]*\.csv'
    encoding = "UTF-8"
    collection = 'cnmc_sips_consums'
    collection_index = [("cups", pymongo.ASCENDING),("fechaFinMesConsumo", pymongo.DESCENDING)]
    index_unic = False


    def __init__(self, strict=False):
        self.schema = CnmcV2MeasuresSchema(strict=strict)
        self.adapter = CnmcV2MeasuresAdapter(strict=strict)
        self.measures_adapter = self.adapter
        self.fields = []
        self.headers = []
        for f in sorted(self.schema.fields,
                key=lambda f: self.schema.fields[f].metadata['position']):
            field = self.schema.fields[f]
            self.fields.append((f, field.metadata))
            self.headers.append(f)

    def parse_line(self, line):

        _l = StringIO(line)
        reader = csv.DictReader(_l, fieldnames=self.headers, delimiter=',')
        linia = next(reader)  # nomes n'hi ha una

        parsed = {'ps': {}, 'measure_cnmc': [], 'orig': line, 'collection': self.collection}

        result, errors = self.adapter.load(linia)

        if errors:
            logger.error(errors)

        parsed['measure_cnmc'] = result

        return parsed, errors

register(CnmcV2Cons)

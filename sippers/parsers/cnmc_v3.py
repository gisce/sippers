from __future__ import absolute_import

import csv
import StringIO

import pymongo

from sippers import logger
from sippers.utils import build_dict
from sippers.parsers.parser import Parser, register
from sippers.adapters.cnmc_v3 import CnmcV3SipsAdapter, CnmcV3MeasuresAdapter
from sippers.models.cnmc_v3 import CnmcV3MeasuresSchema

class CnmcV3(Parser):

    # En aquest cas els llegim amb el csv.DictReader en comptes de fer-ho amb
    # un Schema de marshmallow, ja que el csv pot contenir comes dins d'un
    # camp si van wrapped entre cometes i el marshmallow aixo no ho contempla.

    # amb csv.DictReader obtindrem un diccionari amb els headers_ps
    # que li indiquem aqui, en comptes d'anar-los a buscar a l'Schema.
    # El diccionari el podem utilitzar per passar-li al
    # self.adapter.load igual que el resultat de l'esquema

    pattern = '[0-9]{6}_SIPS2026_PS_ELECTRICIDAD_[a-z]*_?[a-z]*\.csv'
    encoding = "UTF-8"
    collection = 'cnmc_sips'
    collection_index = 'cups'
    index_unic = True

    def __init__(self, strict=False):

        # l'ordre dels camps segons format cnmc
        self.headers_ps = [
            'codigoEmpresaDistribuidora',
            'nombreEmpresaDistribuidora',
            'cups',
            'referenciaCatastralPS',
            'XPS',
            'YPS',
            'HusoPS',
            'BandaPS',
            'PaisPS',
            'codigoProvinciaPS',
            'desProvinciaPS',
            'codigoMunicipioPS',
            'desMunicipioPS',
            'PoblacionPS',
            'desPoblacionPS',
            'codigoPostalPS',
            'tipoViaPS',
            'viaPS',
            'numFincaPS',
            'duplicadorFincaPS',
            'escaleraPS',
            'pisoPS',
            'puertaPS',
            'tipoAclaradorFincaPS',
            'aclaradorFincaPS',
            'fechaAltaSuministro',
            'codigoTarifaATREnVigor',
            'codigoSegmentoCargoEnVigor',
            'codigoTensionV',
            'potenciaMaximaBIEW',
            'potenciaMaximaAPMW',
            'codigoClasificacionPS',
            'tipoControDelPotencia',
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
            'cambioComercializadorEnCurso',
            'codigoComercializadorVigente',
            'fechaUltimoCambioAgregadorIndependiente',
            'cambioAgregadorIndependienteEnCurso',
            'codigoAgregadorIndependienteVigente',
            'fechaLimiteDerechosReconocidos',
            'fechaUltimaLectura',
            'suspensionSuminstroImpago',
            'tipoPersona',
            'tipoIdTitular',
            'idTitular',
            'nombreTitular',
            'apellido1Titular',
            'apellido2Titular',
            'PaisTitular',
            'codigoProvinciaTitular',
            'desProvinciaTitular',
            'codigoMunicipioTitular',
            'desMunicipioTitular',
            'PoblacionTitular',
            'desPoblacionTitular',
            'codigoPostalTitular',
            'tipoViaTitular',
            'viaTitular',
            'numFincaTitular',
            'duplicadorFincaTitular',
            'escaleraTitular',
            'pisoTitular',
            'puertaTitular',
            'tipoAclaradorFincaTitular',
            'aclaradorFincaTitular',
            'esViviendaHabitual',
            'codigoLecturaRemota',
            'codigoFasesEquipoMedida',
            'acogimientoAutoconsumo',
            'aplicacionBonoSocial',
            'fiesuministroEsencialld82',
            'cnae',
            'codigoTipoContrato',
            'codigoPeriodicidadFacturacion',
            'codigoBIE',
            'fechaEmisionBIE',
            'fechaCaducidadBIE',
            'codigoAPM',
            'fechaEmisionAPM',
            'fechaCaducidadAPM',
            'relacionTransformacionIntensidad',
            'codigoModoControlPotencia',
            'potenciaCGPW',
            'codigoDHEquipoDeMedida',
            'codigoAccesibilidadContador',
            'codigoPSContratable',
            'motivoEstadoNoContratable',
            'codigoTensionMedida',
            'codigoClaseExpediente',
            'codigoMotivoExpediente',
            'codigoTipoSuministro'
        ]
        self.adapter = CnmcV3SipsAdapter(strict=strict)

    def parse_line(self, line):

        # passar previament la linia pel csv reader
        # per que agafi be els camps tot i les comes dins del camp direccio
        # per fer-ho cal passar-la a StringIO
        l = StringIO.StringIO(line)
        reader = csv.DictReader(l, fieldnames=self.headers_ps, delimiter=',')
        linia = reader.next() # nomes n'hi ha una

        parsed = {'ps': {}, 'orig': line, 'collection': self.collection}
        result, errors = self.adapter.load(linia)

        if errors:
            logger.error(errors)
        parsed['ps'] = result
        return parsed, errors

register(CnmcV3)

class CnmcV3Cons(Parser):

    # En el cas de les mesures, usem Schema per mantenir el format i
    # perque no hi trobarem mes comes que les delimiters
    pattern = '[0-9]{6}_SIPS2026_CONSUMOS_ELECTRICIDAD_[a-z]*_?[a-z]*\.csv'
    encoding = "UTF-8"
    collection = 'cnmc_sips_consums'
    collection_index = [("cups", pymongo.ASCENDING),("fechaFinMesConsumo", pymongo.DESCENDING)]
    index_unic = False


    def __init__(self, strict=False):
        self.schema = CnmcV3MeasuresSchema(strict=strict)
        self.adapter = CnmcV3MeasuresAdapter(strict=strict)
        self.measures_adapter = self.adapter
        self.fields = []
        self.headers = []
        for f in sorted(self.schema.fields,
                key=lambda f: self.schema.fields[f].metadata['position']):
            field = self.schema.fields[f]
            self.fields.append((f, field.metadata))
            self.headers.append(f)

    def parse_line(self, line):

        l = StringIO.StringIO(line)
        reader = csv.DictReader(l, fieldnames=self.headers, delimiter=',')
        linia = reader.next()  # nomes n'hi ha una

        parsed = {'ps': {}, 'measure_cnmc': [], 'orig': line, 'collection': self.collection}

        result, errors = self.adapter.load(linia)

        if errors:
            logger.error(errors)

        parsed['measure_cnmc'] = result

        return parsed, errors

register(CnmcV3Cons)

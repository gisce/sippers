from __future__ import absolute_import

import csv
import StringIO

import pymongo

from sippers import logger
from sippers.utils import build_dict
from sippers.parsers.parser import Parser, register
from sippers.adapters.cnmc_gas import CnmcGasSipsAdapter, CnmcGasMeasuresAdapter
from sippers.models.cnmc_gas import CnmcGasMeasuresSchema

class CnmcGas(Parser):

    # En aquest cas els llegim amb el csv.DictReader en comptes de fer-ho amb
    # un Schema de marshmallow, ja que el csv pot contenir comes dins d'un
    # camp si van wrapped entre cometes i el marshmallow aixo no ho contempla.

    # amb csv.DictReader obtindrem un diccionari amb els headers_ps
    # que li indiquem aqui, en comptes d'anar-los a buscar a l'Schema.
    # El diccionari el podem utilitzar per passar-li al
    # self.adapter.load igual que el resultat de l'esquema

    pattern = '[0-9]{6}_SIPS2_PS_GAS_[a-z]*_?[a-z]*\.csv'
    encoding = "UTF-8"
    collection = 'cnmc_sips_gas'
    collection_index = 'cups'
    index_unic = True

    def __init__(self, strict=False):

        # l'ordre dels camps segons format cnmc
        self.headers_ps = [
            'codigoEmpresaDistribuidora',
            'nombreEmpresaDistribuidora',
            'cups',
            'codigoProvinciaPS',
            'desProvinciaPS',
            'codigoPostalPS',
            'municipioPS',
            'desMunicipioPS',
            'tipoViaPS',
            'viaPS',
            'numFincaPS',
            'portalPS',
            'escaleraPS',
            'pisoPS',
            'puertaPS',
            'codigoPresion',
            'codigoPeajeEnVigor',
            'caudalMaximoDiarioEnWh',
            'caudalHorarioEnWh',
            'derechoTUR',
            'fechaUltimaInspeccion',
            'codigoResultadoInspeccion',
            'tipoPerfilConsumo',
            'codigoContador',
            'calibreContador',
            'tipoContador',
            'propiedadEquipoMedida',
            'codigoTelemedida',
            'fechaUltimoMovimientoContrato',
            'fechaUltimoCambioComercializador',
            'informacionImpagos',
            'idTipoTitular',
            'idTitular',
            'nombreTitular',
            'apellido1Titular',
            'apellido2Titular',
            'codigoProvinciaTitular',
            'desProvinciaTitular',
            'codigoPostalTitular',
            'municipioTitular',
            'desMunicipioTitular',
            'tipoViaTitular',
            'viaTitular',
            'numFincaTitular',
            'portalTitular',
            'escaleraTitular',
            'pisoTitular',
            'puertaTitular',
            'esViviendaHabitual',
            'cnae',
            'tipoCorrector',
            'codigoAccesibilidadContador',
            'conectadoPlantaSatelite',
            'pctd',
            'presionMedida'
        ]
        self.adapter = CnmcGasSipsAdapter(strict=strict)

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

register(CnmcGas)

class CnmcGasCons(Parser):

    # En el cas de les mesures, usem Schema per mantenir el format i
    # perque no hi trobarem mes comes que les delimiters
    pattern = '[0-9]{6}_SIPS2_CONSUMOS_GAS_[a-z]*_?[a-z]*\.csv'
    encoding = "UTF-8"
    collection = 'cnmc_sips_consums_gas'
    collection_index = [("cups", pymongo.ASCENDING), ("fechaFinMesConsumo", pymongo.DESCENDING)]
    index_unic = False


    def __init__(self, strict=False):
        self.schema = CnmcGasMeasuresSchema(strict=strict)
        self.adapter = CnmcGasMeasuresAdapter(strict=strict)
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

register(CnmcGasCons)

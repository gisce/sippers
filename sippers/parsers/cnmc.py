from __future__ import absolute_import

import csv
import StringIO

from sippers import logger
from sippers.utils import build_dict
from sippers.parsers.parser import Parser, register
from sippers.adapters.cnmc import CnmcSipsAdapter, CnmcMeasuresAdapter
from sippers.models.cnmc import CnmcMeasuresSchema

class Cnmc(Parser):

    # En aquest cas els llegim amb el csv.DictReader en comptes de fer-ho amb
    # un Schema de marshmallow, ja que el csv pot contenir comes dins d'un
    # camp si van wrapped entre cometes i el marshmallow aixo no ho contempla.

    # amb csv.DictReader obtindrem un diccionari amb els headers_ps
    # que li indiquem aqui, en comptes d'anar-los a buscar a l'Schema.
    # El diccionari el podem utilitzar per passar-li al
    # self.adapter.load igual que el resultat de l'esquema

    pattern = '[0-9]{4}-[0-9]{2}-[0-9]{2}_electricidad_sips.csv'
    encoding = "UTF-8"

    def __init__(self, strict=False):

        # l'ordre dels camps segons format cnmc
        self.headers_ps = [
                'cod_distri',
                'name',
                'codi_postal',
                'direccio',
                'poblacio',
                'data_alta',
                'tarifa',
                'tensio',
                'pot_max_bie',
                'pot_max_puesta',
                'tipo_pm',
                'indicatiu_icp',
                'perfil_consum',
                'der_extensio',
                'der_acces_llano', # la dada no sabem si es llano, valle o punta
                'propietat_equip_mesura',
                'propietat_icp',
                'potencies_contractades', # n periodes seguits separats per ';'
                'data_ulti_mov',
                'data_ult_canv',
                'data_lim_exten',
                'data_ult_lect',
                'impago',
                'fianza',
                'tipo_id_titular',
                'id_titular',
                'nom_complet',
                'direccio_titular',
                'primera_vivenda'
            ]
        self.adapter = CnmcSipsAdapter(strict=strict)

    def parse_line(self, line):

        # passar previament la linia pel csv reader
        # per que agafi be els camps tot i les comes dins del camp direccio
        # per fer-ho cal passar-la a StringIO
        l = StringIO.StringIO(line)
        reader = csv.DictReader(l, fieldnames=self.headers_ps, delimiter=',')
        linia = reader.next() # nomes n'hi ha una

        parsed = {'ps': {}, 'orig': line}
        result, errors = self.adapter.load(linia)

        if errors:
            logger.error(errors)
        parsed['ps'] = result
        return parsed, errors

register(Cnmc)

class CnmcCons(Parser):

    # En el cas de les mesures, usem Schema per mantenir el format i
    # perque no hi trobarem mes comes que les delimiters
    pattern = '[0-9]{4}-[0-9]{2}-[0-9]{2}_electricidad_consumos.csv'
    encoding = "UTF-8"
    delimiter = ','

    def __init__(self, strict=False):
        self.schema = CnmcMeasuresSchema(strict=strict)
        self.adapter = CnmcMeasuresAdapter(strict=strict)
        self.measures_adapter = self.adapter
        self.fields = []
        self.headers = []
        for f in sorted(self.schema.fields,
                key=lambda f: self.schema.fields[f].metadata['position']):
            field = self.schema.fields[f]
            self.fields.append((f, field.metadata))
            self.headers.append(f)

    def parse_line(self, line):
        slinia = tuple(line.split(self.delimiter))
        slinia = map(lambda s: s.strip(), slinia)
        parsed = {'ps': {}, 'measure_cnmc': [], 'orig': line}
        all_errors = {}
        consums = build_dict(self.headers, slinia)
        result, errors = self.adapter.load(consums)
        if errors:
            logger.error(errors)
            all_errors.update(errors)
        parsed['measure_cnmc'] = result

        return parsed, errors

register(CnmcCons)

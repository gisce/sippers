from __future__ import absolute_import
import copy

from sippers import logger
from sippers.utils import build_dict
from sippers.adapters.endesa import EndesaSipsAdapter
from sippers.models.endesa import EndesaSipsSchema
from sippers.parsers.parser import Parser, register


class Endesa(Parser):

    pattern = '(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF.SEG0[1-5].(zip|ZIP)'
    date_format = '%Y%m%d'
    encoding = "iso-8859-15"
    adapter = EndesaSipsAdapter()
    schema = EndesaSipsSchema()
    delimiter = ';'

    def __init__(self):
        super(Endesa, self).__init__()

        self.fields_ps = []
        self.headers_ps = []
        for f in sorted(self.schema.fields,
                key=lambda f: self.schema.fields[f].metadata['position']):
            field = self.schema.fields[f]
            self.fields_ps.append((f, field.metadata))
            self.headers_ps.append(f)

        self.fields = self.fields_ps

    def load_config(self):
        pass

    def parse_line(self, line):
        slinia = tuple(unicode(line.decode(self.encoding)).split(self.delimiter))
        slinia = map(lambda s: s.strip(), slinia)
        parsed = {'ps': {}, 'measures': {}}
        try:
            data = build_dict(self.headers_ps, slinia)
            result, errors = self.adapter.load(data)
            if errors:
                logger.error(errors)
            parsed['ps'] = result
            return parsed
        except Exception as e:
            logger.error("Row Error: %s: %s" % (str(e), line))


register(Endesa)


class EndesaCons(Parser):

    delimiter = ';'
    pattern = '(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF2.SEG0[1-5].(zip|ZIP)'
    num_fields = 39
    date_format = '%Y%m%d'
    descartar = []
    encoding = "iso-8859-15"

    def __init__(self):
        super(EndesaCons, self).__init__()
        self.pkeys = ['name', 'data_final']

        self.fields_name = [
            ('name', {'type': 'char', 'position': 0, 'magnituds': False}),
        ]
        self.fields_consums = [
            ('data_final', {'type': "datetime", "position": 1,
                            'magnituds': False}),
            ('real_estimada', {'type': "char", "position": 2,
                               'magnituds': False}),
            ('tipo_a1', {'type': "char", "position": 3, 'magnituds': False}),
            ('activa_1', {'type': 'float', 'position': 4, 'magnituds': 'kWh'}),
            ('tipo_a2', {'type': 'char', 'position': 5, 'magnituds': False}),
            ('activa_2', {'type': 'float', 'position': 6, 'magnituds': 'kWh'}),
            ('tipo_a3', {'type': 'char', 'position': 7, 'magnituds': False}),
            ('activa_3', {'type': 'float', 'position': 8, 'magnituds': 'kWh'}),
            ('tipo_a4', {'type': 'char', 'position': 9, 'magnituds': False}),
            ('activa_4', {'type': 'float', 'position': 10, 'magnituds': 'kWh'}),
            ('tipo_a5', {'type': 'char', 'position': 11, 'magnituds': False}),
            ('activa_5', {'type': 'float', 'position': 12, 'magnituds': 'kWh'}),
            ('tipo_a6', {'type': 'char', 'position': 13, 'magnituds': False}),
            ('activa_6', {'type': 'float', 'position': 14, 'magnituds': 'kWh'}),
            ('tipo_r1', {'type': "char", "position": 15, 'magnituds': False}),
            ('reactiva_1', {'type': 'float', 'position': 16,
                            'magnituds': 'kWh'}),
            ('tipo_r2', {'type': 'char', 'position': 17, 'magnituds': False}),
            ('reactiva_2', {'type': 'float', 'position': 18,
                            'magnituds': 'kWh'}),
            ('tipo_r3', {'type': 'char', 'position': 19, 'magnituds': False}),
            ('reactiva_3', {'type': 'float', 'position': 20,
                            'magnituds': 'kWh'}),
            ('tipo_r4', {'type': 'char', 'position': 21, 'magnituds': False}),
            ('reactiva_4', {'type': 'float', 'position': 22,
                            'magnituds': 'kWh'}),
            ('tipo_r5', {'type': 'char', 'position': 23, 'magnituds': False}),
            ('reactiva_5', {'type': 'float', 'position': 24,
                            'magnituds': 'kWh'}),
            ('tipo_r6', {'type': 'char', 'position': 25, 'magnituds': False}),
            ('reactiva_6', {'type': 'float', 'position': 26,
                            'magnituds': 'kWh'}),
            ('tipo_p1', {'type': "char", "position": 27, 'magnituds': False}),
            ('potencia_1', {'type': 'float', 'position': 28,
                            'magnituds': 'kWh'}),
            ('tipo_p2', {'type': 'char', 'position': 29, 'magnituds': False}),
            ('potencia_2', {'type': 'float', 'position': 30,
                            'magnituds': 'kWh'}),
            ('tipo_p3', {'type': 'char', 'position': 31, 'magnituds': False}),
            ('potencia_3', {'type': 'float', 'position': 32,
                            'magnituds': 'kWh'}),
            ('tipo_p4', {'type': 'char', 'position': 33, 'magnituds': False}),
            ('potencia_4', {'type': 'float', 'position': 34,
                            'magnituds': 'kWh'}),
            ('tipo_p5', {'type': 'char', 'position': 35, 'magnituds': False}),
            ('potencia_5', {'type': 'float', 'position': 36,
                            'magnituds': 'kWh'}),
            ('tipo_p6', {'type': 'char', 'position': 37, 'magnituds': False}),
            ('potencia_6', {'type': 'float', 'position': 38,
                            'magnituds': 'kWh'}),
        ]

        self.fields = self.fields_name + self.fields_consums

    def load_config(self):
        for field in self.fields:
            self.types.append(field[1]['type'])
            self.headers_conf.append(field[0])
            self.positions.append(field[1]['position'])
            self.magnitudes.append(field[1].get('magnituds'))

        self.data = self.prepare_data_set(self.fields, self.types,
                                          self.headers_conf,
                                          self.magnitudes)

    def parse_line(self, line):
        slinia = tuple(line.split(self.delimiter))
        slinia = map(lambda s: s.strip(), slinia)
        fixlist = slinia[0:len(self.fields_name)]

        parsed = {'ps': {}, 'measures': []}

        for plinia in range(len(fixlist), len(slinia),
                            len(self.fields_consums)):
            try:
                data = copy.deepcopy(self.data)
                # Llista dels valors del tros que agafem dins la linia
                part = slinia[plinia:(len(self.fields_consums)+plinia)]
                data.append(fixlist + part)
                for d in self.descartar:
                    del data[d]
                # Creo el diccionari per fer l'insert al mongo
                parsed['measures'].append(data.dict[0])

            except Exception as e:

                logger.error("Row Error: %s: %s" % (str(e), line))
        return parsed


register(EndesaCons)

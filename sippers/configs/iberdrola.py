from __future__ import absolute_import
import copy

from sippers import logger
from sippers.configs.parser import Parser, register


class Iberdrola(Parser):

    delimiter = 'ampfix'
    pattern = 'HGSBKA_(E0021_TXT[A-Z0-9]+\.(zip|ZIP)|TXT[A-Z0-9]+\.TXT)'
    num_fields = 00
    date_format = '%Y-%m-%d'
    descartar = ['any_sub', 'trimestre_sub']
    encoding = "iso-8859-15"

    def __init__(self):
        super(Iberdrola, self).__init__()
        self.pkeys = ['name', ]
        self.fields_ps = [
            ('name', {'type': 'char', 'position': 0, 'magnituds': False,
                      'collection': 'ps', 'length': 22}),
            ('direccio', {'type': "char", "position": 1, 'magnituds': False,
                          'collection': 'ps', 'length': 150}),
            ('poblacio', {'type': "char", "position": 2, 'magnituds': False,
                          'collection': 'ps', 'length': 45}),
            ('codi_postal', {'type': "char", "position": 3, 'magnituds': False,
                              'collection': 'ps', 'length': 10}),
            ('provincia', {'type': "char", "position": 4, 'magnituds': False,
                           'collection': 'ps', 'length': 45}),
            ('persona_fj', {'type': "boolean", "position": 5,
                            'magnituds': False, 'collection': 'ps',
                            'length': 2}),
            ('cognom', {'type': "char", "position": 6, 'magnituds': False,
                        'collection': 'ps', 'length': 50}),
            ('direccio_titular', {'type': "char", "position": 7,
                                  'magnituds': False, 'collection': 'ps',
                                  'length': 80}),
            ('municipi_titular', {'type': "char", "position": 8,
                                  'magnituds': False, 'collection': 'ps',
                                  'length': 45}),
            ('codi_postal_titular', {'type': "char", "position": 9,
                                     'magnituds': False, 'collection': 'ps',
                                     'length': 10}),
            ('provincia_titular', {'type': "char", "position": 10,
                                   'magnituds': False, 'collection': 'ps',
                                   'length': 45}),
            ('data_alta', {'type': "datetime", "position": 11,
                           'magnituds': False, 'collection': 'ps',
                           'length': 10}),
            ('tarifa', {'type': "char", "position": 12, 'magnituds': False,
                        'collection': 'ps', 'length': 3}),
            ('tensio', {'type': "interger", "position": 13, 'magnituds': False,
                        'collection': 'ps', 'length': 9}),
            ('pot_max_bie', {'type': "float", "position": 14,
                             'magnituds': 'kWh', 'collection': 'ps',
                             'length': 12}),
            ('pot_max_puesta', {'type': "float", "position": 15,
                                'magnituds': 'kWh', 'collection': 'ps',
                                'length': 12}),
            ('tipo_pm', {'type': "char", "position": 16, 'magnituds': False,
                         'collection': 'ps', 'length': 2}),
            ('indicatiu_icp', {'type': "boolean", "position": 17,
                               'magnituds': False, 'collection': 'ps',
                               'length': 1}),
            ('perfil_consum', {'type': "char", "position": 18,
                               'magnituds': False, 'collection': 'ps',
                               'length': 2}),
            ('der_acces_reconocido', {'type': "float", "position": 19,
                                      'magnituds': False, 'collection': 'ps',
                                      'length': 12}),
            ('der_extensio', {'type': "float", "position": 20,
                              'magnituds': False, 'collection': 'ps',
                              'length': 12}),
            ('propietat_equip_mesura',
             {'type': "boolean", "position": 21, 'magnituds': False,
              'collection': 'ps', 'length': 1}),
            ('propietat_icp', {'type': "boolean", "position": 22,
                               'magnituds': False, 'collection': 'ps',
                               'length': 1}),
            ('pot_cont_p1', {'type': "float", "position": 23,
                             'magnituds': "kWh", 'collection': 'ps',
                             'length': 12}),
            ('pot_cont_p2', {'type': "float", "position": 24,
                             'magnituds': "kWh", 'collection': 'ps',
                             'length': 12}),
            ('pot_cont_p3', {'type': "float", "position": 25,
                             'magnituds': "kWh", 'collection': 'ps',
                             'length': 12}),
            ('pot_cont_p4', {'type': "float", "position": 26,
                             'magnituds': "kWh", 'collection': 'ps',
                             'length': 12}),
            ('pot_cont_p5', {'type': "float", "position": 27,
                             'magnituds': "kWh", 'collection': 'ps',
                             'length': 12}),
            ('pot_cont_p6', {'type': "float", "position": 28,
                             'magnituds': "kWh", 'collection': 'ps',
                             'length': 12}),
            ('pot_cont_p7', {'type': "float", "position": 29,
                             'magnituds': "kWh", 'collection': 'ps',
                             'length': 12}),
            ('pot_cont_p8', {'type': "float", "position": 30,
                             'magnituds': "kWh", 'collection': 'ps',
                             'length': 12}),
            ('pot_cont_p9', {'type': "float", "position": 31,
                             'magnituds': "kWh", 'collection': 'ps',
                             'length': 12}),
            ('pot_cont_p10', {'type': "float", "position": 32,
                              'magnituds': "kWh", 'collection': 'ps',
                              'length': 12}),
            ('data_ult_canv', {'type': "datetime", "position": 33,
                               'magnituds': False, 'collection': 'ps',
                               'length': 10}),
            ('data_ulti_mov', {'type': "datetime", "position": 34,
                               'magnituds': False, 'collection': 'ps',
                               'length': 10}),
            ('data_lim_exten', {'type': "datetime", "position": 35,
                                'magnituds': False, 'collection': 'ps',
                                'length': 10}),
            ('data_ult_lect', {'type': "datetime", "position": 36,
                               'magnituds': False, 'collection': 'ps',
                               'length': 10}),
            ('pot_disp_caixa', {'type': "float", "position": 37,
                                'magnituds': False, 'collection': 'ps',
                                'length': 12}),
            ('impago', {'type': "float", "position": 38, 'magnituds': False,
                        'collection': 'ps', 'length': 11}),
            ('diposit_garantia', {'type': "boolean", "position": 39,
                                  'magnituds': False, 'collection': 'ps',
                                  'length': 1}),
            ('import_diposit', {'type': "float", "position": 40,
                                'magnituds': False, 'collection': 'ps',
                                'length': 11}),
            ('primera_vivenda', {'type': "boolean", "position": 41,
                                 'magnituds': False, 'collection': 'ps',
                                 'length': 1}),
            ('telegest_actiu', {'type': "char", "position": 42,
                                'magnituds': False, 'collection': 'ps',
                                'length': 2}),
            ('any_sub', {'type': "char", "position": 43, 'magnituds': False,
                         'collection': 'ps', 'length': 4}),
            ('trimestre_sub', {'type': "char", "position": 44,
                               'magnituds': False, 'collection': 'ps',
                               'length': 1}), ]
        self.fields_consums = [
            ('any_consum', {
                'type': "int",
                'length': 4
            }),
            ('facturacio_consum', {
                'type': "integer",
                'length': 4
            }),
            ('data_anterior', {
                'type': "datetime",
                'length': 10
            }),
            ('data_final', {
                'type': "datetime",
                'length': 10
            }),
            ('tarifa_consums', {
                'type': "char",
                'length': 4
            }),
            ('DH', {
                'type': "char",
                'length': 3
            }),
            ('activa_1', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('activa_2', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('activa_3', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('activa_4', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('activa_5', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('activa_6', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('activa_7', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('reactiva_1', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('reactiva_2', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('reactiva_3', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('reactiva_4', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('reactiva_5', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('reactiva_6', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('reactiva_7', {
                'type': "float",
                'magnituds': "kWh",
                'length': 14
            }),
            ('potencia_1', {
                'type': "float",
                'magnituds': "kWh",
                'length': 11
            }),
            ('potencia_2', {
                'type': "float",
                'magnituds': "kWh",
                'length': 11
            }),
            ('potencia_3', {
                'type': "float",
                'magnituds': "kWh",
                'length': 11
            }),
            ('potencia_4', {
                'type': "float",
                'magnituds': "kWh",
                'length': 11
            }),
            ('potencia_5', {
                'type': "float",
                'magnituds': "kWh",
                'length': 11
            }),
            ('potencia_6', {
                'type': "float",
                'magnituds': "kWh",
                'length': 11
            }),
            ('potencia_7', {
                'type': "float",
                'magnituds': "kWh",
                'length': 11
            })
        ]

        self.fields = self.fields_ps

    def slices(self, line, vals):
        # La llista amb les mides dels camps entren per els args.
        position = 0
        lvals = []
        for length in vals:
            lvals.append(line[position:position + length])
            position += length
        return lvals

    def load_config(self):
        for field in self.fields:
            self.types.append(field[1]['type'])
            self.headers_conf.append(field[0])
            self.positions.append(field[1]['position'])
            self.magnitudes.append(field[1]['magnituds'])
            self.vals_long.append(field[1]['length'])

        self.data = self.prepare_data_set(self.fields, self.types,
                                          self.headers_conf, self.magnitudes)
        tarifes = {
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
        self.data.add_formatter(
            'tarifa',
            lambda a: tarifes.get(a, None)
        )
        types = []
        headers_conf = []
        positions = []
        magnitudes = []

        for field in self.fields_consums:
            types.append(field[1]['type'])
            headers_conf.append(field[0])
            magnitudes.append(field[1].get('magnituds', False))
            self.vals_long.append(field[1]['length'])

        self.data_consums = self.prepare_data_set(self.fields_consums, types,
                                                  headers_conf, magnitudes)

    def parse_ps(self, line):
        slinia = tuple(self.slices(unicode(line), [x[1]['length'] for x in self.fields_ps]))
        slinia = map(lambda s: s.strip(), slinia)
        pslist = slinia[0:len(self.fields_ps)]
        data = copy.deepcopy(self.data)
        # Llista dels valors del tros que agafem dins dels sips
        try:
            data.append(pslist)
        except Exception, e:
            logger.error(e)
        for d in self.descartar:
            del data[d]
        # Creo el diccionari per fer l'insert al mongo
        return data.dict[0]

    def parse_measures(self, line):
        measures = []
        start = sum([x[1]['length'] for x in self.fields_ps])
        step = sum([x[1]['length'] for x in self.fields_consums])
        c_line = line[start:start+step].strip()
        length_c = [x[1]['length'] for x in self.fields_consums]
        while c_line:
            m = tuple(self.slices(c_line, length_c))
            m = map(lambda s: s.strip(), m)
            data_consums = copy.deepcopy(self.data_consums)
            data_consums.append(m)
            measures.append(data_consums.dict[0])
            start += step
            c_line = line[start:start + step].strip()
        return measures

    def parse_line(self, line):
        line = unicode(line.decode(self.encoding))
        parsed = {'ps': {}, 'measures': [], 'orig': line}
        try:
            parsed['ps'] = self.parse_ps(line)
        except Exception as e:
            logger.error("Row Error %s", e)
        try:
            parsed['measures'] = self.parse_measures(line)
        except Exception as e:
            logger.error("Row Error consums: %s", e)
        return parsed

register(Iberdrola)

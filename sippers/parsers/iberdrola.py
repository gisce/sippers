from __future__ import absolute_import

from sippers import logger
from sippers.utils import build_dict
from sippers.parsers.parser import Parser, register
from sippers.models.iberdrola import IberdrolaSipsSchema
from sippers.adapters.iberdrola import IberdrolaSipsAdapter


class Iberdrola(Parser):

    pattern = 'HGSBKA_(E0021_TXT[A-Z0-9]+\.(zip|ZIP)|TXT[A-Z0-9]+\.TXT)'
    date_format = '%Y-%m-%d'
    encoding = "iso-8859-15"
    ps_schema = IberdrolaSipsSchema()
    ps_adapter = IberdrolaSipsAdapter()

    def __init__(self):
        super(Iberdrola, self).__init__()

        self.fields_ps = []
        self.headers_ps = []
        self.slices_ps = []
        self.measures_start = 0
        for f in sorted(self.ps_schema.fields,
                key=lambda f: self.ps_schema.fields[f].metadata['position']):
            field = self.ps_schema.fields[f]
            self.fields_ps.append((f, field.metadata))
            self.headers_ps.append(f)
            self.slices_ps.append(field.metadata['length'])
            self.measures_start += field.metadata['length']

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

        self.headers_cons = []
        self.slices_measures = []
        self.measures_step = 0
        for field in self.fields_consums:
            self.headers_cons.append(field[0])
            self.measures_step += field[1]['length']
            self.slices_measures.append(field[1]['length'])

    def get_pos(self, field_name):
        start = 0
        stop = 0
        for field in self.fields_ps:
            stop = start + field[1]['length']
            if field[0] == field_name:
                break
            start += stop
        return start, stop

    def slices(self, line, vals):
        # La llista amb les mides dels camps entren per els args.
        position = 0
        lvals = []
        for length in vals:
            lvals.append(line[position:position + length])
            position += length
        return lvals

    def load_config(self):
        pass

    def parse_ps(self, line):
        slinia = tuple(self.slices(unicode(line), self.slices_ps))
        slinia = map(lambda s: s.strip(), slinia)
        pslist = slinia[0:len(self.fields_ps)]
        # Llista dels valors del tros que agafem dins dels sips
        try:
            data = build_dict(self.headers_ps, pslist)
            result, errors = self.ps_adapter.load(data)
            if errors:
                logger.error(errors)
            return result
        except Exception, e:
            logger.error(e)

    def parse_measures(self, line):
        measures = []
        start = self.measures_start
        step = self.measures_step
        c_line = line[start:start+step].strip()
        length_c = self.slices_measures
        i, j = self.get_pos('name')
        cups = line[i:j]
        while c_line:
            m = tuple(self.slices(c_line, length_c))
            m = map(lambda s: s.strip(), m)
            consums = build_dict(self.headers_cons, m)
            consums['name'] = cups
            measures.append(consums)
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

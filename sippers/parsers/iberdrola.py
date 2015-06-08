from __future__ import absolute_import

from sippers import logger
from sippers.utils import build_dict
from sippers.parsers.parser import Parser, register
from sippers.models.iberdrola import IberdrolaSipsSchema, IberdrolaMeasuresSchema
from sippers.adapters.iberdrola import IberdrolaSipsAdapter, IberdrolaMeasuresAdapter


class Iberdrola(Parser):

    pattern = 'HGSBKA_(E0021_TXT[A-Z0-9]+\.(zip|ZIP)|TXT[A-Z0-9]+\.TXT)$'
    encoding = "iso-8859-15"

    def __init__(self, strict=False):
        self.ps_schema = IberdrolaSipsSchema(strict=strict)
        self.ps_adapter = IberdrolaSipsAdapter(strict=strict)
        self.measures_schema = IberdrolaMeasuresSchema(strict=strict)
        self.measures_adapter = IberdrolaMeasuresAdapter(strict=strict)
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

        self.fields_consums = []
        self.headers_cons = []
        self.slices_measures = []
        self.measures_step = 0
        for f in sorted(self.measures_schema.fields,
                key=lambda f: self.measures_schema.fields[f].metadata['position']):
            field = self.measures_schema.fields[f]
            self.fields_consums.append((f, field.metadata))
            self.headers_cons.append(f)
            self.slices_measures.append(field.metadata['length'])
            self.measures_step += field.metadata['length']

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

    def parse_ps(self, line):
        slinia = tuple(self.slices(unicode(line), self.slices_ps))
        slinia = map(lambda s: s.strip(), slinia)
        pslist = slinia[0:len(self.fields_ps)]
        # Llista dels valors del tros que agafem dins dels sips
        data = build_dict(self.headers_ps, pslist)
        result, errors = self.ps_adapter.load(data)
        if errors:
            logger.error(errors)
        return result, errors

    def parse_measures(self, line):
        measures = []
        start = self.measures_start
        step = self.measures_step
        c_line = line[start:start+step].strip()
        length_c = self.slices_measures
        i, j = self.get_pos('name')
        cups = line[i:j].strip()
        all_errors = {}
        while c_line:
            m = tuple(self.slices(c_line, length_c))
            m = map(lambda s: s.strip(), m)
            consums = build_dict(self.headers_cons, m)
            consums['name'] = cups
            result, errors = self.measures_adapter.load(consums)
            if errors:
                logger.error(errors)
                all_errors.update(errors)
            measures.append(result)
            start += step
            c_line = line[start:start + step].strip()
        return measures, all_errors

    def parse_line(self, line):
        line = unicode(line.decode(self.encoding))
        all_errors = {}
        ps, ps_errors = self.parse_ps(line)
        measures, measures_errors = self.parse_measures(line)
        parsed = {'ps': ps,
                  'measures': measures, 'orig': line}
        if ps_errors:
            all_errors.update(ps_errors)
        if measures_errors:
            all_errors.update(measures_errors)
        return parsed, all_errors

register(Iberdrola)

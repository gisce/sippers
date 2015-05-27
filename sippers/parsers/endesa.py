from __future__ import absolute_import

from sippers import logger
from sippers.utils import build_dict
from sippers.adapters.endesa import EndesaSipsAdapter, EndesaMeasuresAdapter
from sippers.models.endesa import EndesaSipsSchema, EndesaMeasuresSchema
from sippers.parsers.parser import Parser, register


class Endesa(Parser):

    pattern = '(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF.SEG0[1-5].(zip|ZIP)'
    encoding = "iso-8859-15"
    adapter = EndesaSipsAdapter()
    schema = EndesaSipsSchema()
    delimiter = ';'

    def __init__(self):
        self.fields_ps = []
        self.headers_ps = []
        for f in sorted(self.schema.fields,
                key=lambda f: self.schema.fields[f].metadata['position']):
            field = self.schema.fields[f]
            self.fields_ps.append((f, field.metadata))
            self.headers_ps.append(f)

        self.fields = self.fields_ps

    def parse_line(self, line):
        slinia = tuple(unicode(line.decode(self.encoding)).split(self.delimiter))
        slinia = map(lambda s: s.strip(), slinia)
        parsed = {'ps': {}, 'measures': {}, 'orig': line}
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
    encoding = "iso-8859-15"
    schema = EndesaMeasuresSchema()
    adapter = EndesaMeasuresAdapter()

    def __init__(self):
        super(EndesaCons, self).__init__()
        self.fields = []
        self.headers = []
        for f in sorted(self.schema.fields,
                key=lambda f: self.schema.fields[f].metadata['position']):
            field = self.schema.fields[f]
            self.fields.append((f, field.metadata))
            self.headers.append(f)
        self.measures_start = 1
        self.measures_step = len(self.headers) - self.measures_start

    def parse_line(self, line):
        slinia = tuple(line.split(self.delimiter))
        slinia = map(lambda s: s.strip(), slinia)
        start = self.measures_start
        step = self.measures_step
        parsed = {'ps': {}, 'measures': [], 'orig': line}
        c_line = slinia[start:start+step]
        while c_line:
            c_line.insert(0, slinia[0])
            consums = build_dict(self.headers, c_line)
            result, errors = self.adapter.load(consums)
            if errors:
                logger.error(errors)
            parsed['measures'].append(result)
            start += step
            c_line = slinia[start:start+step]
        return parsed


register(EndesaCons)

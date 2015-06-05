from __future__ import absolute_import

from sippers import logger
from sippers.utils import build_dict
from sippers.parsers.parser import Parser, register
from sippers.models.hidrocantabrico import HidrocantabricoSchema, HidrocantabricoMeasuresSchema
from sippers.adapters.hidrocantabrico import HidrocantabricoSipsAdapter, HidrocantabricoMeasuresAdapter


class Hidrocantabrico(Parser):
    pattern = 'HIDROCANTABRICO_PS.*\.(zip|TXT)$'
    encoding = "iso-8859-15"

    def __init__(self, strict=False):
        self.ps_schema = HidrocantabricoSchema(strict=strict)
        self.ps_adapter = HidrocantabricoSipsAdapter(strict=strict)
        self.fields_ps = []
        self.headers_ps = []
        self.slices_ps = []
        for f in sorted(self.ps_schema.fields,
                key=lambda f: self.ps_schema.fields[f].metadata['position']):
            field = self.ps_schema.fields[f]
            self.fields_ps.append((f, field.metadata))
            self.headers_ps.append(f)
            self.slices_ps.append(field.metadata['length'])

    def slices(self, line, vals):
        # La llista amb les mides dels camps entren per els args.
        position = 0
        lvals = []
        for length in vals:
            lvals.append(line[position:position + length])
            position += length
        return lvals

    def parse_line(self, line):
        line = unicode(line.decode(self.encoding))
        slinia = tuple(self.slices(line, self.slices_ps))
        slinia = map(lambda s: s.strip(), slinia)
        pslist = slinia[0:len(self.fields_ps)]
        # Llista dels valors del tros que agafem dins dels sips
        data = build_dict(self.headers_ps, pslist)
        result, errors = self.ps_adapter.load(data)
        if errors:
            logger.error(errors)
        parsed = {'ps': result,
                  'measures': [], 'orig': line}
        return parsed, errors


register(Hidrocantabrico)


class HidrocantabricoMeasures(Parser):
    pattern = 'HIDROCANTABRICO_CO.*\.(zip|TXT)$'
    encoding = "iso-8859-15"

    def __init__(self, strict=False):
        self.schema = HidrocantabricoMeasuresSchema(strict=strict)
        self.adapter = HidrocantabricoMeasuresAdapter(strict=strict)
        self.measures_adapter = self.adapter
        self.fields = []
        self.headers = []
        self.measures_slices = []
        for f in sorted(self.schema.fields,
                key=lambda f: self.schema.fields[f].metadata['position']):
            field = self.schema.fields[f]
            self.fields.append((f, field.metadata))
            self.headers.append(f)
            self.measures_slices.append(field.metadata['length'])

    def slices(self, line, vals):
        # La llista amb les mides dels camps entren per els args.
        position = 0
        lvals = []
        for length in vals:
            lvals.append(line[position:position + length])
            position += length
        return lvals

    def parse_line(self, line):
        line = unicode(line.decode(self.encoding))
        slinia = tuple(self.slices(line, self.measures_slices))
        slinia = map(lambda s: s.strip(), slinia)
        values = slinia[0:len(self.fields)]
        # Llista dels valors del tros que agafem dins dels sips
        data = build_dict(self.headers, values)
        result, errors = self.adapter.load(data)
        if errors:
            logger.error(errors)
        parsed = {'ps': {},
                  'measures': [result], 'orig': line}
        return parsed, errors


register(HidrocantabricoMeasures)
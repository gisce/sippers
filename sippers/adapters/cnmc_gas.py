from sippers.adapters import SipsAdapter, MeasuresAdapter
from sippers.models.cnmc_gas import CnmcGasSipsSchema, CnmcGasMeasuresSchema
from marshmallow import pre_load, fields


class CnmcGasSipsAdapter(SipsAdapter, CnmcGasSipsSchema):
    '''A self.fields tenim els camps per defecte, els de SipsSchema base'''

    @pre_load
    def parse_selections(self, data):
        selection_fields = [
            'propiedadEquipoMedida',
            'codigoResultadoInspeccion',
            'tipoPerfilConsumo',
            'codigoPeajeEnVigor'
         ]
        for select_field in selection_fields:
            if data.get(select_field) == '':
                data[select_field] = None
        return data

    @pre_load
    def parse_propiedad_equipo_medida(self, data):
        if data.get('propiedadEquipoMedida') and len(data.get('propiedadEquipoMedida'))==1:
            data['propiedadEquipoMedida'] = '0' + data['propiedadEquipoMedida']
        return data

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                # si ve alguna data, assumim que ve correcta
                if data[attr] == u'':
                    data[attr] = None
                else:
                    data[attr] += 'T00:00:00'
        return data

class CnmcGasMeasuresAdapter(MeasuresAdapter, CnmcGasMeasuresSchema):

    @pre_load
    def parse_selections(self, data):
        selection_fields = [
            'codigoTarifaPeaje',
            'codigoTipoLectura'
        ]
        for select_field in selection_fields:
            if data.get(select_field) == '':
                data[select_field] = None
        return data

    @pre_load
    def fix_dates(self, data):
        for attr, field in self.fields.iteritems():
            if isinstance(field, fields.DateTime):
                # si ve alguna data, assumim que ve correcta
                if data[attr] == u'':
                    data[attr] = None
                else:
                    data[attr] += 'T00:00:00'
        return data
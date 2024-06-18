from sippers.adapters import SipsAdapter, MeasuresAdapter
from sippers.models.cnmc_v2 import CnmcV2SipsSchema, CnmcV2MeasuresSchema
from marshmallow import pre_load, fields


class CnmcV2SipsAdapter(SipsAdapter, CnmcV2SipsSchema):
    '''A self.fields tenim els camps per defecte, els de SipsSchema base'''

    @pre_load
    def add_distri_description(self, data):
        cod_distri = data.get('codigoEmpresaDistribuidora')
        if cod_distri == '059':
            data['nombreEmpresaDistribuidora'] = 'GRUPO ELECTRIFICACION RURAL BINEFAR'
        return data

    @pre_load
    def parse_selections(self, data):
        selection_fields = [
            'codigoTarifaATREnVigor',
            'codigoTipoSuministro',
            'codigoPropiedadICP',
            'codigoDHEquipoDeMedida',
            'codigoTensionMedida',
            'tipoIdTitular',
            'codigoClasificacionPS',
            'codigoTelegestion',
            'codigoTensionV',
            'codigoTipoContrato',
            'codigoPeriodicidadFacturacion',
            'codigoPropiedadEquipoMedida'
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

class CnmcV2MeasuresAdapter(MeasuresAdapter, CnmcV2MeasuresSchema):

    @pre_load
    def parse_selections(self, data):
        selection_fields = [
            'codigoTarifaATR',
            'codigoDHEquipoDeMedida',
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
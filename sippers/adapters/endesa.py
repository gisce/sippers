from sippers.adapters import Adapter
from sippers.models import SipsSchema
from marshmallow import pre_load


class EndesaSipsAdapter(Adapter, SipsSchema):

    @pre_load
    def fix_dates(self, data):
        for d in ('data_alta', 'data_ulti_mov', 'data_ult_canv', 'data_lim_exten'):
            orig = data[d]
            if orig != '0':
                data[d] = '{}-{}-{}'.format(orig[0:4], orig[4:6], orig[6:8])
            else:
                data[d] = None
        return data
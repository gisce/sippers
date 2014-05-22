from parser import Parser
from datetime import datetime

class Iberdrola(Parser):

    delimiter = 'ampfix'
    pattern = 'HGSBKA_E0021_TXT.\.(zip|ZIP)'
    num_fields = 00
    date_format = '%Y-%m-%d'
    descartar = ['any_sub', 'trimestre_sub']
    collection = None

    def __init__(self, mongodb=None):
        super(Parser, self).__init__()
        self.pkeys = ['name', ]
        self.fields_ps = [
            ('name', {'type': 'char', 'position': 0, 'magnituds': False,
                      'collection': 'ps', 'length': 22}),
            ('direccio', {'type': "char", "position": 1, 'magnituds': False,
                          'collection': 'ps', 'length': 150}),
            ('poblacio', {'type': "char", "position": 2, 'magnituds': False,
                          'collection': 'ps', 'length': 45}),
            ('codi_postal"', {'type': "char", "position": 3, 'magnituds': False,
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
            ('data_alta', {'type': "datetime", "position": 11, 'magnituds': "kWh",
                           'collection': 'ps', 'length': 10}),
            ('tarifa', {'type': "char", "position": 12, 'magnituds': "kWh",
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
                         'collection': 'ps', 'length': 2}),
            ('trimestre_sub', {'type': "char", "position": 44,
                               'magnituds': False, 'collection': 'ps',
                               'length': 1}), ]
        self.fields_consums = [
            ('any_consum', {'type': "datetime", "position": 45,
                            'magnituds': False, 'collection': 'consum',
                            'length': 4}),
            ('facturacio_consum', {'type': "integer", "position": 46,
                                   'magnituds': False, 'collection': 'consum',
                                   'length': 4}),
            ('data_anterior', {'type': "datetime", "position": 47,
                               'magnituds': False, 'collection': 'consum',
                               'length': 10}),
            ('data_final', {'type': "datetime", "position": 48,
                            'magnituds': False, 'collection': 'consum',
                            'length': 10}),
            ('tarifa_consums', {'type': "char", "position": 49,
                                'magnituds': False, 'collection': 'consum',
                                'length': 4}),
            ('DH', {'type': "char", "position": 50, 'magnituds': False,
                    'collection': 'consum', 'length': 3}),
            ('activa_1', {'type': "float", "position": 51, 'magnituds': "kWh",
                          'collection': 'consum', 'length': 14}),
            ('activa_2', {'type': "float", "position": 52, 'magnituds': "kWh",
                          'collection': 'consum', 'length': 14}),
            ('activa_3', {'type': "float", "position": 53, 'magnituds': "kWh",
                          'collection': 'consum', 'length': 14}),
            ('activa_4', {'type': "float", "position": 54, 'magnituds': "kWh",
                          'collection': 'consum', 'length': 14}),
            ('activa_5', {'type': "float", "position": 55, 'magnituds': "kWh",
                          'collection': 'consum', 'length': 14}),
            ('activa_6', {'type': "float", "position": 56, 'magnituds': "kWh",
                          'collection': 'consum', 'length': 14}),
            ('reactiva_1', {'type': "float", "position": 57, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 14}),
            ('reactiva_2', {'type': "float", "position": 58, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 14}),
            ('reactiva_3', {'type': "float", "position": 59, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 14}),
            ('reactiva_4', {'type': "float", "position": 60, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 14}),
            ('reactiva_5', {'type': "float", "position": 61, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 14}),
            ('reactiva_6', {'type': "float", "position": 62, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 14}),
            ('potencia_1', {'type': "float", "position": 63, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 11}),
            ('potencia_2', {'type': "float", "position": 64, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 11}),
            ('potencia_3', {'type': "float", "position": 65, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 11}),
            ('potencia_4', {'type': "float", "position": 66, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 11}),
            ('potencia_5', {'type': "float", "position": 67, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 11}),
            ('potencia_6', {'type': "float", "position": 68, 'magnituds': "kWh",
                            'collection': 'consum', 'length': 11}), ]

        self.fields = self.fields_ps + self.fields_consums

    def slices(self, *args):
        # La llista amb les mides dels camps entren per els args.
        position = 0
        for length in args:
            yield self[position:position + length]
            position += length

    def load_config(self):
        for field in self.fields:
            self.types.append(field[1]['type'])
            self.headers_conf.append(field[0])
            self.positions.append(field[1]['position'])
            self.magnitudes.append(field[1]['magnituds'])
            self.vals_long.append(field[1]['length'])

    def validate_mongo_counters(self):
        # Comprovo que la colletion estigui creada, si no la creo
        if not self.mongodb['counters'].find({"_id": "giscedata_sips_ps"}).count():
            self.mongodb['counters'].save({"_id": "giscedata_sips_ps",
                                           "counter": 1})
        if not self.mongodb['counters'].find({"_id": "giscedata_sips_consums"}).count():
            self.mongodb['counters'].save({"_id": "giscedata_sips_consum",
                                           "counter": 1})
        self.mongodb.eval("""db.giscedata_sips_ps.ensureIndex(
            {"name": 1})""")
        self.mongodb.eval("""db.giscedata_sips_consums.ensureIndex(
            {"name": 1})""")

    def prepare_mongo(self):
        self.collection = self.mongodb.giscedata_sips_ps

    def parse_line(self, line):
        slinia = tuple(self.slices(line, *self.vals_long))
        slinia = map(lambda s: s.strip(), slinia)

        # Inserto el SIPS
        pslist = slinia[0:len(self.fieldsps)]
        name = pslist[0]

        # Usuari del mongodb
        user = 'default'
        try:
            # Llista dels valors del tros que agafem dins dels sips
            self.data.append(pslist)
            for d in self.descartar:
                del self.data[d]
            # Creo el diccionari per fer l'insert al mongo
            document = self.data.dict[0]
            # Id incremental
            counter = self.mongodb['counters'].find_and_modify(
                {'_id': 'giscedata_sips_ps'},
                {'$inc': {'counter': 1}})
            # Update del index
            document.update(
                {'id': counter['counter'],
                 'create_uid': user,
                 'create_date': datetime.now()}
            )
            # Inserto el document al mongodb
            self.insert_mongo(document, self.collection)

            #Borrar els valors del tros
            self.data.wipe()
            #Torno a establir les headers
            self.data.headers = self.headers_conf
        except Exception as e:
            #Faig el wipe per no extendre l'error
            self.data.wipe()
            self.data.headers = self.headers_conf
            print "Row Error"

        for plinia in range(len(self.fieldsps), len(slinia),
                            len(self.fieldsconsums)):
            # Usuari del mongodb
            user = 'default'
            try:
                # Llista dels valors del tros que agafem dins la linia
                self.data.append(name + plinia)
                for d in self.descartar:
                    del self.data[d]
                # Creo el diccionari per fer l'insert al mongo
                document = self.data.dict[0]
                # Id incremental
                counter = self.mongodb['counters'].find_and_modify(
                    {'_id': 'giscedata_sips_consums'},
                    {'$inc': {'counter': 1}})
                # Update del index
                document.update(
                    {'id': counter['counter'],
                     'create_uid': user,
                     'create_date': datetime.now()}
                )
                # Inserto el document al mongodb
                self.insert_mongo(document, self.collection)

                #Borrar els valors del tros
                self.data.wipe()
                #Torno a establir les headers
                self.data.headers = self.headers_conf
            except Exception as e:
                #Faig el wipe per no extendre l'error
                self.data.wipe()
                self.data.headers = self.headers_conf
                print "Row Error"

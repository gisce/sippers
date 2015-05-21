from __future__ import absolute_import
from datetime import datetime
import copy

from sippers import logger
from sippers.configs.parser import Parser, register


class Endesa(Parser):

    delimiter = ';'
    pattern = '(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF.SEG0[1-5].(zip|ZIP)'
    num_fields = 43
    date_format = '%Y%m%d'
    descartar = ['facturacio', 'salt']
    collection = None

    def __init__(self, mongodb=None):
        super(Parser, self).__init__()
        self.pkeys = ['name', ]
        self.fields_ps = [
            ('name', {'type': 'char', 'position': 0, 'magnituds': False}),
            ('distri', {'type': "char", "position": 1, 'magnituds': False}),
            ('cod_distri', {'type': "char", "position": 2, 'magnituds': False}),
            ('direccio"', {'type': "char", "position": 3, 'magnituds': False}),
            ('poblacio', {'type': "char", "position": 4, 'magnituds': False}),
            ('codi_postal',
             {'type': "char", "position": 5, 'magnituds': False}),
            ('provincia', {'type': "char", "position": 6, 'magnituds': False}),
            ('data_alta',
             {'type': "datetime", "position": 7, 'magnituds': False}),
            ('tarifa', {'type': "char", "position": 8, 'magnituds': False}),
            ('des_tarifa', {'type': "char", "position": 9, 'magnituds': False}),
            ('tensio', {'type': "char", "position": 10, 'magnituds': False}),
            ('pot_max_bie',
             {'type': "float", "position": 11, 'magnituds': "kWh"}),
            ('pot_max_puesta',
             {'type': "float", "position": 12, 'magnituds': "kWh"}),
            ('tipo_pm', {'type': "char", "position": 13, 'magnituds': False}),
            ('indicatiu_icp',
             {'type': "boolean", "position": 14, 'magnituds': False}),
            ('perfil_consum',
             {'type': "char", "position": 15, 'magnituds': False}),
            ('der_extensio',
             {'type': "float", "position": 16, 'magnituds': "kWh"}),
            ('der_acces_llano',
             {'type': "float", "position": 17, 'magnituds': "kWh"}),
            ('der_acces_valle',
             {'type': "float", "position": 18, 'magnituds': "kWh"}),
            ('propietat_equip_mesura',
             {'type': "boolean", "position": 19, 'magnituds': False}),
            ('propietat_icp',
             {'type': "boolean", "position": 20, 'magnituds': False}),
            ('pot_cont_p1',
             {'type': "float", "position": 21, 'magnituds': "kWh"}),
            ('pot_cont_p2',
             {'type': "float", "position": 22, 'magnituds': "kWh"}),
            ('pot_cont_p3',
             {'type': "float", "position": 23, 'magnituds': "kWh"}),
            ('pot_cont_p4',
             {'type': "float", "position": 24, 'magnituds': "kWh"}),
            ('pot_cont_p5',
             {'type': "float", "position": 25, 'magnituds': "kWh"}),
            ('pot_cont_p6',
             {'type': "float", "position": 26, 'magnituds': "kWh"}),
            ('data_ulti_mov',
             {'type': "datetime", "position": 27, 'magnituds': False}),
            ('data_ult_canv',
             {'type': "datetime", "position": 28, 'magnituds': False}),
            ('data_lim_exten',
             {'type': "datetime", "position": 29, 'magnituds': False}),
            ('data_ult_lect',
             {'type': "datetime", "position": 30, 'magnituds': False}),
            ('talls', {'type': "integer", "position": 31, 'magnituds': False}),
            ('fianza', {'type': "integer", "position": 32, 'magnituds': False}),
            ('persona_fj',
             {'type': "boolean", "position": 33, 'magnituds': False}),
            ('nom', {'type': "char", "position": 34, 'magnituds': False}),
            ('cognom', {'type': "char", "position": 35, 'magnituds': False}),
            ('direccio_titular',
             {'type': "char", "position": 36, 'magnituds': False}),
            ('municipi_titular',
             {'type': "char", "position": 37, 'magnituds': False}),
            ('codi_postal_titular',
             {'type': "char", "position": 38, 'magnituds': False}),
            ('provincia_titular',
             {'type': "char", "position": 39, 'magnituds': False}),
            ('primera_vivenda',
             {'type': "boolean", "position": 40, 'magnituds': False}),
            ('facturacio',
             {'type': "char", "position": 41, 'magnituds': False}),
            ('salt', {'type': "char", "position": 42, 'magnituds': False}), ]

        self.fields = self.fields_ps

    def load_config(self):
        for field in self.fields:
            self.types.append(field[1]['type'])
            self.headers_conf.append(field[0])
            self.positions.append(field[1]['position'])
            self.magnitudes.append(field[1]['magnituds'])

    def validate_mongo_counters(self):
        # Comprovo que la colletion estigui creada, si no la creo
        if not self.mongodb['counters'].find(
                {"_id": "giscedata_sips_ps"}).count():
            self.mongodb['counters'].save(
                {"_id": "giscedata_sips_ps", "counter": 1})
            self.mongodb.eval(
                'db.giscedata_sips_ps.createIndex({"name": 1})')

    def prepare_mongo(self):
        self.collection = self.mongodb.giscedata_sips_ps

    def parse_line(self, line):
        slinia = tuple(line.split(self.delimiter))
        slinia = map(lambda s: s.strip(), slinia)

        # Usuari del mongodb
        user = 'default'
        try:
            data = copy.deepcopy(self.data)
            # Llista dels valors del tros que agafem dins la linia
            data.append(slinia)
            if self.num_fields and len(slinia) != int(self.num_fields):
                logger.error("Row lenght incorrect")
            for d in self.descartar:
                del data[d]
            # Creo el diccionari per fer l'insert al mongo
            document = data.dict[0]
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

            # #Borrar els valors del tros
            # self.data.wipe()
            # #Torno a establir les headers
            # self.data.headers = self.headers_conf
        except Exception as e:
            # #Faig el wipe per no extendre l'error
            # self.data.wipe()
            # self.data.headers = self.headers_conf
            logger.error("Row Error: %s: %s" % (str(e), line))


register(Endesa)


class EndesaCons(Parser):

    delimiter = ';'
    pattern = '(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF2.SEG0[1-5].(zip|ZIP)'
    num_fields = 39
    date_format = '%Y%m%d'
    descartar = []
    collection = None

    def __init__(self, mongodb=None):
        super(Parser, self).__init__()
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
            self.magnitudes.append(field[1]['magnituds'])

    def validate_mongo_counters(self):
        # Comprovo que la colletion estigui creada, si no la creo
        if not self.mongodb['counters'].find(
                {"_id": "giscedata_sips_consums"}).count():
            self.mongodb['counters'].save(
                {"_id": "giscedata_sips_consums", "counter": 1})
            self.mongodb.eval(
                'db.giscedata_sips_consums.createIndex({"name": 1})')

    def prepare_mongo(self):
        self.collection = self.mongodb.giscedata_sips_consums

    def parse_line(self, line):
        slinia = tuple(line.split(self.delimiter))
        slinia = map(lambda s: s.strip(), slinia)
        fixlist = slinia[0:len(self.fields_name)]

        for plinia in range(len(fixlist), len(slinia),
                            len(self.fields_consums)):
            # Usuari del mongodb
            user = 'default'
            try:
                data = copy.deepcopy(self.data)
                # Llista dels valors del tros que agafem dins la linia
                part = slinia[plinia:(len(self.fields_consums)+plinia)]
                data.append(fixlist + part)
                if (self.num_fields and len(fixlist + part) != int(
                        self.num_fields)):
                    logger.error("Row lenght incorrect")
                for d in self.descartar:
                    del data[d]
                # Creo el diccionari per fer l'insert al mongo
                document = data.dict[0]
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

                # #Borrar els valors del tros
                # self.data.wipe()
                # #Torno a establir les headers
                # self.data.headers = self.headers_conf
            except Exception as e:
                # #Faig el wipe per no extendre l'error
                # self.data.wipe()
                # self.data.headers = self.headers_conf
                logger.error("Row Error: %s: %s" % (str(e), line))


register(EndesaCons)

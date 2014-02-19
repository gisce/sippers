# -*- coding: utf-8 -*-

import os
import zipfile
import sys
import tablib
import codecs
from pymongo import MongoClient
import ConfigParser
import re
# from rq.decorators import job
#
# from redis import Redis
# conn = Redis()
#
# @job(queue='sips', connection=conn)
# def test_redis(documents, db):
#     pass

class Parsejador():

    def extract_file(self, tail, head):
        arxiuex = open(head + '/' + tail, 'rb')
        z = zipfile.ZipFile(arxiuex)
        for name in z.namelist():
            outpath = head + '/'
            z.extract(name, outpath)
        arxiuex.close()
        return {}

    def extractzips(self, path):
        head, tail = os.path.split(path)
        self.extract_file(tail, head)
        for fitxer in os.listdir("./"):
            self.detecta_conf(head + '/' + fitxer)
        return {}

    def detecta_conf(self, arxiu):
        # Per ara suposo que tots son de ENDESA
        head, tail = os.path.split(arxiu)
        #Afago el tipus segons el nom del fitxer
        #tipus = tail.split('.')[-3]
        headers = ''
        descartar = ''
        rutacompleta = ''
        for fitxer in os.listdir("configs"):
            conf = ConfigParser.RawConfigParser()
            conf.readfp(open("configs/"+fitxer))
            pattern = conf.get('parser', 'pattern')
            if re.match(pattern, tail):
                # Camps del header
                headers = conf.options('fields')
                # Camps per borrar del header
                descartar = conf.options('descartar')
                # Extreure el fitxer
                self.extract_file(tail, head)
                # Tinc de buscar el fitxer extret
                pathex = conf.get('parser', 'pathex')
                for fitex in os.listdir(head + '/' + pathex):
                    rutacompleta = head + '/' + pathex + fitex

        if not headers or not descartar:
            print "Error, no s'ha trobat cap coincidencia amb els fitxers de " \
                  "configuració"
        return headers, descartar, rutacompleta

    def insert_mongo(self, documents, collection):
        # Afegeixo les entrades
        collection.insert(documents)
        return {}

    def parser_sips_endesa(self, arxiu, headerss, descartar):
        filecodificat = codecs.open(arxiu, "r", "iso-8859-15")
        midafitxer = os.stat(arxiu).st_size
        #print 'Mida del fitxer: {}'.format(midafitxer)

        # headerss = ('cups', 'distri', 'cod_distri', 'direccio_suministre',
        #             'localitat', 'codi_postal', 'provincia', 'data_alta',
        #             'tarifa', 'des_tarifa', 'tensio', 'pot_max_bie',
        #             'pot_max_puesta', 'tipo_pm', 'indicatiu_icp',
        #             'perfil_consum', 'der_extensio', 'der_acces_llano',
        #             'der_acces_valle', 'propietat_equip_mesura',
        #             'propietat_icp', 'pot_cont_p1', 'pot_cont_p2',
        #             'pot_cont_p3', 'pot_cont_p4', 'pot_cont_p5', 'pot_cont_p6',
        #             'data_ulti_mov', 'data_ult_camb_comer', 'data_lim_exten',
        #             'data_ult_lect', 'talls', 'fianza', 'persona_fj', 'nom',
        #             'cognom', 'direccio_titular', 'municipi_titular',
        #             'codi_postal_titular', 'provincia_titular',
        #             'primera_vivenda', 'facturacio', 'salt')

        sumatori = 0
        part = []
        count = 0
        # Número de camps del header
        len_header = str(len(headerss))

        # Log per els errors de lectura
        flog = open("log.txt", "w")

        # Connectar i escollir la bbdd
        client = MongoClient()
        db = client.test_database
        # Agafo la colecció
        sips = db.giscedata_sips_ps

        #Add incremental id to store vals
        counter = db['counters'].find_and_modify(
            {'_id': db.giscedata_sips_ps},
            {'$inc': {'counter': 1}})

        # counter = db.get_collection('counters').find_and_modify(
        #     {'_id': db.giscedata_sips_ps},
        #     {'$inc': {'counter': 1}})

        while filecodificat.tell() < midafitxer:

            #import pdb; pdb.set_trace()
            linia = filecodificat.readline()
            slinia = tuple(linia.split(';'))
            slinia = map(lambda s: s.strip(), slinia)
            #print 'length slinia:{} length headers:{}'.format(len(slinia),
            #                                                  len(headerss))

            # Comprovar que el número de camps es el mateix que la del header
            if len(slinia) != int(len_header):
                flog.write('\nError: ' + 'length:' + str(len(slinia)) +
                           'Correcte :' + len_header + '\n' +
                           'numero de linia: ' + str(count) + "\n")
            else:
                part.append(slinia)

            if count % 100:
                if part:
                    data = tablib.Dataset(*part, headers=headerss)

                    # Borro les claus que em surt l'arxiu de configuracio
                    for d in descartar:
                        del data[d]

                    document = data.dict

                    # Update del index
                    document.update({'id': counter['counter']})
                    # Inserto el document al mongodb
                    self.insert_mongo(document, sips)

                    # Borro la llista dels 11+1 elements
                    del part[:]

            count += 1
            sumatori += len(linia)
            tantpercent = float(sumatori) / midafitxer * 100.0
            print "Completat: {} %".format(tantpercent)

        # Per les entrades que queden
        if part:
            data = tablib.Dataset(*part, headers=headerss)
            document = data.dict
            self.insert_mongo(document, sips)

        #print data.json
        filecodificat.close()
        flog.close()
        return {}

    def __init__(self):
        ####
        # Parser del fitxer de SIPS de Endesa
        ####
        #arxiu = "/home/pau/Documents/sips/20131217 Endesa.zip"
        #arxiu = "/home/pau/Documents/sips/SCEF/EXPT/" \
        #        "INFPSCOM/INF/SEG01/SORT/S131213_backup"
        arxiu = "/home/pau/Documents/sips/prova/FECSA.INF.SEG01.ZIP"
        #Descomprimir el zip arrel i processar tots els fitxer - WIP -
        #self.extractzips(arxiu)

        #Detectar tipus d'arxiu
        header, descartar, rutacompleta = self.detecta_conf(arxiu)

        #Parsejar l'arxiu
        self.parser_sips_endesa(rutacompleta, header, descartar)


# -*- coding: utf-8 -*-

import os
import zipfile
import sys
import tablib
import codecs
from pymongo import MongoClient
import ConfigParser
import re
from datetime import datetime

# from rq.decorators import job
#
# from redis import Redis
# conn = Redis()
#
# @job(queue='sips', connection=conn)
# def test_redis(documents, db):
#     pass

class Parsejador(object):

    def extract_file(self, tail, head):
        arxiuex = open(head + '/' + tail, 'rb')
        z = zipfile.ZipFile(arxiuex)
        for name in z.namelist():
            outpath = head + '/'
            z.extract(name, outpath)
        arxiuex.close()
        return {}

    # def extractzips(self, path):
    #     head, tail = os.path.split(path)
    #     self.extract_file(tail, head)
    #
    #     for fitxer in os.listdir("./"):
    #         self.detecta_conf(head + '/' + fitxer)
    #     return {}

    def afagaarxius(self, path):
        llista_arxius = []
        for fitxer in os.listdir(path):
            fparts = fitxer.split(".")
            #print "fparts {}".format(fparts)
            if fparts[-1] == 'ZIP':
                llista_arxius.append(fitxer)
        return llista_arxius

    def detecta_conf(self, arxiu, directori, numseg):
        # Per ara suposo que tots son de ENDESA
        head, tail = os.path.split(arxiu)
        #Afago el tipus segons el nom del fitxer
        #tipus = tail.split('.')[-3]
        headers = ''
        delimiter = ''
        classe = ''
        dictres = {}
        print os.listdir("configs")
        for fitxer in os.listdir("configs"):
            conf = ConfigParser.RawConfigParser()
            conf.readfp(open("configs/"+fitxer))
            pattern = conf.get('parser', 'pattern')
            delimiter = conf.get('parser', 'delimiter')
            classe = conf.get('parser', 'class')

            if re.match(pattern, tail):
                print "pattern {}".format(pattern)
                # Camps del header
                headers = conf.items('fields')

                #print "read headers:{}".format(conf.items('fields'))

                # Camps per borrar del header
                descartar = conf.options('descartar')

                #print "headers: {}, descartar: {}".format(headers, descartar)
                # Extreure el fitxer
                #print "head:{}, tail:{}, directori:{}".format(head, tail,
                #                                              directori)
                self.extract_file(tail, directori)
                # Tinc de buscar el fitxer extret
                pathex = conf.get('parser', 'pathex')

                for fitex in os.listdir(directori + pathex + numseg + '/SORT/'):
                    rutacompleta = directori + pathex + numseg + '/SORT/' + fitex
                    print "rutacompleta:{}".format(rutacompleta)
                    dictres[rutacompleta] = [headers, descartar, delimiter,
                                             classe]
            else:
                print "no fa match"
        if not headers:
            print "Error, no s'ha trobat cap coincidencia amb els fitxers de " \
                  "configuració"
        return dictres

    def insert_mongo(self, documents, collection):
        # Afegeixo les entrades
        collection.insert(documents)
        return {}

    def importamongo_ps(self, filecodificat, midafitxer, collection, contador,
                        db, valors):

        # Log per els errors de lectura
        flog = open("log_ps.txt", "w")
        # Camps del conf
        headerss = valors[0]
        headers_conf = [h[0] for h in headerss]

        descartar = valors[1]
        delimiter = valors[2]
        classe = valors[3]
        # Contador de linies
        count = 0
        # Per calcular la progressió
        sumatori = 0
        #USUARI DE TEST QUE FEM SERVIR
        user = 'default'
        # Número de camps del header
        len_header = str(len(headerss))

        # Comprovo que la collecció estigui creada, si no la creo
        if not db['counters'].count():
            db['counters'].save({"_id": "giscedata_sips_ps",
                                 "counter": 1})

        while filecodificat.tell() < midafitxer:

            #import pdb; pdb.set_trace()
            linia = filecodificat.readline()
            slinia = tuple(linia.split(delimiter))
            slinia = map(lambda s: s.strip(), slinia)

            # print 'length slinia:{} length headers:{}'.format(len(slinia),
            #                                                   len(headerss))
            #print 'db:{}'.format(db)
            #Add incremental id to store vals
            counter = db['counters'].find_and_modify(
                {'_id': classe},
                {'$inc': {'counter': 1}})
            #print 'counter ps: {}'.format(counter)

            # Comprovar que el número de camps es el mateix que la del header
            if len(slinia) != int(len_header):
                flog.write('\nError: ' + 'length:' + str(len(slinia)) +
                           'Correcte :' + len_header + '\n' +
                           'numero de linia: ' + str(count) + "\n")
            else:
                #print slinia
                data = tablib.Dataset(slinia, headers=headers_conf)

                # Borro les claus que em surt l'arxiu de configuracio
                for d in descartar:
                    del data[d]

                # Millores: posar la cadena de lambda al fitxer de conf
                for h in headerss:
                    if h[1] == 'float':
                        data.add_formatter(h[0], lambda a: a and float(a) or 0)
                    if h[1] == 'integer':
                        print "h[1]:{}, h[0]:{}".format(h[1], h[0])
                        data.add_formatter(h[0],
                                           lambda a:
                                           a and int(a.replace(',', '')) or 0)
                    # TO DO : afegir format datetime
                    #         camp per boolean
                    if h[1] == 'long':
                        data.add_formatter(h[0], lambda a: a and long(a) or 0)

                document = data.dict[0]

                # Update del index
                #print counter
                document.update(
                    {'id': counter['counter'],
                     'create_uid': user,
                     'create_date': datetime.now()}
                )

                # Inserto el document al mongodb
                self.insert_mongo(document, collection)

            count += 1
            sumatori += len(linia)
            tantpercent = float(sumatori) / midafitxer * 100.0
            print "Completat: {} %".format(tantpercent)

        print "Numero de linies: {}".format(count)
        flog.close()

        return {}

    def importamongo_consums(self, filecodificat, midafitxer, collection,
                             contador, db, valors):

        # Log per els errors de lectura
        flog = open("log_consums.txt", "w")
        # Camps del conf
        headerss = valors[0]
        headers_conf = [h[0] for h in headerss]

        descartar = valors[1]
        delimiter = valors[2]
        classe = valors[3]
        # Contador de linies
        count = 0
        # Per calcular la progressió
        sumatori = 0
        #USUARI DE TEST QUE FEM SERVIR
        user = 'default'
        # Número de camps del header
        len_header = str(len(headerss))

        # Comprovo que la collecció estigui creada, si no la creo
        if not db['counters_con'].count():
            db['counters_con'].save({"_id": "giscedata_sips_consums",
                                     "counter": 1})

        while filecodificat.tell() < midafitxer:

            #import pdb; pdb.set_trace()
            linia = filecodificat.readline()
            slinia = tuple(linia.split(delimiter))
            slinia = map(lambda s: s.strip(), slinia)

            #print "cups: {}".format(slinia[0])
            cups = slinia[0]

            # print 'length slinia:{} length headers:{}'.format(len(slinia),
            #                                                   len(headerss))

            # Menys el primer element (cups) amb la long del header menys 1
            for i in range(1, len(slinia), int(len_header)-1):
                data = slinia[i:i+int(len_header)-1]
                data.insert(0, cups)

                #print "data: {}, len_data: {}, len_headers: {}".format(
                #    data, len(data), len(headerss))
                data = tablib.Dataset(data, headers=headers_conf)

                # Borro les claus que em surt l'arxiu de configuracio
                for d in descartar:
                    del data[d]

                # Millores: posar la cadena de lambda al fitxer de conf
                for h in headerss:
                    if h[1] == 'float':
                        data.add_formatter(h[0], lambda a: a and float(a) or 0)
                    if h[1] == 'integer':
                        data.add_formatter(h[0], lambda a: a and int(a) or 0)
                    # TO DO : afegir format datetime
                    # if h[1] == 'datetime':
                    #     data.add_formatter(
                    #         h[0], lambda a: a and datetime.strptime
                    # (a, '%Y%m%d')
                    #         or None)
                    if h[1] == 'long':
                        data.add_formatter(h[0], lambda a: a and long(a) or 0)

                document = data.dict[0]

                #id incremental
                counter = db['counters_con'].find_and_modify(
                    {'_id': classe},
                    {'$inc': {'counter': 1}})

                # Update del index
                #print counter
                document.update(
                    {'id': counter['counter'],
                     'create_uid': user,
                     'create_date': datetime.now()}
                )

                # Inserto el document al mongodb
                self.insert_mongo(document, collection)

            # Comprovar que el número de camps es el mateix que la del header
            if len(slinia) != ((int(len_header)-1)*12)+1:
                flog.write('\nError: ' + 'length:' + str(len(slinia)) +
                           ' Correcte :' + str(int(len_header)*12) + '\n' +
                           'numero de linia: ' + str(count) + "\n")

            count += 1
            sumatori += len(linia)
            tantpercent = float(sumatori) / midafitxer * 100.0
            print "Completat: {} %".format(tantpercent)

        print "Numero de linies: {}".format(count)
        flog.close()
        return {}

    def parser_sips_endesa(self, arxiu_conf):

        arxiu, valors = arxiu_conf.popitem()

        print arxiu
        filecodificat = codecs.open(arxiu, "r", "iso-8859-15")
        midafitxer = os.stat(arxiu).st_size
        print 'Mida del fitxer: {}'.format(midafitxer)

        # Connectar i escollir la bbdd
        client = MongoClient()
        # Base de dades
        db = client.openerp
        #db = client.test_database

        # Segons el model del config
        classe = valors[3]
        if classe == 'giscedata_sips_ps':
            collection = db.giscedata_sips_ps
            contador = 'counters'
            self.importamongo_ps(filecodificat, midafitxer, collection,
                                 contador, db, valors)

        elif classe == 'giscedata_sips_consums':
            collection = db.giscedata_sips_consums
            contador = 'counters_con'
            self.importamongo_consums(filecodificat, midafitxer, collection,
                                      contador, db, valors)
        else:
            print "Error"

        #print data.json
        filecodificat.close()

        return {}

    def __init__(self):
        ####
        # Parser del fitxer de SIPS de Endesa
        ####
        #arxiu = "/home/pau/Documents/sips/20131217 Endesa.zip"
        #arxiu = "/home/pau/Documents/sips/SCEF/EXPT/" \
        #        "INFPSCOM/INF/SEG01/SORT/S131213_backup"

        #arxiu = "/home/pau/Documents/sips/prova/FECSA.INF.SEG01.ZIP"

        #Descomprimir el zip arrel i processar tots els fitxer - WIP -
        #self.extractzips(arxiu)

        #Afagar els arxius de un directori
        directori = "/home/pau/Documents/sips/prova"
        llista_arxius = self.afagaarxius(directori)
        print "llista_arxius: {}".format(llista_arxius)
        for arxiu in llista_arxius:
            print "Arxiu: {}".format(arxiu)
            fparts = arxiu.split(".")
            numseg = fparts[-2]
            arxiu_conf = self.detecta_conf(arxiu, directori, numseg)
            #print arxiu_conf
            self.parser_sips_endesa(arxiu_conf)

        #Detectar tipus d'arxiu
        #header, descartar, rutacompleta = self.detecta_conf(arxiu)

        #Parsejar l'arxiu
        #self.parser_sips_endesa(rutacompleta, header, descartar)


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
            contador = conf.get('parser', 'contador')

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
                                             classe, contador]
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

    def importamongo(self, filecodificat, midafitxer, db, valors):
        # Log per els errors de lectura
        flog = open("log.txt", "w")
        # Camps del conf
        headerss = valors[0]
        headers_conf = [h[0] for h in headerss]
        valores = [h[1] for h in headerss]
        vals = [v.split() for v in valores]
        #Només utilitzare aquests valors en cas de que hi hagin posicions
        if len(vals[0]) == 2:
            vals_tipus = [v[0] for v in vals]
            vals_apa = [v[1] for v in vals]

        descartar = valors[1]
        delimiter = valors[2]
        classe = valors[3]
        contador = valors[4]

        # Contador de linies
        count = 0
        # Per calcular la progressió
        sumatori = 0
        #USUARI DE TEST QUE FEM SERVIR
        user = 'default'
        # Número de camps del header
        len_header = str(len(headerss))

        # Afago la coleccio que vull
        if classe == 'giscedata_sips_ps':
            collection = db.giscedata_sips_ps
        elif classe == 'giscedata_sips_consums':
            collection = db.giscedata_sips_consums
        else:
            print "Error, no es troba la collection al conf"

        # Comprovo que la collecció estigui creada, si no la creo
        if not db[contador].count():
            db[contador].save({"_id": classe, "counter": 1})

        # llegeixo per tot el fitxer
        while filecodificat.tell() < midafitxer:
            linia = filecodificat.readline()
            slinia = tuple(linia.split(delimiter))
            slinia = map(lambda s: s.strip(), slinia)

            # Comprovar que el número de camps es el mateix que la del header
            if len(slinia) == int(len_header):
                # creo el dataset amb els headers del conf
                data = tablib.Dataset(slinia, headers=headers_conf)
                # Borro les claus que em surt l'arxiu de configuracio
                for d in descartar:
                    del data[d]
                # Millores: posar la cadena de lambda al fitxer de conf
                for h in headerss:
                    if h[1] == 'float':
                        data.add_formatter(h[0], lambda a: a and float(a) or 0)
                    if h[1] == 'integer':
                        #print "h[1]:{}, h[0]:{}".format(h[1], h[0])
                        data.add_formatter(h[0],
                                           lambda a:
                                           a and int(a.replace(',', '')) or 0)
                    # TO DO : afegir format datetime
                    #         camp per boolean
                    if h[1] == 'long':
                        data.add_formatter(h[0], lambda a: a and long(a) or 0)
                # creo el dict anomenat document per insertar al mongo
                document = data.dict[0]

                #Add incremental id to store vals
                counter = db[contador].find_and_modify(
                    {'_id': classe},
                    {'$inc': {'counter': 1}})

                # Update del index
                document.update(
                    {'id': counter['counter'],
                     'create_uid': user,
                     'create_date': datetime.now()}
                )

                self.insert_mongo(document, collection)
            elif len(slinia) != int(len_header) and len(vals[0]) < 2:
                print "Error en la dimensió de la linea, no coincideix amb" \
                      " la dimensió dels camps del fitxer de configuració."
                print "long de slinia: {} long de headerss: {}".format(
                    len(slinia), len(headerss)
                )
                print "slinia:{}".format(slinia)
                print "headerss:{}".format(headerss)
            else:
                contadorlinia = 0
                position = [eval(num, {"n": contadorlinia}) for num in vals_apa]

                for i in range(0, len(slinia), len(position)):
                    data = [slinia[p] for p in position]
                    data = tablib.Dataset(data, headers=headers_conf)

                    # Borro les claus que em surt l'arxiu de configuracio
                    for d in descartar:
                        del data[d]

                    # Millores: posar la cadena de lambda al fitxer de conf
                    for h, v in zip(headerss, vals_tipus):
                        if v == 'float':
                            data.add_formatter(h[0],
                                               lambda a: a and float(a) or 0)
                        if v == 'integer':
                            data.add_formatter(h[0],
                                               lambda a: a and int(a) or 0)
                        # TO DO : afegir format datetime
                        if v == 'long':
                            data.add_formatter(h[0],
                                               lambda a: a and long(a) or 0)

                    document = data.dict[0]
                    #id incremental
                    counter = db[contador].find_and_modify(
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

                    contadorlinia += 1
                    position = [eval(num, {"n": contadorlinia}) for num in
                                vals_apa]

            count += 1
            sumatori += len(linia)
            tantpercent = float(sumatori) / midafitxer * 100.0
            print "Completat: {} %".format(tantpercent)

        print "Numero de linies: {}".format(count)
        flog.close()

        return {}

    def parser_sips_endesa(self, arxiu_conf):

        arxiu, valors = arxiu_conf.popitem()

        #print arxiu
        filecodificat = codecs.open(arxiu, "r", "iso-8859-15")
        midafitxer = os.stat(arxiu).st_size
        #print 'Mida del fitxer: {}'.format(midafitxer)

        # Connectar i escollir la bbdd
        client = MongoClient()
        # Base de dades
        db = client.openerp
        #db = client.test_database

        self.importamongo(filecodificat, midafitxer, db, valors)

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
            #print "Arxiu: {}".format(arxiu)
            fparts = arxiu.split(".")
            numseg = fparts[-2]
            arxiu_conf = self.detecta_conf(arxiu, directori, numseg)
            #print arxiu_conf
            self.parser_sips_endesa(arxiu_conf)

        #Detectar tipus d'arxiu
        #header, descartar, rutacompleta = self.detecta_conf(arxiu)

        #Parsejar l'arxiu
        #self.parser_sips_endesa(rutacompleta, header, descartar)


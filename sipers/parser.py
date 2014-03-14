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

import shutil
import tempfile


def parse_datetime(value, format='%Y%m%d'):
    try:
        res = datetime.strptime(value, format)
    except:
        res = value
    return res


class Parsejador(object):

    mongodb = None
    fitxer_conf = None
    dictres = None
    filecodificat = None

    delimiter = None
    classe = None
    contador = None
    num_fields = None
    headers = None
    descartar = None

    midafitxer = None

    def extreu_arxiu(self, tail, head, tmp_dir):
        arxiuex = open(head + '/' + tail, 'rb')
        z = zipfile.ZipFile(arxiuex)
        #print "namelist:{}".format(z.namelist())
        for name in z.namelist():
            z.extract(name, tmp_dir)
        arxiuex.close()
        return True

    def afagaarxius(self, path):
        llista_arxius = []
        for fitxer in os.listdir(path):
            fparts = fitxer.split(".")
            #print "fparts {}".format(fparts)
            if fparts[-1] == 'ZIP':
                llista_arxius.append(fitxer)
        return llista_arxius

    def insert_mongo(self, documents, collection):
        # Afegeixo les entrades
        collection.insert(documents)
        return {}

    def get_available_conf(self):
        """Aquesta funció retorna una llista amb tots els tipus de fitxers que
           podem carregar"""
        confs = []
        for fitxer in os.listdir("configs"):
            fparts = fitxer.split(".")
            confs.append(fparts[0])
        return confs

    def detectaconf(self, arxiu):
        head, tail = os.path.split(arxiu)

        for fitxer in os.listdir("configs"):
            conf = ConfigParser.RawConfigParser()
            conf.readfp(open("configs/"+fitxer))
            pattern = conf.get('parser', 'pattern')
            # Coincideix el nom del fitxer amb camp de configuració
            if re.match(pattern, tail):
                self.fitxer_conf = fitxer

        if not self.fitxer_conf:
            print "Error, no s'ha trobat cap coincidencia amb els fitxers de " \
                  "configuració"
            return False
        else:
            return True

    def load_conf(self, arxiu, directori):
        head, tail = os.path.split(arxiu)
        conf = ConfigParser.RawConfigParser()
        conf.readfp(open("configs/"+self.fitxer_conf))

        # Valors de la configuracio
        self.delimiter = conf.get('parser', 'delimiter')
        self.classe = conf.get('parser', 'class')
        self.contador = conf.get('parser', 'contador')
        self.headers = conf.items('fields')
        self.descartar = conf.options('descartar')
        try:
            self.num_fields = conf.get('parser', 'num_fields')
        except:
            self.num_fields = False


        # Crear directori temporal
        try:
            tmp_dir = tempfile.mkdtemp()
            self.extreu_arxiu(tail, directori, tmp_dir)
            # Tinc de buscar el fitxer extret
            for (path, dirs, files) in os.walk(tmp_dir):
                if files:
                    self.filecodificat = codecs.open(path+'/'+files[0], "r",
                                                     "iso-8859-15")
                    self.midafitxer = os.stat(path+'/'+files[0]).st_size
        finally:
            try:
                shutil.rmtree(tmp_dir)
            except OSError as exc:
                if exc.errno != 2:
                    raise

        return {}

    def connectamongo(self):
        # Connectar i escollir la bbdd
        client = MongoClient()
        # Base de dades
        self.mongodb = client.openerp
        return self.mongodb

    def carregar_mongo(self):
        # Log per els errors de lectura
        flog = open("log.txt", "w")
        # Camps del conf
        headers_conf = [h[0] for h in self.headers]
        valores = [h[1] for h in self.headers]
        vals = [v.split() for v in valores]
        vals_tipus = [v[0] for v in vals]
        vals_apa = [v[1] for v in vals]

        # Contador de linies
        count = 0
        # Per calcular la progressió
        sumatori = 0
        #USUARI DE TEST QUE FEM SERVIR
        user = 'default'

        # Afago la coleccio que vull
        if self.classe == 'giscedata_sips_ps':
            collection = self.mongodb.giscedata_sips_ps
        elif self.classe == 'giscedata_sips_consums':
            collection = self.mongodb.giscedata_sips_consums
        else:
            print "Error, no es troba la collection al conf"
            raise SystemExit

        # Comprovo que la collecció estigui creada, si no la creo
        if not self.mongodb[self.contador].count():
            self.mongodb[self.contador].save({"_id": self.classe, "counter": 1})

        # llegeixo per tot el fitxer
        while self.filecodificat.tell() < self.midafitxer:
            linia = self.filecodificat.readline()
            slinia = tuple(linia.split(self.delimiter))
            slinia = map(lambda s: s.strip(), slinia)

            contadorlinia = 0
            position = [eval(num, {"n": contadorlinia}) for num in vals_apa]

            for i in range(0, len(slinia), len(position)):
                try:
                    datal = [slinia[p] for p in position]
                    data = tablib.Dataset(datal, headers=headers_conf)

                    if self.num_fields and len(datal) != int(self.num_fields):
                        flog.write("Longitud de la fila {} incorrecte\n"
                                   "len_data:{}, "
                                   "self.num_fields:{}".format(count,
                                                               len(datal),
                                                               self.num_fields))

                    # Borro les claus que em surt l'arxiu de configuracio
                    for d in self.descartar:
                        del data[d]

                    # Millores: posar la cadena de lambda al fitxer de conf
                    for h, v in zip(self.headers, vals_tipus):
                        if v == 'float':
                            data.add_formatter(h[0],
                                               lambda a: a and float(a) or 0)
                        if v == 'integer':
                            data.add_formatter(h[0],
                                               lambda a:
                                               a and int
                                               (a.replace(',', '')) or 0)
                        if v == 'datetime':
                                    data.add_formatter(h[0], parse_datetime)
                        if v == 'long':
                            data.add_formatter(h[0],
                                               lambda a: a and long(a) or 0)

                    document = data.dict[0]
                    #id incremental
                    counter = self.mongodb[self.contador].find_and_modify(
                        {'_id': self.classe},
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
                except Exception as e:
                    print "Error a la linia: {}".format(e.message)
                    flog.write("Error a la fila {} , "
                               "no s'ha processat\n".format(count))

                contadorlinia += 1
                position = [eval(num, {"n": contadorlinia}) for num in
                            vals_apa]

            count += 1
            sumatori += len(linia)
            tantpercent = float(sumatori) / self.midafitxer * 100.0
            print "Completat: {} %".format(tantpercent)

        print "Numero de linies: {}".format(count)
        flog.close()

        return {}

    def parser(self, arxiu, directori, conf=False):
        # Si ve conf comprovar que sigui una opció possible
        if conf not in self.get_available_conf() or conf is False:
            print "Error, aquest tipus no existeix"
        # Si no passem cap configuracio predeterminada
        if not conf:
            if self.detectaconf(arxiu):
                self.load_conf(arxiu, directori)
                self.carregar_mongo()

        return True

    def __init__(self):
        ####
        # Parser del fitxer de SIPS de Endesa
        ####

        #Afagar els arxius de un directori
        directori = "/home/pau/Documents/sips/prova3"
        llista_arxius = self.afagaarxius(directori)

        for arxiu in llista_arxius:

            if self.connectamongo():
                self.parser(arxiu, directori)

            ## Part que funciona (antiga)
            # fparts = arxiu.split(".")
            # numseg = fparts[-2]
            # arxiu_conf = self.detecta_conf(arxiu, directori, numseg)
            # #print arxiu_conf
            # self.parser_sips_endesa(arxiu_conf)

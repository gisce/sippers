# -*- coding: utf-8 -*-

import os
import sys
import zipfile
import tablib
import codecs
from pymongo import MongoClient
import ConfigParser
import re
from datetime import datetime, date
import shutil
import tempfile
import pymongo
import copy


def parse_datetime(value, dataformat):
    # Funcio per l'add_formatter converteixi de string a datetime
    try:
        res = datetime.strptime(value, dataformat)
    except:
        res = None
    return res


def parse_float(value):
    # Funcio per l'add_formatter converteixi valors en coma a float amb punt
    try:
        punts = value.replace(',', '.')
        deci = punts.split('.')[-1]
        nume = punts.split('.')[:-1]
        if nume:
            res = float('{}.{}'.format(''.join(nume), deci))
        else:
            res = value
    except:
        res = None
    return res


"""
Variables amb els tipus de consums
- comprovarem que sigui un dels dos valors 'x' in MAGNITUDS
- Sempre dividirem pel valor de la unitat consum/MAGNITUDS['x']
"""

MAGNITUDS = {
    'Wh': 1000,
    'kWh': 1
}


class FitxerSips(object):
    # Variables estatiques
    arxiu = None
    directori = None
    dbname = None
    uri = None
    mongodb = None
    fitxer_conf = None
    filecodificat = None
    delimiter = None
    classe = None
    classes = None
    classeps = None
    classeconsums = None
    contador = None
    num_fields = None
    headers = None
    descartar = None
    primary_keys = None
    midafitxer = None
    flog = None
    log = None
    pkeys = None
    tmpdir = None
    data_format = None
    parser = None
    files = []

    def __init__(self, arxiu, directori=None, dbname=None, dburi=None):
        # Parser del fitxer de SIPS
        #Afagar els arxius de un directori
        self.directori = directori
        self.dbname = dbname
        self.dburi = dburi
        self.arxiu = arxiu
        if re.match(
                '(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF.SEG0[1-5].(zip|ZIP)',
                self.arxiu):
            from configs.endesa import Endesa
            self.parser = Endesa()
        elif re.match(
                '(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF2.SEG0[1-5].(zip|ZIP)',
                self.arxiu):
            from configs.endesacons import EndesaCons
            self.parser = EndesaCons()
        elif re.match(
                '(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF[34].SEG0[1-5].(zip|ZIP)',
                self.arxiu):
            pass
        elif re.match('HGSBKA_E0021_TXT.\.(zip|ZIP)',
                      self.arxiu):
            from configs.iberdrola import Iberdrola
            self.parser = Iberdrola()
        else:
            raise SystemError

    def extreu_arxiu(self, tail, head, tmp_dir):
        """Mètode per descomprimir el zip"""
        arxiuex = open(head + '/' + tail, 'rb')
        z = zipfile.ZipFile(arxiuex)

        for name in z.namelist():
            z.extract(name, tmp_dir)
        arxiuex.close()
        return True

    def agafarxius(self, path):
        """Donat un directori retorna llista dels zips existents"""
        llista_arxius = []
        for fitxer in os.listdir(path):
            fparts = fitxer.split(".")
            print "fparts {}".format(fparts)
            if fparts[-1].upper() == 'ZIP':
                llista_arxius.append(fitxer)
        return llista_arxius

    def get_available_conf(self):
        """Aquest mètode retorna una llista amb tots els tipus de fitxers que
           podem carregar"""
        confs = []
        for fitxer in os.listdir("configs"):
            fparts = fitxer.split(".")
            confs.append(fparts[0])
        return confs

    def detectaconfservers(self, server):
        """Afagar directori i bbdd del fitxer de conf de servers"""
        for config in os.listdir("configs/servers"):
            conf = ConfigParser.RawConfigParser()
            conf.readfp(open("configs/servers/"+config))
            serverpat = conf.get('global', 'server')

            if serverpat == server:
                self.directori = conf.get('global', 'directori')
                self.dbname = conf.get('global', 'dbname')
                self.tmpdir = conf.get('global', 'tmp_dir')
            else:
                print "Error, no s'ha trobat cap coincidencia en les confs" \
                      "dels servidors"
                return False

        return True

    def detectaconf(self, arxiu):
        """Agafar la configuració corresponent dels fitxers de configuració"""
        head, tail = os.path.split(arxiu)

        for fitxer in os.listdir("configs"):
            conf = ConfigParser.RawConfigParser()
            try:
                conf.readfp(open("configs/"+fitxer))
            except IOError:
                continue
            pattern = conf.get('parser', 'pattern')
            # Coincideix el nom del fitxer amb camp de configuració
            if re.match(pattern, tail) and len(pattern):
                self.fitxer_conf = fitxer

        if not self.fitxer_conf:
            return False
        else:
            return True

    def detectaconfselect(self, selector=None):
        """mètode alternatiu per trobar configuració, amb un selector"""

        for fitxer in os.listdir("configs"):
            conf = ConfigParser.RawConfigParser()
            try:
                conf.readfp(open("configs/"+fitxer))
            except IOError:
                continue
            try:
                nom_distri = conf.get('distri_name', 'pattern')
            except Exception as e:
                continue

            # Buscar el nom de la distribuidora
            if nom_distri == selector:
                self.fitxer_conf = fitxer

        if not self.fitxer_conf:
            return False
        else:
            return True

    def load_conf(self, arxiu, directori, dirtmp=None):
        """Mètode per agafar valors de la configuració, crear un directori
        temporal """
        #Crear directori temporal
        try:
            self.tmpdir = tempfile.mkdtemp(dir=dirtmp)
            self.extreu_arxiu(arxiu, directori, self.tmpdir)
            # Buscar el fitxer extret
            for (path, dirs, files) in os.walk(self.tmpdir):
                if files:
                    # Guardar el fitxer i la mida
                    self.files.append(path+'/'+files[0])
                    self.midafitxer = os.stat(path+'/'+files[0]).st_size
        except Exception as e:
            self.flog.write("Error: a la extració del zip, info: {}"
                            .format(e.message))
            # Borrar el directori temporal
            shutil.rmtree(self.tmpdir)
            raise SystemExit
        return True

    def connectamongo(self):
        try:
            # Connectar i escollir la bbdd
            if self.dburi:
                # Permet usr/pwd
                client = MongoClient(self.dburi)
                if self.dbname:
                    self.mongodb = client[self.dbname]
            else:
                client = MongoClient()
                # Base de dades
                self.mongodb = client[self.dbname]
            self.parser.mongodb = self.mongodb
            # Comprovo que la collecció counters estigui creada, si no la creo
            if not self.mongodb['counters'].count():
                self.mongodb['counters'].save({"_id": "log_fitxer",
                                               "counter": 1})
        except Exception as e:
            self.flog.write("Error: No s'ha pogut connectar a la base de dades,"
                            "info: {}".format(e.message))
            raise SystemExit
        return self.mongodb

    def carregar_mongo(self):
        # Carregar el mongo
        self.parser.load()
        # Contador de linies
        count = 0
        # Per calcular la progressió
        sumatori = 0

        # recorro per tots els fitxers extrets
        for arxiu in self.files:
            # Llegeixo per tot el fitxer
            with codecs.open(arxiu, "r", "iso-8859-15") as f:
                for linia in f:
                    self.parser.parse_line(linia)
                    # Actualitzo contador de linies, sumatori i
                    # tantpercent completat
                    count += 1
                    sumatori += len(linia)
                    tantpercent = float(sumatori) / self.midafitxer * 100.0

                    self.update_progress(tantpercent)

                    sys.stdout.write("\r%d%%" % int(tantpercent))
                    sys.stdout.flush()

                print "\nNumero de linies: {}".format(count)
                return True

    def parser_file(self, arxiu, directori, conf=False, selector=None):
        self.load_conf(arxiu, directori)
        self.carregar_mongo()
        return True

    def rename_file(self, extension):
        os.rename(self.directori+'/'+self.arxiu,
                  self.directori+'/'+self.arxiu+'.'+extension)
        return self.arxiu+'.'+extension

    def start(self, conf=False, selector=None):
        try:
            self.flog = open(self.arxiu + ".txt", "w")
            nom_arxiu = self.arxiu
            if self.connectamongo():
                self.arxiu = self.rename_file('lock')
                self.guardar_log_mongo()
                self.parser_file(self.arxiu, self.directori, conf, selector)
                self.arxiu = self.rename_file('end')

            self.flog.write("Fitxer finalitzat")
        except (OSError, IOError) as e:
            print "Error al intentar obrir el fitxer de log {} ({}:{})".format(
                self.flog, e.errno, str(e))
        except Exception as e:
            self.flog.write("Hi ha hagut algun error: %s" % str(e))
            self.arxiu = self.rename_file('error')
        finally:
            if self.tmpdir:
                shutil.rmtree(self.tmpdir)
            if self.flog:
                self.flog.close()

    def guardar_log_mongo(self):
        collection = self.mongodb.log_fitxer

        # Usuari del mongodb
        user = 'default'

        # Comprovo que la colletion estigui creada, si no la creo
        if not self.mongodb['counters'].find({"_id": "log_fitxer"}).count():
            self.mongodb['counters'].save({"_id": "log_fitxer",
                                           "counter": 1})
            self.mongodb.eval("""db.log_fitxer.createIndex(
                                 {"name": 1})""")

        counter = self.mongodb['counters'].find_and_modify(
            {'_id': 'log_fitxer'},
            {'$inc': {'counter': 1}})

        # Update del index
        document = {'id': counter['counter'],
                    'name': self.arxiu,
                    'create_uid': user,
                    'create_date': datetime.now()}
        try:
            document.update({'name': self.arxiu, 'estat': "INICI",
                             'progres': 0.0, 't_inici': datetime.now(),
                             't_final': None, 'message': ""})
            res = collection.update({'name': self.arxiu}, document)
            if res and res['updatedExisting'] is False:
                collection.insert(document)
        except pymongo.errors.OperationFailure:
            print "ERROR a l'actualitzar el log"

    def log_actualitzar(self, level, message):
        collection = self.mongodb.log_fitxer
        try:
            message += '[{}] {}'.format(level, message)
            document = ({'name': self.arxiu, 'message': message})
            res = collection.update({'name': self.arxiu}, document)
            if res and res['updatedExisting'] is False:
                collection.insert(document)
        except pymongo.errors.OperationFailure:
            print "ERROR a l'actualitzar el log"

    def update_progress(self, progress):
        collection = self.mongodb.log_fitxer
        try:
            collection.update({'name': self.arxiu},
                              {'$set': {'progres': progress}})
        except pymongo.errors.OperationFailure:
            print "ERROR a l'actualitzar el log"

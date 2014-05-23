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

    def __init__(self, arxiu, directori=None, dbname=None):
        # Parser del fitxer de SIPS
        #Afagar els arxius de un directori
        self.directori = directori
        self.dbname = dbname
        self.arxiu = arxiu
        if re.match('(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF.SEG0[1-5].(zip|ZIP)',
                    self.arxiu):
            from configs.endesa import Endesa
            self.parser = Endesa()
        elif re.match('(SEVILLANA|FECSA|ERZ|UNELCO|GESA).INF2.SEG0[1-5].(zip|ZIP)',
                    self.arxiu):
            from configs.endesacons import EndesaCons
            self.parser = EndesaCons()
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
                self.log.estat = 'Error'
                self.log.escriure_message_mongo('ERROR', "Error, no s'ha "
                                                         "trobat cap "
                                                         "coincidencia en les "
                                                         "confs dels servidors")
                self.log.escriure_mongo()
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
            self.log.estat = 'Error'
            self.log.escriure_message_mongo('ERROR', "La extració del zip. "
                                                     "Info: "
                                                     "{}\n".format(e.message))
            self.log.escriure_mongo()

            self.flog.write("Error: a la extració del zip, info: {}"
                            .format(e.message))
            # Borrar el directori temporal
            shutil.rmtree(self.tmpdir)
            raise SystemExit
        return True

    def connectamongo(self):
        try:
            # Connectar i escollir la bbdd
            client = MongoClient()
            # Base de dades
            self.mongodb = client[self.dbname]
            self.parser.mongodb = self.mongodb
        except Exception as e:
            self.log.estat = 'Error'
            self.log.escriure_message_mongo('ERROR', "No s'ha pogut connectar a "
                                                     "la base de dades. Info: "
                                                     "{}\n ".format(e.message))
            self.log.escriure_mongo()

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
                    # Actualitzo contador de linies, sumatori i tantpercert completat
                    count += 1
                    sumatori += len(linia)
                    tantpercent = float(sumatori) / self.midafitxer * 100.0

                    self.log.progres = float(tantpercent)
                    self.log.estat = 'Important linia'
                    self.log.escriure_mongo()

                    sys.stdout.write("\r%d%%" % int(tantpercent))
                    sys.stdout.flush()

                self.log.estat = "Finalitzat"
                self.log.escriure_message_mongo('INFO', "S'ha acabat de "
                                                        "processar l'arxiu.")
                self.log.escriure_mongo()

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
            self.arxiu = self.rename_file('lock')
            if self.connectamongo():
                # Import del fitxer de log
                from configs.log_fitxers import LogFitxers
                self.log = LogFitxers(self.mongodb, nom_arxiu, 'Start',
                                      0, str(datetime.now()))
                self.log.escriure_message_mongo('INFO', "------------ Starting the process ------------")
                self.log.escriure_mongo()

                self.parser_file(self.arxiu, self.directori, conf, selector)
                self.arxiu = self.rename_file('end')

            self.log.estat = "Fitxer finalitzat"
            self.log.escriure_message_mongo('INFO', "------------ Fitxer finalitzat ------------")
            self.log.escriure_mongo()

            self.flog.write("Fitxer finalitzat")


        except (OSError, IOError) as e:
            self.log.estat = 'Error'
            self.log.escriure_message_mongo('ERROR', " Al intentar "
                                                     "obrir el fitxer.\n "
                                                     "{}".format(e.message))
            self.log.escriure_mongo()


            print "Error al intentar obrir el fitxer de log {}".format(
                e.errno)
        except Exception as e:
            self.log.estat = 'Error'
            self.log.escriure_message_mongo('ERROR', "{}".format(e.message))
            self.log.escriure_mongo()

            self.flog.write("Hi ha hagut algun error")
            self.arxiu = self.rename_file('error')
        finally:
            shutil.rmtree(self.tmpdir)
            self.flog.close()
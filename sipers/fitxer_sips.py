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

def slices(s, *args):
    # Funció per tallar un string donades les mides dels camps.
    # La llista amb les mides dels camps entren per els args.
    position = 0
    for length in args:
        yield s[position:position + length]
        position += length

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
            import configs.endesa as Parser
            self.parser = Parser()



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

    def insert_mongo(self, document, collection):
        # Afegeixo les entrades
        try:
            pvalues = [document[k] for k in self.pkeys]
            query = dict(zip(self.pkeys, pvalues))

            res = collection.update(query, document)
            if res['updatedExisting'] is False:
                collection.insert(document)
        except pymongo.errors.OpertionFailure:
            self.flog.write("Error: A l'insert del mongodb")

        return True

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
            client = MongoClient()
            # Base de dades
            self.mongodb = client[self.dbname]
            self.parser.mongodb = self.mongodb
        except Exception as e:
            self.flog.write("Error: No s'ha pogut connectar a la base de dades,"
                            "info: {}".format(e.message))
            raise SystemExit
        return self.mongodb

    def carregar_mongo(self):
        # Contador de linies
        count = 0
        # Per calcular la progressió
        sumatori = 0
        # Usuari del mongodb
        user = 'default'

        # Llegeixo per tot el fitxer
        with codecs.open(self.files[0], "r","iso-8859-15") as f:
            for linia in f:
                self.parser.parse_line(linia)
                # Actualitzo contador de linies, sumatori i tantpercert completat
                count += 1
                sumatori += len(linia)
                tantpercent = float(sumatori) / self.midafitxer * 100.0

                sys.stdout.write("\r%d%%" % int(tantpercent))
                sys.stdout.flush()

            print "\nNumero de linies: {}".format(count)
            return True

    def parser_file(self, arxiu, directori, conf=False, selector=None):
        # Si ve conf comprovar que sigui una opció possible
        if conf not in self.get_available_conf() and conf:
            self.flog.write("Error, la configuració {} que ha entrat no es "
                            "troba als fitxers de configuració".format(conf))
            raise SystemExit
        elif conf in self.get_available_conf() and conf:
            self.fitxer_conf = conf
        # Si no passem cap configuracio predeterminada
        if not conf:
            if selector:
                try:
                    self.detectaconfselect(selector)
                except Exception as e:
                    self.flog.write("Error, en detectar la configuracio amb "
                                    "el selector. {}".format(e.message))
            else:
                try:
                    self.detectaconf(arxiu)
                except Exception as e:
                    self.flog.write("Error, No s'ha trobat el fitxer de "
                                    "configuració correcte de "
                                    "forma automatica. {}".format(e.message))
            if self.fitxer_conf:
                self.load_conf(arxiu, directori)
                self.carregar_mongo()
            else:
                self.flog.write("Error, No s'ha trobat el fitxer de "
                                    "configuració correcte de "
                                    "forma automatica. {}".format(e.message))
        return True

    def rename_file(self, extension):
        os.rename(self.directori+'/'+self.arxiu,
                  self.directori+'/'+self.arxiu+'.'+extension)
        return self.arxiu+'.'+extension

    def start(self, conf=False, selector=None):
        try:
            self.flog = open(self.arxiu + ".txt", "w")
            self.arxiu = self.rename_file('lock')
            if self.connectamongo():
                self.parser_file(self.arxiu, self.directori, conf, selector)
                self.arxiu = self.rename_file('end')
            self.flog.write("Fitxer finalitzat")

        except (OSError, IOError) as e:
            print "Error al intentar obrir el fitxer de log {}".format(
                e.errno)
        except:
            self.flog.write("Hi ha hagut algun error")
            self.arxiu = self.rename_file('error')
        finally:
            self.flog.close()
            try:
                # Borrar el directori temporal
                shutil.rmtree(self.tmpdir)
            except OSError as exc:
                if exc.errno != 2:
                    raise SystemExit

    # def run(self, conf=False, selector=None):
    #     llista_arxius = self.agafarxius(self.directori)
    #     # Processar per cada un dels arxius zip
    #     for arxiu in llista_arxius:
    #         # Log per els errors de lectura
    #         print "Arxiu:{}".format(arxiu)
    #         try:
    #             self.flog = open(arxiu + ".txt", "w")
    #         except (OSError, IOError) as e:
    #             print "Error al intentar obrir el fitxer de log {}".format(
    #                 e.errno)
    #
    #         try:
    #             if self.connectamongo():
    #                 self.parser(arxiu, self.directori, conf, selector)
    #             self.flog.write("Fitxer finalitzat")
    #         except:
    #             self.flog.write("Hi ha hagut algun error")
    #
    #         self.flog.close()

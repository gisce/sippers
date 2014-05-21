# -*- coding: utf-8 -*-

import os
import time
from parser import Parsejador
from datetime import datetime



DEFAULT_DIRECTORI = '/tmp'
DEFAULT_DBNAME = 'sips'


def agafarxius(path):
    """Donat un directori retorna llista dels zips existents"""
    llista_arxius = []
    for fitxer in os.listdir(path):
        fparts = fitxer.split(".")
        print "fparts {}".format(fparts)
        if fparts[-1].upper() == 'ZIP':
            # right now, as epoch
            now = int(time.mktime(datetime.now().timetuple()))
            # last modification of file, as epoch
            mtime = os.stat(path+'/'+fitxer)[8]
            if now >= mtime:
                llista_arxius.append(fitxer)
    return llista_arxius

def run(directori, dbname):
    print directori
    print dbname

    llista_arxius = agafarxius(directori)
    # Processar per cada un dels arxius zip
    for arxiu in llista_arxius:
        # Log per els errors de lectura
        print "Arxiu:{}".format(arxiu)
        parser = Parsejador(arxiu=arxiu, directori=directori, dbname=dbname)
        parser.start()

def main():
    import optparse
    import sys
    parser = optparse.OptionParser()
    parser.add_option("--directori", dest="directori")
    parser.add_option("--dbname", dest="dbname")
    (options, args) = parser.parse_args()
    if not options.directori and not options.dbname:
        print 'Usage: sipers --directori=PATH --dbname=DBNAME'
        sys.exit(1)
    sys.exit(run(**options.__dict__))

if __name__ == '__main__':
    main()
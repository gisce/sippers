# -*- coding: utf-8 -*-

import ConfigParser
import os
import zipfile
import sys
import tablib
import codecs
from pymongo import MongoClient
import ConfigParser

class Parsejador():

    def insert_mongo(self, documents):
        # Connectar i escollir la bbdd
        client = MongoClient()
        db = client.test_database
        # Agafo la colecció
        sips = db.sips
        # Afegeixo les entrades
        sips.insert(documents)
        return {}

    def parser_sips_endesa(self, arxiu):
        filecodificat = codecs.open(arxiu, "r", "iso-8859-15")
        midafitxer = os.stat(arxiu).st_size
        #print 'Mida del fitxer: {}'.format(midafitxer)

        # Llegeixo headers del fitxer de configuracio (FER APART)
        config = ConfigParser.RawConfigParser()
        config.readfp(open('configs/Endesa.cfg'))
        headerss = config.options('fields')

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

        while filecodificat.tell() < midafitxer:
            #import pdb; pdb.set_trace()
            linia = filecodificat.readline()
            slinia = tuple(linia.split(';'))
            slinia = map(lambda s: s.strip(), slinia)
            #print 'length slinia:{} length headers:{}'.format(len(slinia),
            #                                                  len(headerss))
            part.append(slinia)
            if count % 100:
                data = tablib.Dataset(*part, headers=headerss)
                document = data.dict
                #print document
                self.insert_mongo(document)
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
            self.insert_mongo(document)

        #print data.json
        filecodificat.close()

        return {}

    def __init__(self):
        ####
        # Parser del fitxer de SIPS de Endesa
        ####

        arxiu = "/home/pau/Documents/sips/SCEF/EXPT/" \
                "INFPSCOM/INF/SEG01/SORT/S131213_backup"

        #Detectar tipus d'arxiu
        #Escollir configuració
        #Parsejar l'arxiu
        self.parser_sips_endesa(arxiu)


import pymongo

class LogFitxers(object):
    name = None
    estat = None
    progres = 0.0
    t_inici = None
    t_final = None
    message = ""
    linia = None
    mongodb = None
    collection = None

    LEVELS = ('INFO',
              'WARNING',
              'ERROR')

    def __init__(self, mongodb=None, name=None, estat=None, progres=None,
                 t_inici=None, t_final=None, message=""):
        self.mongodb = mongodb
        self.name = name
        self.estat = estat
        self.progres = progres
        self.t_inici = t_inici
        self.t_final = t_final
        self.message = message

        self.collection = self.mongodb.log_fitxer
        self.mongodb.eval("""db.log_fitxer.ensureIndex(
        {"name": 1})""")


    def escriure_mongo(self):
        try:
            document = {'name': self.name, 'estat': self.estat,
                        'progres': self.progres, 't_inici': self.t_inici,
                        't_final': self.t_final, 'message': self.message}
            res = self.collection.update({'name': self.name}, document)
            if res['updatedExisting'] is False:
                self.collection.insert(document)
        except pymongo.errors.OperationFailure:
            print "ERROR a l'actualitzar el log"

    def escriure_message_mongo(self, level, message):
        self.message += '[{}] {}'.format(level, message)
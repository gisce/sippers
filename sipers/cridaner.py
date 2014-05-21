from fitxer_sips import FitxerSips

parser = FitxerSips(
    directori="/home/pau/Documents/sips/IBERDROLA 20140127/Iberdrola/prova",
    dbname="somenergia")
print "parser:{}".format(parser)
parser.run()


from parser import Parsejador

parser = Parsejador(
    directori="/home/pau/Documents/sips/IBERDROLA 20140127/Iberdrola/prova",
    dbname="somenergia")
print "parser:{}".format(parser)
parser.run()


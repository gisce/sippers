from parser import Parsejador

parser = Parsejador(
    directori="/home/pau/Documents/sips/prova3",
    dbname="somenergia")
print "parser:{}".format(parser)
parser.run()


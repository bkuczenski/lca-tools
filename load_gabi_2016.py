import sys
from optparse import OptionParser

from lcatools.db_catalog import load_gabi_set

GABI_2016_INDEX = "http://www.gabi-software.com/support/gabi/gabi-database-2016-lci-documentation/"

parser = OptionParser()
parser.add_option("-d", "--directory", dest="dir", default=".",
                  help="Specify output directory")

(options, args) = parser.parse_args(sys.argv)

load_gabi_set(GABI_2016_INDEX, version='2016', savedir=options.dir)


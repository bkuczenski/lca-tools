import sys

from db_catalog import load_gabi_set

ref = sys.argv[1]


load_gabi_set(ref, version='2016', savedir='/data/GitHub/lca-tools-datafiles/')


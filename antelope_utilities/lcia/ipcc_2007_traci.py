"""
Extract GWP factors from the old TRACI 2014 spreadsheet and store them in JSON.

This project inspired the "domesticate" kwarg in write_to_file.
"""
# TODO: add provenance data upon domestication
# TODO: this could be done via the catalog
# TODO: figure out how to authorize refs

from lcatools.lcia_engine import LciaEngine
from antelope_catalog.providers import Traci21Factors
from antelope_catalog import LcCatalog
from antelope_catalog.data_sources.local import CATALOG_ROOT


from argparse import ArgumentParser

TARGET = '/data/GitHub/lca-tools/lcatools/lcia_engine/data/ipcc_2007_gwp.json'
TRACI_source = '/data/LCI/TRACI/traci_2_1_2014_dec_10_0.xlsx'

# AUTHORIZED_REF = 'lcia.ipcc.2007.traci21'  # this should not be defined here

COMMENT = """IPCC GWP factors, as implemented in TRACI 2.1 [traci_2_1_2014_dec_10_0.xlsx]

should probably auto-generate some provenance data here or something..."""

SOURCE = "IPCC Fourth Assessment Report, Working Group I: Technical Summary, table TS-2"


parser = ArgumentParser()
parser.add_argument("-s", "--source", dest="source",
                    default=TRACI_source,
                    help="Source file for TRACI 2.1", metavar="FILE")
parser.add_argument("-t", "--target", dest="target",
                    default=TARGET,
                    help="Target JSON file")


def delete_existing_resources(source):
    print('deleting existing resources in local catalog')
    cat = LcCatalog(CATALOG_ROOT)
    ress = [res for res in cat._resolver.resources_with_source(source)]
    for res in ress:
        cat.delete_resource(res)


def generate_ipcc_2007_traci21(source, target):
    print('Exporting TRACI factors as IPCC 2007')
    T = Traci21Factors(source, term_manager=LciaEngine())
    q = T['Global Warming Air']
    q['Comment'] = COMMENT
    q['Source'] = SOURCE

    T.add_method_and_compartment(method=q)

    print(str(q))

    T.export_quantity(target, q, domesticate=True)


if __name__ == '__main__':
    args = parser.parse_args()
    generate_ipcc_2007_traci21(args.source, args.target)
    delete_existing_resources(args.source)

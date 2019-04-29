"""
Extract GWP factors from the old TRACI 2014 spreadsheet and store them in JSON.

This project inspired the "domesticate" kwarg in write_to_file.
"""
# TODO: add provenance data upon domestication
# TODO: this could be done via the catalog
# TODO: figure out how to authorize refs


from lcatools.archives import BasicArchive
from antelope_catalog.providers import Traci21Factors

from argparse import ArgumentParser

TARGET = '/data/GitHub/lca-tools/lcatools/qdb/data/ipcc_2007_gwp.json'
TRACI_source = '/data/LCI/TRACI/traci_2_1_2014_dec_10_0.xlsx'

AUTHORIZED_REF = 'lcia.ipcc.2007.traci21'

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


def generate_ipcc_2007_traci21(source, target):
    T = Traci21Factors(source)
    q = T['Global Warming Air']
    q['Comment'] = COMMENT
    q['Source'] = SOURCE

    T.load_all()

    print(str(q))

    ar = BasicArchive(None, ref=AUTHORIZED_REF, ns_uuid=T.init_args['ns_uuid'])
    Ti = T.make_interface('index')

    for f in Ti.flows():
        dels = []
        if not f.has_characterization(q):
            continue
        for c in f.characterizations():
            if c.quantity is f.reference_entity:
                continue
            if c.quantity is q:
                continue
            dels.append(c.quantity)
        for qq in dels:  # dictionary changed size during iteration
            f.del_characterization(qq)
        ar.add_entity_and_children(f)

    ar.write_to_file(target, complete=True, domesticate=True)


if __name__ == '__main__':
    args = parser.parse_args()
    generate_ipcc_2007_traci21(args.source, args.target)

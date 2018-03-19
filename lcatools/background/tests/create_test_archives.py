"""
This function performs the (surpassingly slow) task of extracting reduced aggregated LCI results from the large bundled
ecoinvent 7z LCI archives.  (The operation is slow on RAM-limited machines because the 7z algorithm requires tremendous
memory)

The routine requires the process inventory test case to be selected manually.

The selected inventory is loaded, and then one hundred exchanges are selected at random and the rest are removed.  This
reduces the file size (and load time) of the generated archives without sacrificing the representativeness of the
computation.
"""

import os
import random
import sys

from lcatools.providers.ecospold2 import EcospoldV2Archive
from lcatools.providers.lc_archive import LcArchive

sources = {'3_2_apos': '/data/LCI/Ecoinvent/3.2/current_Version_3.2_apos_lci_ecoSpold02.7z',
           '3_2_conseq': '/data/LCI/Ecoinvent/3.2/current_Version_3.2_consequential_longterm_lci_ecoSpold02.7z'
           }

nodes = {'3_2_apos': '18085d22-72d0-4588-9c69-7dbeb24f8e2f',
         '3_2_conseq': ''
         }

DEST_DIR = os.path.dirname(os.path.abspath(__file__))


def dest_archive(source):
    return os.path.join(DEST_DIR,
                        'ei_%s_lci_test.json.gz' % source)



def extract_and_reduce_inventory(source):
    ext_ref = nodes[source]
    archive = EcospoldV2Archive(sources[source], prefix='datasets')
    p = archive.retrieve_or_fetch_entity(ext_ref)

    keylist = random.sample([x.key for x in p.inventory()], 100)
    for x in p.references():
        keylist.append(x.key)

    xlist = [x for x in p._exchanges.keys()]
    for x in xlist:
        if x not in keylist:
            p._exchanges.pop(x)

    a = LcArchive(None, ref='test.%s' % source)
    a.add_entity_and_children(p)
    a.write_to_file(dest_archive(source), complete=True, gzip=True)


if __name__ == '__main__':
    force = '-force' in sys.argv
    for k in sources.keys():
        if force or not os.path.exists(dest_archive(k)):
            print('Creating test archive for source %s')
            extract_and_reduce_inventory(k)

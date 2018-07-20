import os
import re

from .ecospold2 import EcospoldV2Archive
from .xml_widgets import find_tags


ECOSPOLD = os.path.join('/data', 'Dropbox', 'data', 'Ecoinvent', '3.2', 'current_Version_3.2_cutoff_lci_ecoSpold02.7z')
ES_FILE = '00009573-c174-463a-8ebf-183ec587ba0d_7cb72345-4338-4f2d-830f-65bba3530fdb.spold'


def get_ecospold_exchanges(archive=ECOSPOLD, prefix='datasets', file=ES_FILE):
    E = EcospoldV2Archive(archive, prefix=prefix)
    o = E.objectify(file)
    return find_tags(o, 'elementaryExchange')


def _add_syn_if(syn, synset):
    g = syn.strip()
    if g != '' and g != 'PSM':
        synset.add(syn)


def synonyms_from_ecospold_exchange(exch):
    """
    Ecospold exchanges: synonyms are Name, CAS Number, and ', '-separated contents of synonym tags.
    Care must be taken not to split on ',' as some chemical names include commas
    :param exch:
    :return: set of synonyms (stripped)
    """
    syns = set()
    name = str(exch['name'])
    syns.add(name)
    cas = exch.get('casNumber')
    if cas is not None:
        syns.add(cas)
    synonym_tag = find_tags(exch, 'synonym')
    if len(synonym_tag) == 1:
        # parse the comma-separated list
        if bool(re.search('etc\.', str(synonym_tag[0]))):
            syns.add(str(synonym_tag[0]).strip())
        else:
            for x in str(synonym_tag[0]).split(', '):
                _add_syn_if(x, syns)
    else:
        # multiple entries- allow embedded comma-space
        for syn in synonym_tag:
            _add_syn_if(str(syn), syns)
    return name, syns



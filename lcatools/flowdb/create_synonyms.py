import os
import json
import re

from lcatools.providers.ecospold2 import EcospoldV2Archive
from lcatools.providers.ilcd_lcia import IlcdLcia
from lcatools.providers.ilcd import grab_flow_name
from lcatools.providers.xml_widgets import find_tag, find_common, find_ns
from lcatools.flowdb.synlist import Flowables, InconsistentIndices, ConflictingCas


ECOSPOLD = os.path.join('/data', 'Dropbox', 'data', 'Ecoinvent', '3.2', 'current_Version_3.2_cutoff_lci_ecoSpold02.7z')
ES_FILE = '00009573-c174-463a-8ebf-183ec587ba0d_7cb72345-4338-4f2d-830f-65bba3530fdb.spold'

ELCD = os.path.join('/data', 'Dropbox', 'data', 'ELCD', 'ELCD3.2-a.zip')

SYNONYMS = os.path.join(os.path.dirname(__file__), 'synonyms.json')


def get_ecospold_exchanges(archive=ECOSPOLD, prefix='datasets', file=ES_FILE):
    E = EcospoldV2Archive(archive, prefix=prefix)
    o = E.objectify(file)
    return find_tag(o, 'elementaryExchange')


def ilcd_flow_generator(archive=ELCD, **kwargs):
    I = IlcdLcia(archive, **kwargs)
    count = 0
    for f in I.list_objects('Flow'):
        o = I.objectify(f, dtype='Flow')
        if o is not None:
            yield o
            count += 1
            if count % 1000 == 0:
                print('%d data sets completed' % count)


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
    synonym_tag = find_tag(exch, 'synonym')
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


def synonyms_from_ilcd_flow(flow):
    """
    ILCD flow files have long synonym blocks at the top. They also have a CAS number and a basename.
    :param flow:
    :return:
    """
    ns = find_ns(flow.nsmap, 'Flow')
    syns = set()
    name = grab_flow_name(flow, ns=ns)
    syns.add(name)
    uid = str(find_common(flow, 'UUID')[0]).strip()
    syns.add(uid)
    cas = str(find_tag(flow, 'CASNumber', ns=ns)[0]).strip()
    if cas != '':
        syns.add(cas)
    for syn in find_common(flow, 'synonyms'):
        for x in str(syn).split(';'):
            if x.strip() != '' and x.strip().lower() != 'wood':
                syns.add(x.strip())
    return name, syns, uid


cas_regex = re.compile('^[0-9]{,6}-[0-9]{2}-[0-9]$')


def _add_set(synlist, name, syns, xid):
    try:
        index = synlist.add_set(syns, merge=True, name=name)
    except ConflictingCas:
        index = synlist.new_set(syns, name=name)
    except InconsistentIndices:
        dups = synlist.find_indices(syns)
        matches = []
        for i in dups:
            for j in syns:
                if j in synlist[i]:
                    matches.append((j, i))
                    break

        try:
            index = synlist.merge_indices(dups)
            print('Merged Inconsistent indices in ID %s, e.g.:' % xid)
            for match in matches:
                print('  [%s] = %d' % match)
        except ConflictingCas:
            #print('Conflicting CAS on merge.. creating new group')
            index = synlist.new_set(syns, name=name)
    return index


def create_new_synonym_list():
    """
    This just makes a SynList and populates it, first with ecoinvent, then with ILCD, and saves it to disk
    :return:
    """
    synonyms = Flowables()

    # first, ecoinvent
    exchs = get_ecospold_exchanges()
    for exch in exchs:
        name, syns = synonyms_from_ecospold_exchange(exch)
        _add_set(synonyms, name, syns, exch.get('id'))

    # next, ILCD - but hold off for now
    for flow in ilcd_flow_generator():
        name, syns, uid = synonyms_from_ilcd_flow(flow)
        _add_set(synonyms, name, syns, uid)

    with open(SYNONYMS, 'w') as fp:
        json.dump(synonyms.serialize(), fp)
        print('Wrote synonym file to %s' % SYNONYMS)
    return synonyms


def load_synonyms(file=SYNONYMS):
    with open(file) as fp:
        return Flowables.from_json(json.load(fp))

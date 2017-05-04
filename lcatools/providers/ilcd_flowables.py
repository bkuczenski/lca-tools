import os
import re

from lcatools.providers.ilcd_lcia import IlcdLcia
from lcatools.providers.ilcd import grab_flow_name
from lcatools.providers.xml_widgets import find_tag, find_common, find_ns

ELCD = os.path.join('/data', 'LCI', 'ELCD', 'ELCD3.2.zip')


def ilcd_flow_generator(archive=ELCD, **kwargs):
    """
    This generates flows from the current reference ELCD archive.
    :param archive:
    :param kwargs:
    :return:
    """
    I = IlcdLcia(archive, **kwargs)
    count = 0
    for f in I.list_objects('Flow'):
        o = I.objectify(f, dtype='Flow')
        if o is not None:
            yield o
            count += 1
            if count % 1000 == 0:
                print('%d data sets completed' % count)


ilcd_bad_synonyms = {
    'fe0acd60-3ddc-11dd-a6f8-0050c2490048': "Crude Oil; 42.3 MJ/kg",
    '08a91e70-3ddc-11dd-944a-0050c2490048': "Carbon, resource, in ground",
    '3e4d9eab-6556-11dd-ad8b-0800200c9a66': "Wood;  14.7 MJ/kg"
}


ilcd_bad_cas = {

}


def synonyms_from_ilcd_flow(flow):
    """
    ILCD flow files have long synonym blocks at the top. They also have a CAS number and a basename.

    Skips synonym blocks for ILCD flows known to have bad synonyms:
      * "Crude Oil; 42.3 MJ/kg" is not a synonym for "Benzene, pure", etc.
      * "Carbon [resource, in ground]" is not a synonym for the variety of compounds that may be manufactured from it
      * "Wood;  14.7 MJ/kg" says synonyms removed but weren't.

    Skips 'wood' from any list, which is abused badly in the ILCD synonyms list. Methanol and wood are not synonymous.
    :param flow:
    :return: uuid (str), name (str), syns (set, includes name, excludes uuid)
    """
    ns = find_ns(flow.nsmap, 'Flow')
    uid = str(find_common(flow, 'UUID')[0]).strip()
    syns = set()
    name = grab_flow_name(flow, ns=ns)
    syns.add(name)
    cas = str(find_tag(flow, 'CASNumber', ns=ns)[0]).strip()
    if cas != '':
        syns.add(cas)
    if uid in ilcd_bad_synonyms:
        print('Skipping Synonyms for %s' % name)
    else:
        for syn in find_common(flow, 'synonyms'):
            for x in str(syn).split(';'):
                if x.strip() != '' and x.strip().lower() != 'wood':
                    syns.add(x.strip())
    return uid, name, syns

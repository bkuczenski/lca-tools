"""
Container for tools used to compute interesting things about / across archives
"""

from __future__ import print_function, unicode_literals

import os
import re

from collections import defaultdict, Counter

from lcatools.from_json import from_json

from lcatools.providers.base import LcArchive
from lcatools.providers.ilcd import IlcdArchive
from lcatools.providers.ilcd_lcia import IlcdLcia
from lcatools.providers.ecospold2 import EcospoldV2Archive
from lcatools.providers.ecoinvent_spreadsheet import EcoinventSpreadsheet
from lcatools.providers.ecospold import EcospoldV1Archive
from lcatools.providers.ecoinvent_lcia import EcoinventLcia
from lcatools.providers.study import LcStudy
from lcatools.providers.traci_2_1_spreadsheet import Traci21Factors
# from lcatools.foreground.foreground import ForegroundArchive

# from lcatools.db_catalog import from_json  # included for "from tools import *" by user

# TODO: re-implement these tools to work directly on json catalogs; put them in lca-tools-datafiles

catalog_dir = '/data/GitHub/lca-tools-datafiles/catalogs'


def gz_files(path):
    return [os.path.join(path, f) for f in filter(lambda x: re.search('\.json\.gz$', x), os.listdir(path))]


def split_nick(fname):
    return re.sub('\.json\.gz$', '', os.path.basename(fname))


def create_archive(source, ds_type, **kwargs):
    if ds_type.lower() == 'json':
        a = archive_from_json(source, **kwargs)
    else:
        a = archive_factory(source, ds_type, **kwargs)
    return a


def archive_factory(source, ds_type, **kwargs):
    """
    creates an archive
    :param source:
    :param ds_type:
    :param kwargs:
    :return:
    """
    init_fcn = {
        'lcarchive': LcArchive,
        'ilcdarchive': IlcdArchive,
        'ilcd': IlcdArchive,
        'ilcdlcia': IlcdLcia,
        'ilcd_lcia': IlcdLcia,
        'ecospoldv1archive': EcospoldV1Archive,
        'ecospold': EcospoldV1Archive,
        'ecospoldv2archive': EcospoldV2Archive,
        'ecospold2': EcospoldV2Archive,
        'ecoinventspreadsheet': EcoinventSpreadsheet,
        'ecoinventlcia': EcoinventLcia,
        'ecoinvent_lcia': EcoinventLcia,
        'study': LcStudy,
        'traci2': Traci21Factors,
        'traci21': Traci21Factors,
        'traci21factors': Traci21Factors
    }[ds_type.lower()]
    return init_fcn(source, **kwargs)
#        'foregroundarchive': ForegroundArchive.load,
#        'study': ForegroundArchive.load


def archive_from_json(fname, **archive_kwargs):
    """
    :param fname: JSON filename
    :return: an ArchiveInterface
    """
    j = from_json(fname)
    archive_kwargs['quiet'] = True

    if 'prefix' in j.keys():
        archive_kwargs['prefix'] = j['prefix']

    if 'nsUuid' in j.keys():
        archive_kwargs['ns_uuid'] = j['nsUuid']

    if j['dataSourceType'] == 'EcoinventSpreadsheet':
        archive_kwargs['internal'] = bool(j['internal'])
        archive_kwargs['version'] = j['version']

    ref = None
    if 'dataSourceReference' in j:
        # old style
        source = j['dataSourceReference']
    else:
        # new style
        source = j['dataSource']
        if 'dataReference' in j:
            ref = j['dataReference']

    try:
        a = archive_factory(source, j['dataSourceType'], **archive_kwargs)
    except KeyError:
        raise ValueError('Unknown dataSourceType %s' % j['dataSourceType'])

    if 'catalogNames' in j:
        a.catalog_names = j['catalogNames']

    if ref is not None:
        a.catalog_names[ref] = source

    if 'upstreamReference' in j:
        print('**Upstream reference encountered: %s\n' % j['upstreamReference'])
        a._serialize_dict['upstreamReference'] = j['upstreamReference']

    a.load_json(j)
    return a


def parse_exchange(exch_ref):
    return exch_ref.split(': ')


def count_ref_flows(archive):
    rfs = Counter()
    for i in archive.processes():
        x = i['referenceExchange']
        if x is None:
            rfs[(None, None)] += 1
        else:
            rfs[(x.flow['Name'], x.direction)] += 1
    return rfs


def print_ref_flows(archive, count=10):
    print(archive.source)
    rfs = count_ref_flows(archive)
    print('%6s %9s %-30s' % ('Count', 'Direction', 'Flow'))
    print('%s' % '-'*50)
    for i in rfs.most_common(count):
        print('%6d %8s  %s' % (i[1], i[0][1], i[0][0]))


def count_exchanges(archive):
    rfs = Counter()
    for x in archive.exchanges():

        rfs[(str(x.flow), x.direction)] += 1
    return rfs


def print_exch_counts(archive, count=12):
    print(archive.source)
    rfs = count_exchanges(archive)
    print('%6s %9s %-30s' % ('Count', 'Direction', 'Flow'))
    print('%s' % '-'*50)
    for i in rfs.most_common(count):
        print('%6d %8s  %s' % (i[1], i[0][1], i[0][0]))


def tags(entity, delimiter=';\s*|,\s*|\s*\(|\)\s*|/', exclude=('TemporalScope', 'IsicNumber')):
    t = set()
    for k, v in entity._d.items():
        if v is None:
            continue
        if k not in exclude:
            try:
                t = t.union('='.join([k, f]) for f in filter(bool, re.split(delimiter, v)))
            except TypeError:
                t = t.union('='.join([k, f]) for f in filter(bool, re.split(delimiter, ', '.join(v))))
    return t


def count_tags(e_list, search=None):
    d = Counter()
    m = defaultdict(list)
    for e in e_list:
        t = tags(e)
        if search is not None:
            if not any([bool(re.search(search, k, flags=re.IGNORECASE)) for k in t]):
                continue
        for i in t:
            d[i] += 1
            m[i].append(e.get_uuid())
    return d, m

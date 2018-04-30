"""
Container for tools used to compute interesting things about / across archives
"""

from __future__ import print_function, unicode_literals

import os
import re

from collections import defaultdict, Counter

from lcatools.from_json import from_json

from lcatools.providers.lc_archive import LcArchive
from lcatools.providers.ilcd import IlcdArchive, IlcdLcia
from lcatools.providers.ecospold2 import EcospoldV2Archive
from lcatools.providers.ecoinvent_spreadsheet import EcoinventSpreadsheet
from lcatools.providers.ecospold import EcospoldV1Archive
from lcatools.providers.ecoinvent_lcia import EcoinventLcia
from lcatools.providers.foreground import LcForeground
from lcatools.providers.openlca_jsonld import OpenLcaJsonLdArchive
from lcatools.providers.traci import Traci21Factors
# from lcatools.providers.antelope.antelope_v1 import AntelopeV1Client
# from lcatools.foreground.foreground import ForegroundArchive

# from lcatools.db_catalog import from_json  # included for "from tools import *" by user

# TODO: re-implement these tools to work directly on json catalogs; put them in lca-tools-datafiles

catalog_dir = '/data/GitHub/lca-tools-datafiles/catalogs'


class ArchiveError(Exception):
    pass


needs_catalog = {'foreground', 'lcforeground', 'antelope', 'antelopev1', 'antelopev1archive', 'antelopev1client'}


def gz_files(path):
    return [os.path.join(path, f) for f in filter(lambda x: re.search('\.json\.gz$', x), os.listdir(path))]


def split_nick(fname):
    return re.sub('\.json\.gz$', '', os.path.basename(fname))


def create_archive(source, ds_type, catalog=None, **kwargs):
    """
    Create an archive from a source and type specification.
    :param source:
    :param ds_type:
    :param catalog: required to identify upstream archives, if specified
    :param kwargs:
    :return:
    """
    if ds_type.lower() == 'json':
        a = archive_from_json(source, catalog=catalog, **kwargs)
    elif ds_type.lower() in needs_catalog:
        # LcForeground needs a catalog
        a = archive_factory(source, ds_type, catalog=catalog, **kwargs)
    else:
        a = archive_factory(source, ds_type, **kwargs)
    return a


def update_archive(archive, json_file):
    archive.load_json(from_json(json_file), jsonfile=json_file)


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
#        'antelope': AntelopeV1Client,
#        'antelopev1': AntelopeV1Client,
#        'antelopev1archive': AntelopeV1Client,
#        'antelopev1client': AntelopeV1Client,
        'ecospoldv1archive': EcospoldV1Archive,
        'ecospold': EcospoldV1Archive,
        'ecospoldv2archive': EcospoldV2Archive,
        'ecospold2': EcospoldV2Archive,
        'ecoinventspreadsheet': EcoinventSpreadsheet,
        'ecoinventlcia': EcoinventLcia,
        'ecoinvent_lcia': EcoinventLcia,
        'openlcajsonldarchive': OpenLcaJsonLdArchive,
        'openlca': OpenLcaJsonLdArchive,
        'openlca_jsonld': OpenLcaJsonLdArchive,
        'olca': OpenLcaJsonLdArchive,
        'olca_jsonld': OpenLcaJsonLdArchive,
        'foreground': LcForeground,
        'lcforeground': LcForeground,
        'traci2': Traci21Factors,
        'traci': Traci21Factors,
        'traci21factors': Traci21Factors
    }[ds_type.lower()]
    try:
        return init_fcn(source, **kwargs)
#        'foregroundarchive': ForegroundArchive.load,
#        'foreground': ForegroundArchive.load
    except KeyError as e:
        raise ArchiveError('%s' % e)


def archive_from_json(fname, static=True, catalog=None, **archive_kwargs):
    """
    :param fname: JSON filename
    :param catalog: [None] necessary to retrieve upstream archives, if specified
    :param static: [True]
    :return: an ArchiveInterface
    """
    j = from_json(fname)
    archive_kwargs['quiet'] = True
    archive_kwargs['static'] = static

    if 'prefix' in j.keys():
        archive_kwargs['prefix'] = j['prefix']

    if 'nsUuid' in j.keys():
        archive_kwargs['ns_uuid'] = j['nsUuid']

    if j['dataSourceType'] == 'EcoinventSpreadsheet':
        archive_kwargs['internal'] = bool(j['internal'])
        archive_kwargs['version'] = j['version']

    if 'dataSourceReference' in j:
        # old style
        source = j['dataSourceReference']
    else:
        # new style
        source = j['dataSource']

    try:
        a = archive_factory(source, j['dataSourceType'], **archive_kwargs)
    except KeyError:
        raise ValueError('Unknown dataSourceType %s' % j['dataSourceType'])

    if 'upstreamReference' in j:
        print('**Upstream reference encountered: %s\n' % j['upstreamReference'])
        if catalog is not None:
            try:
                upstream = catalog.get_archive(j['upstreamReference'])
                a.set_upstream(upstream)
            except KeyError:
                print('Upstream reference not found in catalog!')
                a._serialize_dict['upstreamReference'] = j['upstreamReference']
            except ValueError:
                print('Upstream reference is ambiguous!')
                a._serialize_dict['upstreamReference'] = j['upstreamReference']
        else:
            a._serialize_dict['upstreamReference'] = j['upstreamReference']

    a.load_json(j, jsonfile=fname)
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

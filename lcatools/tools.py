"""
Container for tools used to compute interesting things about / across archives
"""

from __future__ import print_function, unicode_literals

import os
import re
import gzip
import json

from collections import defaultdict, Counter

from lcatools.db_catalog import from_json  # included for "from tools import *" by user

# TODO: re-implement these tools to work directly on json catalogs; put them in lca-tools-datafiles

catalog_dir = '/data/GitHub/lca-tools-datafiles/catalogs'


def gz_files(path):
    return [os.path.join(path, f) for f in filter(lambda x: re.search('\.gz$', x), os.listdir(path))]


def load_json(file):
    with gzip.open(file, 'rt') as fp:
        return json.load(fp)


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
    print(archive.ref)
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
    print(archive.ref)
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

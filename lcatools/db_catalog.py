"""
Python script to generate interfaces from DBs (local and remote) and dump them into local files.
 The interfaces are BasicInterfaces for now- include only metadata- exchange values are not stored
 and are instead left for the individual DBs.  The local imprints should be suitable to identify
 data sets of interest and should function to dereference query requests.

 Command-line usage:
 $ python gen_db_catalog.py <db_name>

 where db_name is stored locally in db_info.py, will load the database and dump it into <db_name>.json.

 $ python gen_db_catalog.py <db_name> --ref <ref> --type <type> **kwargs

 will establish a database of the specified type (default: IlcdArchive) from the specified ref, and pass
 the remaining arguments to the initializer.


GaBi 2016 database index page:
http://www.gabi-software.com/support/gabi/gabi-database-2016-lci-documentation/
look for a elements in div class="csc-default"
"""

from __future__ import print_function, unicode_literals

import six

import json
import gzip
import re

# import sys
# from optparse import OptionParser

from lcatools.providers.ilcd import IlcdArchive
from lcatools.providers.ecoinvent_spreadsheet import EcoinventSpreadsheet
from lcatools.providers.ecospold import EcospoldV1Archive
from lcatools.providers.ecospold2 import EcospoldV2Archive
from lcatools.providers.gabi_web_catalog import GabiWebCatalog


def _archive_from_json(j):
    """

    :param j: json dictionary containing an archive
    :return:
    """
    if j['dataSourceType'] == 'IlcdArchive':
        if 'prefix' in j.keys():
            prefix = j['prefix']
        else:
            prefix = None

        a = IlcdArchive(j['dataSourceReference'], prefix=prefix, quiet=True)
    elif j['dataSourceType'] == 'EcospoldV1Archive':
        if 'prefix' in j.keys():
            prefix = j['prefix']
        else:
            prefix = None

        a = EcospoldV1Archive(j['dataSourceReference'], prefix=prefix, ns_uuid=j['nsUuid'], quiet=True)
    elif j['dataSourceType'] == 'EcospoldV2Archive':
        if 'prefix' in j.keys():
            prefix = j['prefix']
        else:
            prefix = None

        a = EcospoldV2Archive(j['dataSourceReference'], prefix=prefix, quiet=True)

    elif j['dataSourceType'] == 'EcoinventSpreadsheet':
        a = EcoinventSpreadsheet(j['dataSourceReference'], internal=bool(j['internal']), version=j['version'],
                                 ns_uuid=j['nsUuid'], quiet=True)
    else:
        raise ValueError('Unknown dataSourceType %s' % j['dataSourceType'])

    if 'catalogNames' in j:
        a.catalog_names = j['catalogNames']

    for e in j['quantities']:
        a.entity_from_json(e)
    for e in j['flows']:
        a.entity_from_json(e)
    for e in j['processes']:
        a.entity_from_json(e)
    if 'exchanges' in j:
        a.handle_old_exchanges(j['exchanges'])
    if 'characterizations' in j:
        a.handle_old_characterizations(j['characterizations'])
    a.check_counter('quantity')
    a.check_counter('flow')
    a.check_counter('process')
    return a


def from_json(fname, **kwargs):
    """
    Routine to reconstruct a catalog from a json archive.
    :param fname: json file, optionally gzipped
    :param kwargs: TBD
    :return: a subclass of ArchiveInterface
    """
    if bool(re.search('\.gz$', fname)):
        if six.PY3:
            with gzip.open(fname, 'rt') as fp:
                j = json.load(fp)
        else:
            with gzip.open(fname, 'r') as fp:
                j = json.load(fp)
    else:
        with open(fname, 'r') as fp:
            j = json.load(fp)

    if 'collectionType' in j.keys():
        if j['collectionType'] == 'GabiWebCatalog':
            g = GabiWebCatalog(j['collectionReference'], quiet=True)
        else:
            raise ValueError('Unknown collectionType %s' % j['collectionType'])
        for a in j['archives']:
            g.install_archive(_archive_from_json(a))
        return g

    else:
        return _archive_from_json(j)



# if __name__ == '__main__':
    """
    Usage: db_catalog action
    where action is one of the following:
         'gabi_2016_collection'
         'gabi_
    """

    """
    parser = OptionParser()
    parser.add_option("-t", "--type", dest="itype", default="gabi_set",
                      help="Specify the interface type used to interpret the reference")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=True,
                      help="Quiet operation (default)")
    parser.add_option("-v", "--verbose", action="store_false", dest="quiet",
                      help="Verbose operation (reverses -q)")
    parser.add_option("-p", "--prefix", dest="prefix",
                      help="accept an optional prefix argument to pass to the interface")

    (options, args) = parser.parse_args(sys.argv)

    if len(args) > 1:
        raise ValueError("Too many arguments supplied")
    ref = args[0]

    if options.itype == 'gabi_set':
        load_gabi_set(ref)
    elif options.itype == 'gabi_collection':
        load_gabi_collection(ref)
    else:
        raise ValueError("Unknown type argument %s" % options.itype)
    """



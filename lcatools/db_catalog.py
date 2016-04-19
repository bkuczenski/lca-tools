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

from urllib.request import urlopen, urljoin
from bs4 import BeautifulSoup
import os
import json
import gzip

# import sys
# from optparse import OptionParser

from lcatools.providers.gabi_web_catalog import GabiWebCatalog


def load_gabi_collection(url, version='', savedir='.'):
    collection_name = [f for f in filter(bool, url.split('/'))][-1]  # last chunk of url
    file_name = 'gabi_' + version + '_' + collection_name + '.json.gz'
    file_path = os.path.join(savedir, file_name)
    if os.path.exists(file_path):
        # future we can try "pick up where we left off"; for now we just bail if the file is already there
        return

    G = GabiWebCatalog(url, quiet=True)
    G.load_all()
    j = G.serialize(exchanges=True)
    with gzip.open(file_path, 'wt') as fp:
        print('Writing %s to %s...\n' % (collection_name, file_path))
        json.dump(j, fp, indent=2, sort_keys=True)
    return


def load_gabi_set(index, cname="csc-default", version='', savedir='.'):
    html = urlopen(index).read()
    dom = BeautifulSoup(html, 'lxml')

    base = dom.findAll('base')[0].attrs['href']

    if not os.path.exists(savedir):
        os.makedirs(savedir)

    # grab links to database browser pages
    links = []
    for d in dom.findAll('div', {"class": cname}):
        links.extend([x.attrs['href'] for x in d.findAll('a')])

    for link in links:
        print('Attempting to load %s...\n' % link)
        load_gabi_collection(urljoin(base, link), version=version, savedir=savedir)


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



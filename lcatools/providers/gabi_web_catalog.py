
from lcatools.providers.ilcd import IlcdArchive
from bs4 import BeautifulSoup

import os
import re

try:
    from urllib.request import urlopen, urljoin
except ImportError:
    from urllib2 import urlopen
    from urlparse import urljoin


def load_gabi_collection(url, version='', savedir='.'):
    collection_name = [f for f in filter(bool, url.split('/'))][-1]  # last chunk of url
    file_name = 'gabi_' + version + '_' + collection_name + '.json.gz'
    file_path = os.path.join(savedir, file_name)
    if os.path.exists(file_path):
        # future we can try "pick up where we left off"; for now we just bail if the file is already there
        G = from_json(file_path)
    else:
        G = GabiWebCatalog(url, quiet=True)

    G.catalog_names[collection_name] = url

    G.load_all()
    j = G.serialize(exchanges=True)
    with gzip.open(file_path, 'wt') as fp:
        print('Writing %s to %s...\n' % (collection_name, file_path))
        json.dump(j, fp, indent=2, sort_keys=True)
    return


def grab_db_browser_links(index, cname="csc-default"):
    html = urlopen(index).read()
    dom = BeautifulSoup(html, 'lxml')

    base = dom.findAll('base')[0].attrs['href']

    browser_divs = dom.findAll('div', {"class": cname})
    links = []
    for d in browser_divs:
        links.extend([x.attrs['href'] for x in d.findAll('a')])
    return base, links


def load_gabi_set(index, cname="csc-default", version='', savedir='.'):
    if not os.path.exists(savedir):
        os.makedirs(savedir)

    # grab links to database browser pages

    base, links = grab_db_browser_links(index, cname=cname)

    for link in links:
        print('Attempting to load %s...\n' % link)
        load_gabi_collection(urljoin(base, link), version=version, savedir=savedir)


class GabiWebCatalog(object):
    """
    An ILCD archive wrapper for a GaBi webpage that presents a process list inside an HTML table.

    This tool will create "one or more" IlcdArchive interfaces for each ILCD reference it finds in the table.

    """

    ilcd_re = re.compile('^(http.*/)processes/(.*)\.xml$')

    def _find_roots(self, element):
        """

        :param element:
        :return:
        """
        for a in element.findAll('a'):
            r = re.match(self.ilcd_re, a.attrs['href']).groups()[0]
            if len(r) == 0:
                continue
            if r not in self._roots:
                self._roots.add(r)

    def _generate_process_uuids(self, element):
        """

        :return:
        """
        for a in element.findAll('a'):
            parts = re.match(self.ilcd_re, a.attrs['href']).groups()
            if len(parts) == 0:
                continue
            if parts[0] not in self._roots:
                continue
            yield parts[0], parts[1]

    def __init__(self, web_ref, html_id='processListTable', quiet=False):
        """

        :param web_ref: http address of catalog page
        :param html_id: id tag of the document element that contains hrefs to follow
        :return:
        """
        self._roots = set()
        self.catalog = web_ref

        html = urlopen(web_ref).read()
        dom = BeautifulSoup(html, 'lxml')
        tables = dom.findAll(attrs={'id': html_id})

        for t in tables:
            self._find_roots(t)

        self.archive = []
        self._archives = dict()
        for root in self._roots:
            self.install_archive(IlcdArchive(root, prefix=None, quiet=quiet))

        # populate list of objects
        self._list_objects = []
        for t in tables:
            self._list_objects.extend([(archive, uuid) for archive, uuid in self._generate_process_uuids(t)])

    def load_all(self, count=None):
        if count is None:
            count = len(self._list_objects)
        for archive, uuid in self._list_objects:
            self._archives[archive].retrieve_or_fetch_entity(uuid, dtype='Process')
            count -= 1
            if count == 0:
                break

    def serialize(self, **kwargs):
        return {
            'collectionType': self.__class__.__name__,
            'collectionReference': self.catalog,
            'archives': [archive.serialize(**kwargs) for archive in self.archive]
        }

    def install_archive(self, archive):
        if archive.ref not in self._roots:
            raise ValueError('Reference %s not found' % archive.ref)
        self._archives[archive.ref] = archive
        self.archive = [self._archives[k] for k in self._roots]


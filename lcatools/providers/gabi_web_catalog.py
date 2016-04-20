
from lcatools.providers.ilcd import IlcdArchive
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re


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



from __future__ import print_function, unicode_literals

from eight import *

from collections import defaultdict

from lcatools.tools import gz_files, from_json

from lcatools.providers.ilcd import IlcdArchive
from lcatools.providers.ecoinvent_spreadsheet import EcoinventSpreadsheet
from lcatools.providers.ecospold import EcospoldV1Archive
from lcatools.providers.ecospold2 import EcospoldV2Archive
from lcatools.providers.ecoinvent_lcia import EcoinventLcia


class LcaCatalog(object):
    """
    Class for storing, searching, and referencing lca-tools archives. The catalog will be used to perform
    queries for foreground construction, and fragment traversal, scoring, and publishing.
    """

    def __init__(self, catalog_dir=None):
        self.archives = []  # a list of installed archives
        self._nicknames = dict()  # a mapping of nickname to archive index
        self._shortest = []  # a list of the shortest nickname for each archive
        if catalog_dir is not None:
            self._install_catalogs_from_dir(catalog_dir)

    def _install_catalogs_from_dir(self, dr):
        files, names = gz_files(dr)
        for i, f in enumerate(files):
            self._archive_from_json(from_json(f), names[i])

    def _set_shortest(self, k):
        self._shortest[k] = min([n for n, v in self._nicknames.items() if v == k], key=len)

    def _install_archive(self, a, nick, overwrite=False):
        if nick in self._nicknames:
            if overwrite:
                print('Overwriting archive "%s" with %s')
                self.archives[self._nicknames[nick]] = a
                self._set_shortest(self._nicknames[nick])
                return
            raise KeyError('Nickname %s already exists', nick)
        self.archives.append(a)
        self._nicknames[nick] = len(self.archives)
        self._shortest.append(nick)

    def _archive_from_json(self, j, nick, **kwargs):
        """

        :param j: json dictionary containing an archive
        :param nick: nickname to reference the archive
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

        elif j['dataSourceType'] == 'EcoinventLcia':
            a = EcoinventLcia(j['dataSourceReference'], ns_uuid=j['nsUuid'])

        else:
            raise ValueError('Unknown dataSourceType %s' % j['dataSourceType'])

        if 'catalogNames' in j:
            a.catalog_names = j['catalogNames']

        if 'upstreamReference' in j:
            print('**Upstream reference not resolved: %s\n' % j['upstreamReference'])
            a._serialize_dict['upstreamReference'] = j['upstreamReference']

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
        self._install_archive(a, nick, **kwargs)

    def load_json_archive(self, f, nick, **kwargs):
        self._archive_from_json(from_json(f), nick, **kwargs)

    def alias(self, nick, alias):
        """
        create an alias for an existing database, referenced by nickname
        :param nick:
        :param alias:
        :return:
        """
        if nick in self._nicknames:
            self._nicknames[alias] = self._nicknames[nick]
            self._set_shortest(self._nicknames[nick])
        else:
            print('Nickname %s not in catalog!' % nick)

    def show(self):
        l = max([len(k) for k in self._nicknames])
        nicks = defaultdict(list)
        for k, v in self._nicknames.items():
            nicks[v].append(k)
        print('LCA Catalog with the following archives:')
        for i, a in enumerate(self.archives):
            n = nicks[i]
            print('%*s: %s' % (l, n[0], a))
            n.remove(n[0])
            for k in n:
                print('%*s (alias)' % (l, k))

    def search(self, archive=None, **kwargs):
        """
        Search for a string in the catalog,
        :param archive: a nickname or a list of nicknames
        :param kwargs: search arguments passed to the archives- must be in the form of key=value.  Some useful
          keys include 'Name', 'Comment', 'Compartment', 'Classification'
        :return:
        """
        if archive is None:
            archive = self._shortest
        if isinstance(archive, str):
            archive = [archive]
        res_set = []
        for a in archive:
            i = self._nicknames[a]
            r = self.archives[i].search(**kwargs)
            if r is not None:
                res_set.extend([(self._shortest[i], t) for t in r])
        return res_set

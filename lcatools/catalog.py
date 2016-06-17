
from __future__ import print_function, unicode_literals

from eight import *

from collections import defaultdict

from lcatools.tools import gz_files, split_nick, archive_from_json



class LcaCatalog(object):
    """
    Class for storing, searching, and referencing lca-tools archives. The catalog will be used to perform
    queries for foreground construction, and fragment traversal, scoring, and publishing.
    """

    def __init__(self, catalog_dir=None):
        self.archives = []  # a list of installed archives
        self._nicknames = dict()  # a mapping of nickname to archive index
        self._shortest = []  # a list of the shortest nickname for each archive
        self._sources_loaded = dict()  # map input source to archive
        self._refs_loaded = dict()  # map archive.ref to archive
        if catalog_dir is not None:
            self._install_catalogs_from_dir(catalog_dir)

    def _install_catalogs_from_dir(self, dr):
        files = gz_files(dr)
        for i, f in enumerate(files):
            self.load_json_archive(f)

    def _set_shortest(self, k):
        self._shortest[k] = min([n for n, v in self._nicknames.items() if v == k], key=len)

    def _install_archive(self, a, source, nick=None, overwrite=False):
        """
        overwrite should always be false except on data reload
        (i.e. changing sources inplace is a user error)
        :param a:
        :param nick:
        :param overwrite:
        :return:
        """
        if source in self._sources_loaded:
            if overwrite:
                print('Overwriting archive "%s" with %s')
                self.archives[self._sources_loaded[source]] = a
                self._set_shortest(self._sources_loaded[source])
                return
            else:
                print('Archive already exists and overwrite is false.')
                return
        if nick is None:
            nick = split_nick(source)
        if nick in self._nicknames:
            raise KeyError('Nickname %s already exists', nick)
        self.archives.append(a)
        self._shortest.append(nick)
        assert len(self.archives) == len(self._shortest)
        self._nicknames[nick] = len(self.archives) - 1
        self._sources_loaded[source] = len(self.archives) - 1
        self._refs_loaded[a.ref] = len(self.archives) - 1

    def load_json_archive(self, f, **kwargs):
        if f in self._sources_loaded:
            print('source %s already loaded' % f)
            if 'overwrite' not in kwargs or kwargs['overwrite'] is False:
                print('overwrite=True to overwrite')
                return
            else:
                # if overwrite is true- go ahead and load it
                pass

        a = archive_from_json(f)
        self._install_archive(a, f, **kwargs)

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

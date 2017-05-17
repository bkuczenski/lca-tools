"""
The StudyManager is an LcCatalog that has further abilities to deal with LcStudies: to create and compute with
 fragments.

"""
import os
import json

from lcatools.catalog.catalog import LcCatalog
from lcatools.providers.foreground import LcForeground, AmbiguousReference
from lcatools.entities.editor import FragmentEditor


class ForegroundCatalog(LcCatalog):
    """
    A foreground manager is a catalog that adds in a set of foregrounds to the set of known references.
    The resolver is still used to locate inventory data and static content by reference, and the quantity database is
    still used to provide basic data for LCIA (and flow properties generally).

    But in addition there is a collection of LcForeground interfaces, each of which is mapped to a specific directory
    or Antelope instance.
    """
    @property
    def _known_foregrounds(self):
        return os.path.join(self._rootdir, 'known_foregrounds.json')

    def __init__(self, catalog_dir, qdb=None):
        super(ForegroundCatalog, self).__init__(catalog_dir, qdb=qdb)
        self._foregrounds = dict()  # _foregrounds := name --> path
        self._known_fgs = dict()
        self._ed = FragmentEditor(qdb=self._qdb, interactive=False)

    def add_foreground(self, name, path):
        """
        A foreground needs a short name and a path to a local folder which stores the entities and fragments.
        The foreground's reference in the catalog will become 'foreground.name'
        :param name: functions as a reference specifier
        :param path: stores entities.json and the fragments directory
        :return: I suppose it could return a foreground interface.. we'll see
        """
        if path in self._foregrounds.values():
            print('Path is already registered to name %s' % next(k for k, v in self._foregrounds if v == path))
            return
        if path in self._archives:
            raise KeyError('Source collision')
        if name in self._foregrounds:
            raise ValueError('Foreground name is already registered')
        ref = '.'.join(['foreground', name])
        f = LcForeground(path, ref=ref)
        self._archives[path] = f
        self._names[ref] = path
        self._nicknames[name] = path
        self._foregrounds[ref] = path
        return f

    @property
    def foregrounds(self):
        for k, v in self._foregrounds.items():
            yield v

    def show_foregrounds(self):
        for k, v in self._foregrounds.items():
            print('%s [%s]' % (k, v))

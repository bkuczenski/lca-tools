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
        self._foregrounds = dict()  # _foregrounds := name --> path of LOADED foregrounds
        self._known_fgs = dict()  # := name --> path of KNOWN foregrounds
        self._ed = FragmentEditor(qdb=self._qdb, interactive=False)
        self._load_known_foregrounds()

    def add_foreground(self, name, path=None):
        """
        A foreground needs a short name and a path to a local folder which stores the entities and fragments.
        The foreground's reference in the catalog will become 'foreground.name'
        :param name: functions as a reference specifier
        :param path: stores entities.json and the fragments directory
        :return: I suppose it could return a foreground interface.. we'll see
        """
        if name.split('.')[0] != 'foreground':
            ref = '.'.join(['foreground', name])
        else:
            ref = name
        if path is None:
            path = self._known_fgs[ref]
        if path in self._foregrounds.values():
            print('Path is already loaded with name %s' % next(k for k, v in self._foregrounds if v == path))
            return
        if path in self._archives:
            raise KeyError('Source collision')
        if name in self._foregrounds:
            raise ValueError('Foreground name is already registered')
        return self._load_foreground(ref, path)

    def _load_foreground(self, ref, path):
        name = '.'.join(ref.split('.')[1:])
        f = LcForeground(path, catalog=self, ref=ref)
        self._archives[path] = f
        self._names[ref] = path
        self._nicknames[name] = path
        self._foregrounds[ref] = path
        self._known_fgs[ref] = path
        self._save_known_foregrounds()
        return f

    def _load_known_foregrounds(self):
        if os.path.exists(self._known_foregrounds):
            with open(self._known_foregrounds) as fp:
                self._known_fgs = json.load(fp)

    def _save_known_foregrounds(self):
        with open(self._known_foregrounds, 'w') as fp:
            json.dump(self._known_fgs, fp, sort_keys=True, indent=2)

    @property
    def foregrounds(self):
        for k, v in self._foregrounds.items():
            yield v

    def show_foregrounds(self):
        for k, v in self._foregrounds.items():
            print('%s [%s]' % (k, v))

    def _ensure_foreground(self, ref, path):
        if path not in self._archives:
            return self._load_foreground(ref, path)
        return self._archives[path]

    def ensure_foreground(self, ref):
        self._ensure_foreground(ref, self._known_fgs[ref])

    def _retrieve(self, req, external_ref):
        """
        Analogous to _dereference except for foreground content.
        :param req: foreground origin or partial origin
        :param external_ref:
        :return:
        """
        terms = req.split('.')
        ent = None
        for ref, path in self._known_fgs.items():
            if ref.split('.')[:len(terms)] == terms:
                fg = self._ensure_foreground(ref, path)
                try:
                    ent = fg[external_ref]
                    break
                except KeyError:
                    continue
        return ent

    def lookup(self, ref):
        """
        Here I am making a dangerous decision to treat foregrounds differently
        :param ref:
        :return:
        """
        orgs = set()
        if ref.origin.split('.')[0] == 'foreground':
            f = self._retrieve(ref.origin, ref.external_ref)
            if f is not None:
                orgs.add(f.origin)
            return list(orgs)
        return super(ForegroundCatalog, self).lookup(ref)

    def fetch(self, ref):
        ent = self._retrieve(ref.origin, ref.external_ref)
        if ent is None:
            ent = super(ForegroundCatalog, self).fetch(ref)
        return ent

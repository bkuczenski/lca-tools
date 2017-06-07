"""
The StudyManager is an LcCatalog that has further abilities to deal with LcStudies: to create and compute with
 fragments.

"""
import os
import json

from lcatools.catalog.catalog import LcCatalog
from lcatools.providers.foreground import LcForeground  # , AmbiguousReference
from lcatools.entities.editor import FragmentEditor
from lcatools.lcia_results import LciaResult
from lcatools.exchanges import comp_dir, ExchangeValue
from lcatools.characterizations import Characterization
from lcatools.terminations import SubFragmentAggregation


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

    @property
    def known_foregrounds(self):
        return self._known_fgs

    def __init__(self, catalog_dir, qdb=None):
        super(ForegroundCatalog, self).__init__(catalog_dir, qdb=qdb)
        self._foregrounds = dict()  # _foregrounds := name --> path of LOADED foregrounds
        self._known_fgs = dict()  # := name --> path of KNOWN foregrounds
        self.ed = FragmentEditor(qdb=self._qdb, interactive=False)
        self._load_known_foregrounds()

    def __getitem__(self, item):
        """
        Retrieve foreground by name, or entity by catalog ref
        :param item:
        :return:
        """
        if isinstance(item, str):
            ref = self._ensure_ref(item)
            self.ensure_foreground(ref)
            return self._archives[self._foregrounds[ref]]
        else:
            return self.fetch(item)

    @staticmethod
    def _ensure_ref(name):
        """
        Ensures that the reference begins with 'foreground', and prepends it if it does not
        :param name:
        :return:
        """
        if name.split('.')[0] != 'foreground':
            ref = '.'.join(['foreground', name])
        else:
            ref = name
        return ref

    def add_foreground(self, name, path=None):
        """
        A foreground needs a short name and a path to a local folder which stores the entities and fragments.
        The foreground's reference in the catalog will become 'foreground.name'
        :param name: functions as a reference specifier
        :param path: stores entities.json and the fragments directory
        :return: I suppose it could return a foreground interface.. we'll see
        """
        ref = self._ensure_ref(name)
        if path is None:
            path = self._known_fgs[ref]
        if path in self._foregrounds.values():
            print('Path is already loaded with name %s' % next(k for k, v in self._foregrounds if v == path))
            return
        if path in self._archives:
            raise KeyError('Source collision')
        if ref in self._foregrounds:
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
        ref = self._ensure_ref(ref)
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
        Here I am making a dangerous decision to treat foregrounds differently - no interface, no abstraction
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

    def fragment_lcia(self, fragmentflows, q_ref):
        """
        Takes a stack of FragmentFlow objects and performs LCIA for the given quantity.
        :param fragmentflows:
        :param q_ref:
        :return:
        """
        q = self.fetch(q_ref)
        if not self._qdb.is_loaded(q_ref):
            self.load_lcia_factors(q_ref)
        result = LciaResult(q)
        for ff in fragmentflows:
            if ff.term.is_null:
                continue

            if ff.node_weight == 0:
                continue

            try:
                v = ff.term.score_cache(quantity=q, qdb=self._qdb)
            except SubFragmentAggregation:
                v = self.fragment_lcia(ff.term.subfragments, q_ref)
            value = v.total()
            if value == 0:
                continue

            if ff.term.direction == ff.fragment.direction:
                # if the directions collide (rather than complement), the term is getting run in reverse
                value *= -1

            result.add_component(ff.fragment.uuid, entity=ff)
            x = ExchangeValue(ff.fragment, ff.term.term_flow, ff.term.direction, value=ff.node_weight)
            try:
                l = ff.term.term_node['SpatialScope']
            except KeyError:
                l = None
            f = Characterization(ff.term.term_flow, q, value=value, location=l)
            result.add_score(ff.fragment.uuid, x, f, l)
        return result


from __future__ import print_function, unicode_literals

import json
import os

from eight import *

from collections import defaultdict, namedtuple

from lcatools.interfaces import to_uuid

# from lcatools.characterizations import CharacterizationSet  # , Characterization
# from lcatools.logical_flows import LogicalFlow, ExchangeRef
# from lcatools.entities import LcProcess, LcFlow


from lcatools.tools import split_nick, archive_from_json, archive_factory


ExchangeRef = namedtuple('ExchangeRef', ('index', 'exchange'))
CFRef = namedtuple('CFRef', ('index', 'characterization'))
ArchiveRef = namedtuple('ArchiveRef', ['source', 'nicknames', 'dataSourceType', 'parameters'])

DEFAULT_CATALOG = os.path.join(os.path.dirname(__file__), 'default_catalog.json')


def get_entity_uuid(item):
    if to_uuid(item) is not None:
        return item
    if hasattr(item, 'get_uuid'):
        return item.get_uuid()
    raise TypeError('Don\'t know how to get ID from %s' % type(item))


class CatalogRef(object):
    """
    A catalog ref is a catalog-specific reference which consists of an archive index and an entity ID.
    Given the proper catalog, catalog[index][entity_id] returns the corresponding entity
    """
    def __init__(self, catalog, index, entity_id):
        self.catalog = catalog
        self.index = catalog.get_index(index)
        self.id = get_entity_uuid(entity_id)

    def __iter__(self):
        for k in (self.index, self.id):
            yield k

    @property
    def archive(self):
        return self.catalog[self.index]

    def names(self):
        for n in (self.id, str(self.entity()), self):
            yield n

    def keys(self):
        return self.entity().keys()

    def show(self):
        print('Catalog reference: %s' % self.catalog.label(self.index))
        self.entity().show()

    def get_uuid(self):
        return self.id

    def __getitem__(self, item):
        return self.entity().__getitem__(item)

    def entity(self):
        return self.catalog[self.index][self.id]

    @property
    def entity_type(self):
        return self.entity().entity_type

    def __str__(self):
        e = self.entity()
        if e is None:
            return '(%s) {%1.1s} %s' % (self.catalog.name(self.index), '-', self.id)
        return '(%s) {%1.1s} %s' % (self.catalog.name(self.index), e.entity_type, e)

    def __hash__(self):
        return hash((self.index, self.id))

    def __eq__(self, other):
        return (self.catalog is other.catalog) and (self.index == other.index) and (self.id == other.id)

    def validate(self, catalog):
        """

        :param catalog:
        :return:
        """
        if catalog is not self.catalog:
            raise ValueError('CatalogRef refers to a different catalog!')
        if self.entity() is None:
            raise KeyError('CatalogRef does not resolve!')

    def serialize(self):
        return {
            'index': self.index,
            'entity': self.id
        }


class CatalogInterface(object):
    """
    A catalog stores a list of archives and exposes retrieval methods that return entity references
    as CatalogRef objects
    """
    @classmethod
    def new(cls, fg_dir=None):
        with open(DEFAULT_CATALOG) as fp:
            return cls.from_json(json.load(fp), fg_dir=fg_dir)

    @classmethod
    def from_json(cls, j, fg_dir=None):
        """
        Create a catalog and populate it with archives as a set of ArchiveRefs specified in a json file.

        Foreground entry is omitted; index starts at 1. fg dir can be specified on the command line.
        :return:
        """
        catalog = cls(foreground_dir=fg_dir)
        for i in range(1, len(j['catalogs']) + 1):
            try:
                cat = [c for c in j['catalogs'] if c['index'] == i][0]
            except IndexError:
                print('Index error at %d' % i)
                break
            if 'nicknames' in cat.keys():
                nicks = cat['nicknames']
            else:
                nicks = [None]
            if 'parameters' in cat.keys():
                params = cat['parameters']
                if params is None:
                    params = dict()
            else:
                params = dict()

            catalog.add_archive(cat['source'], nicks, cat['dataSourceType'], **params)

        return catalog

    def __init__(self, foreground_dir=None):
        self.archives = []  # a list of archives
        self._archive_refs = []  # archive references
        self._nicknames = dict()  # a mapping of nickname to archive index
        self._shortest = []  # a list of the shortest nickname for each archive
        self._sources = dict()  # map input source to archive

        self._loaded = []  # list of booleans
        self._refs_loaded = dict()  # map archive.ref to archive

        self.add_archive(foreground_dir, 'FG', 'ForegroundArchive')

    def __len__(self):
        return len(self.archives)

    @staticmethod
    def _purge_entries_for(d, index):
        old = [k for k, v in d.items() if v == index]
        for o in old:
            d.pop(o)

    @property
    def fg(self):
        return self._archive_refs[0].source

    def set_foreground_dir(self, fg_dir):
        ar = ArchiveRef(source=fg_dir, nicknames=self._archive_refs[0].nicknames,
                        dataSourceType='ForegroundArchive', parameters={'quiet': True})
        self._purge_entries_for(self._sources, 0)
        self._purge_entries_for(self._refs_loaded, 0)
        self._loaded[0] = False
        self._archive_refs[0] = ar
        self.archives[0] = None
        self._sources[fg_dir] = 0

    def _update_nicks(self, item):
        index = self.get_index(item)
        for nick in self._archive_refs[index].nicknames:
            self._nicknames[nick] = index

        self._shortest[item] = min([n for n in self._archive_refs[index].nicknames])

    def _new_archive(self, archive_ref):
        k = len(self.archives)
        assert k == len(self._archive_refs) == len(self._shortest) == len(self._loaded)
        assert isinstance(archive_ref, ArchiveRef)
        self.archives.append(None)
        self._loaded.append(False)
        self._archive_refs.append(archive_ref)
        self._shortest.append(None)
        self._update_nicks(k)
        self._sources[archive_ref.source] = k
        return k

    def add_archive(self, source, nicknames, ds_type, **kwargs):
        if source in self._sources.keys():
            print('Data source %s already listed as %s' % (source, [k for k, v in self._nicknames.items()
                                                                    if v == self._sources[source]]))
            return self._sources[source]
        nicks = set()
        if nicknames is None or len(nicknames) == 0:
            nicks.add(split_nick(source))
        elif isinstance(nicknames, str):
            nicks.add(nicknames)
        else:
            for i in nicknames:
                nicks.add(i)

        ar = ArchiveRef(source=source, nicknames=nicks, dataSourceType=ds_type, parameters=kwargs)
        k = self._new_archive(ar)
        print('%s archive added in position %d' % (ds_type, k))
        return k

    def add_nick(self, item, nick):
        index = self.get_index(item)
        self._archive_refs[index].nicknames.add(nick)
        self._update_nicks(index)

    def update_params(self, item, **kwargs):
        index = self.get_index(item)
        self._archive_refs[index].parameters.update(**kwargs)

    def ref(self, index, item):
        return CatalogRef(self, index, item)

    def get_index(self, item):
        if isinstance(item, int):
            return item
        elif isinstance(item, str):
            return self._nicknames[item]
        else:
            raise TypeError('unhandled type %s' % type(item))

    def name(self, index):
        return self._shortest[self.get_index(index)]

    def label(self, item):
        index = self.get_index(item)
        return '%s' % self.archives[index] or self._archive_refs[index].source

    def __getitem__(self, item):
        """
        catalog[ref] - if ref is an integer, index into self.archives
        if ref is a string, interpret as a nickname
        :param item:
        :return: an archive
        """
        return self.archives[self.get_index(item)]

    def _set_upstream(self, archive):
        ref = archive.query_upstream_ref()
        if ref is not None:
            if ref in self._refs_loaded:
                archive.set_upstream(self[self._refs_loaded[ref]])
                print('Upstream ref %s found and linked' % ref)
            else:
                print('Upstream ref %s not found in catalog!' % ref)
                # because of lost upstream, need to rewrite uuid to match local index
                archive.truncate_upstream()
                # [maybe this should be done in general?]

    def _install(self, index, archive):
        self.archives[index] = archive
        self._refs_loaded[archive.ref] = index
        self._loaded[index] = True

    def install_archive(self, archive, nick, **kwargs):
        """
        install an already-loaded archive currently in memory
        :param archive:
        :param nick:
        :param kwargs:
        :return:
        """
        index = self.add_archive(archive.ref, nick, archive.__class__.__name__, **kwargs)
        self._install(index, archive)

    def load(self, item, reload=False):
        """
        instantiate the archive ref.
        :param item:
        :param reload:
        :return:
        """
        index = self.get_index(item)
        if self._loaded[index] is True and reload is False:
            print('Archive already loaded; specify reload=True to rewrite')
            return

        source = self._archive_refs[index].source
        ds_type = self._archive_refs[index].dataSourceType
        params = self._archive_refs[index].parameters
        if ds_type.lower() == 'json':
            a = archive_from_json(source, **params)
        else:
            a = archive_factory(source, ds_type, **params)
        self._install(index, a)

    def load_all(self, item):
        """
        run the archive's load_all() function
        :param item:
        :return:
        """
        index = self.get_index(item)
        if self._loaded[index] is False:
            self.load(index)
        self.archives[index].load_all()

    def show(self):
        l = max([len(k) for k in self._shortest])
        print('\nLCA Catalog with the following archives:')
        for i, a in enumerate(self.archives):
            ldd = 'X' if self._loaded[i] else ' '
            print('%s [%2d] %-*s: %s' % (ldd, i, l, self._shortest[i], a or self._archive_refs[i].source))
            """
            n.remove(n[0])
            for k in n:
                print('%*s (alias)' % (l, k))
            """

    def is_loaded(self, index):
        if index is None:
            return True  # a bit smelly.  But 'None' really means 'all loaded archives' so it's true reflexively
        return self._loaded[self.get_index(index)]

    def show_loaded(self):
        l = max([len(k) for k in self._shortest])
        print('\nCurrently loaded archives: ')
        num_loaded = 0
        for i, a in enumerate(self.archives):
            if self._loaded[i]:
                num_loaded += 1
                print('X [%2d] %-*s: %s' % (i, l, self._shortest[i], a or self._archive_refs[i].source))
        return num_loaded

    def save_default(self):
        with open(DEFAULT_CATALOG, 'w') as fp:
            json.dump(self.serialize(), fp, indent=2)
            print('Default catalog saved to %s' % DEFAULT_CATALOG)

    def retrieve(self, archive, key):
        """
        Method to retrieve a specific, concretely identified entity from a single archive.  the provided key
        must be interpretable by the archive's key_to_id method (i.e. either a UUID or a valid external reference)

        If archive is None, the catalog will iterate through them and return the first match.

        separate method to retrieve all elements for a given ID- this function only retrieves a single catalog ref.
        :param archive:
        :param key:
        :return:
        """
        if archive is not None:
            entity = self[archive][key]
            if entity is not None:
                return CatalogRef(self, archive, entity)
            return None
        else:
            r = None
            for i, a in enumerate(self.archives):
                entity = a[key]
                if entity is not None:
                    r = CatalogRef(self, i, entity)
                    break
            return r

    def processes_for(self, item):
        index = self.get_index(item)
        return [self.ref(index, p.get_uuid()) for p in self.archives[index].processes()]

    def flows_for(self, item):
        index = self.get_index(item)
        return [self.ref(index, p.get_uuid()) for p in self.archives[index].flows()]

    def quantities_for(self, item):
        index = self.get_index(item)
        return [self.ref(index, p.get_uuid()) for p in self.archives[index].quantities()]

    @staticmethod
    def _show(res):
        for i, k in enumerate(res):
            print('[%2d] %s ' % (i, k))

    def search(self, archive=None, etype=None, show=False, **kwargs):
        """
        Search for a string in the catalog,
        :param archive: a nickname or a list of nicknames
        :param etype: positional shortcut for entity_type=x
        :param show: [False] if True, print a tabular list of results to stdout
        :param kwargs: search arguments passed to the archives- must be in the form of key=value.  Some useful
        keys include 'Name', 'Comment', 'Compartment', 'Classification'
        :return:
        """
        if archive is None:
            archive = [i for i, k in enumerate(self._loaded) if k is True]
        if etype is not None:
            if 'entity_type' in kwargs:
                raise KeyError('colliding entity_type and etype!')
            kwargs['entity_type'] = etype
        if isinstance(archive, str) or isinstance(archive, int):
            # turn single references into a list
            archive = [archive]
        res_set = []
        for a in archive:
            i = self.get_index(a)
            r = self.archives[i].search(**kwargs)
            if r is not None:
                res_set.extend([CatalogRef(self, i, t.get_uuid()) for t in r])
        if show:
            self._show(res_set)
        return res_set

    def _check_exchanges(self, index, flow_id, dirn, show=False):
        z = []
        for p in self[index].processes():
            if any([x.flow.get_uuid() == flow_id and x.direction == dirn
                    for x in p.exchanges()]):
                z.append(self.ref(index, p))
        if show:
            self._show(z)
        return z

    def terminate(self, exch_ref, show=False):
        """
        for some reason, doing this as a list comprehension didn't work
        :param exch_ref: an ExchangeRef
        :param show: [False] display
        :return:
        """
        return self._check_exchanges(exch_ref.index, exch_ref.exchange.flow.get_uuid(),
                                     exch_ref.exchange.comp_dir, show=show)

    def originate(self, exch_ref, show=False):
        """
        for some reason, doing this as a list comprehension didn't work
        :param exch_ref: an ExchangeRef
        :param show: [False] display
        :return:
        """
        return self._check_exchanges(exch_ref.index, exch_ref.exchange.flow.get_uuid(),
                                     exch_ref.exchange.direction, show=show)

    def source(self, flow_ref, show=False):
        return self._check_exchanges(flow_ref.index, flow_ref.id, 'Output', show=show)

    def sink(self, flow_ref, show=False):
        return self._check_exchanges(flow_ref.index, flow_ref.id, 'Output', show=show)

    def _serialize_archive(self, item):
        index = self.get_index(item)
        ar = self._archive_refs[index]._asdict()
        ar['index'] = index
        ar['nicknames'] = list(ar['nicknames'])
        return ar

    def serialize(self):
        return {
            "catalogs": sorted([self._serialize_archive(index) for index in range(1, len(self.archives))],
                               key=lambda x: x['index'])
        }

'''
class ProcessFlowInterface(object):
    """
    a ProcessFlow interface creates a standard mechanism to answer inventory queries.  The main purpose of the
      interface is to return exchanges for a given process or flow.

    The interface provides a *dictionary of logical flows* and allows the user to specify *synonyms*.

    The following queries are
     [to be] supported:

     - given a process, return all exchanges [this comes for free]

     - given a flow

    def list_processes(self):
        r = []
        for k, v in self.catalogs.items():
            if v['EntityType'] == 'process':
                r.append(v.get_signature())
        return sorted(r)

    def exchanges(self, dataframe=False):
        x = [ex for ex in self._exchanges]
        if dataframe:
            pass  # return self._to_pandas(x, Exchange)
        return x
    """
    def __init__(self, catalog):
        self._catalog = catalog
        self._flows = dict()

    def add_archive(self, index):
        for p in self._catalog[index].processes():
            self.add_exchanges(index, p.exchanges())

    def add_exchanges(self, index, exchanges):
        for x in exchanges:
            f_id = x.flow.get_uuid()
            key = CatalogRef(self._catalog, index, f_id)
            if f_id in self._flows:
                self._flows[f_id].add_ref(key)
            else:
                self._flows[f_id] = LogicalFlow.create(key)
            self._flows[f_id].add_exchange(key, x)

    def exchanges(self, cat_ref):
        """
        :param cat_ref: a catalog reference
        :return: a generator that produces exchanges
        """
        entity = cat_ref.entity()
        if isinstance(entity, LcProcess):
            # need to upgrade process exchanges to ExchangeRefs
            return (ExchangeRef(cat_ref.index, x) for x in entity.exchanges())
        elif isinstance(entity, LcFlow):
            cat_ref.validate(self._catalog)
            return self._flows[cat_ref.id].exchanges()

    def characterizations(self, cat_ref):
        cat_ref.validate(self._catalog)
        return self._flows[cat_ref.id].characterizations()


class FlowQuantityInterface(object):
    """
    A Flow-Quantity service stores linked observations of flows and quantities with "factors" which report the
     magnitude of the quantity, in proportion to the flow's reference quantity (which is implicitly mass in
     the ecoinvent LCIA spreadsheet).

    The flow-quantity interface allows the following:

      * add_cf : register a link between a flow and a quantity having a particular factor

      * lookup_cf : specify characteristics to match and return a result set

      *

      * report characterizations that link one flow with one quantity.




    """
    def __init__(self, catalog):
        self._catalog = catalog

        self._characterizations = CharacterizationSet()  # set of flow characterizations among the entities

    def _add_characterization(self, characterization):
        if characterization.entity_type == 'characterization':
            self._characterizations.add(characterization)

    def characterizations(self, dataframe=False):
        x = [ex for ex in self._characterizations]
        if dataframe:
            pass  # return self._to_pandas(x, Characterization)
        return x
'''
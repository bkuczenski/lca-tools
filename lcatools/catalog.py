
from __future__ import print_function, unicode_literals

from eight import *

from collections import defaultdict  # , namedtuple

from lcatools.interfaces import to_uuid

from lcatools.characterizations import CharacterizationSet  # , Characterization
from lcatools.logical_flows import LogicalFlow, ExchangeRef
from lcatools.entities import LcProcess, LcFlow
from lcatools.tools import gz_files, split_nick, archive_from_json, archive_factory


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

    def names(self):
        for n in (self.id, str(self.entity()), self):
            yield n

    def entity(self):
        return self.catalog[self.index][self.id]

    def entity_type(self):
        return self.entity().entity_type

    def __str__(self):
        e = self.entity()
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
    def from_json(cls, j):
        """
        Create a catalog and populate it with archives as specified in a json file.

        The format should be as follows:
        {
          'catalogs': [
            {
              'index': 0,
              'dataSourceType': '...',
              'source': '/path/to/file',
              'nicknames': [
                ...
              ],
              'parameters':
              {
                'param': 'value',
                ...
              }
            },
            ...
          ]
        }

        j['dataSourceType'].lower() should be either 'json' or something understood by archive_factory.

         * If 'json', then source points to the json file and 'parameters' is ignored.

         * else, source points to the archive ref, and 'parameters' are passed as keyword arguments to archive_factory

        In either case, each nickname is associated with the catalog once it's loaded.

        Every index from 0 up to len(j['catalogs']) must be present. Only sequential indices starting from 0 will
         be loaded.
        :return:
        """
        catalog = cls()
        for i in range(len(j['catalogs'])):
            try:
                cat = [c for c in j['catalogs'] if c['index'] == i][0]
            except IndexError:
                break
            if 'nicknames' in cat.keys():
                nicks = cat['nicknames']
            else:
                nicks = [None]

            source = cat['source']

            assert i == len(catalog.archives), "something's wrong with archive loading: sequence is corrupted"
            if cat['dataSourceType'].lower() == 'json':
                catalog.load_json_archive(source, nick=nicks[0])
            else:
                if 'parameters' in cat.keys():
                    params = cat['parameters']
                else:
                    params = dict()
                a = archive_factory(source, cat['dataSourceType'], **params)
                catalog._install_archive(a, source, nick=nicks[0])
            if len(nicks) > 1:
                for k in range(1, len(nicks)):
                    catalog.alias(nicks[0], nicks[k])

    def __init__(self):
        self.archives = []  # a list of installed archives
        self._nicknames = dict()  # a mapping of nickname to archive index
        self._shortest = []  # a list of the shortest nickname for each archive
        self._sources_loaded = dict()  # map input source to archive
        self._refs_loaded = dict()  # map archive.ref to archive
        self._from_json = []
        self._sources = []
        self._params = []

    def get_index(self, item):
        if isinstance(item, int):
            return item
        elif isinstance(item, str):
            return self._nicknames[item]
        else:
            raise TypeError('unhandled type %s' % type(item))

    def name(self, index):
        return self._shortest[self.get_index(index)]

    def __getitem__(self, item):
        """
        catalog[ref] - if ref is an integer, index into self.archives
        if ref is a string, interpret as a nickname
        :param item:
        :return: an archive
        """
        return self.archives[self.get_index(item)]

    def _install_catalogs_from_dir(self, dr):
        files = gz_files(dr)
        for i, f in enumerate(files):
            self.load_json_archive(f)

    def _set_shortest(self, k):
        self._shortest[k] = min([n for n, v in self._nicknames.items() if v == k], key=len)

    def _set_upstream(self, archive):
        ref = archive.query_upstream_ref()
        if ref is not None:
            if ref in self._refs_loaded:
                archive.set_upstream(self[self._refs_loaded[ref]])
                print('Upstream ref %s found and linked' % ref)
            else:
                print('Upstream ref %s not found in catalog!' % ref)

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
        print('Installing %s archive in position %d', a.__class__.__name__, len(self.archives))
        self.archives.append(a)
        self._sources.append(source)
        self._shortest.append(nick)
        assert len(self.archives) == len(self._shortest)
        new_index = len(self.archives) - 1
        self._nicknames[nick] = new_index
        self._sources_loaded[source] = new_index
        self._refs_loaded[a.ref] = new_index
        self._params.append(None)
        self._from_json.append(False)
        return new_index

    def install_archive(self, archive, **kwargs):
        self._install_archive(archive, 'memory', **kwargs)

    def load_archive(self, ref, ds_type, overwrite=False, nick=None, **kwargs):
        if ds_type.lower() == 'json':
            self.load_json_archive(ref, overwrite=overwrite, nick=nick)
        else:
            a = archive_factory(ref, ds_type, **kwargs)
            new_index = self._install_archive(a, ref, overwrite=overwrite, nick=nick)
            self._params[new_index] = kwargs

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
        new_index = self._install_archive(a, f, **kwargs)
        self._set_upstream(a)
        self._from_json[new_index] = True

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
            print('[%2d] %-*s: %s' % (i, l, n[0], a))
            n.remove(n[0])
            for k in n:
                print('%*s (alias)' % (l, k))

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

    def search(self, archive=None, show=False, **kwargs):
        """
        Search for a string in the catalog,
        :param archive: a nickname or a list of nicknames
        :param show: [False] if True, print a tabular list of results to stdout
        :param kwargs: search arguments passed to the archives- must be in the form of key=value.  Some useful
        keys include 'Name', 'Comment', 'Compartment', 'Classification'
        :return:
        """
        if archive is None:
            archive = range(len(self.archives))
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
            for i, k in enumerate(res_set):
                print('[%2d] %s ' % (i, k))
        return res_set

    def _serialize_archive(self, index):
        nicks = [k for k, v in self._nicknames.items() if v == index]
        if self._from_json[index]:
            return {
                'index': index,
                'dataSourceType': 'JSON',
                'source': self._sources[index],
                'nicknames': nicks
            }
        else:
            return {
                'index': index,
                'dataSourceType': self.archives[index].__class__.__name__,
                'source': self._sources[index],
                'nicknames': nicks,
                'parameters': self._params[index]
            }

    def serialize(self):
        return {
            "catalogs": sorted([self._serialize_archive(index) for index in range(len(self.archives))],
                               key=lambda x: x['index'])
        }


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

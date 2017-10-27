"""
The LcCatalog provides a semantic interface to a collection of (local and remote) read-only LcArchives, which provide
access to physical data.

It is made up of the following components:

  * A resolver, which translates semantic references into resources.  Input: semantic ref. output: CatalogInterface.
  * an archive factory, which creates static archives on demand based on resource information from the resolver
  * An internal cache of entities retrieved, by full reference

From the catalog_ref file, the catalog should meet the following spec:
          Automatic - entity information
           catalog.lookup(origin, external_ref) - returns [bool, bool, bool] -> catalog, fg, bg avail.
           catalog.entity_type(origin, external_ref) - returns entity type
           catalog.fetch(origin, external_ref) - return catalog entity (fg preferred; fallback to entity)

          LC Queries:
           catalog.terminate(origin, external_ref, direction)
           catalog.originate(origin, external_ref, direction)
           catalog.mix(origin, external_ref, direction)
           catalog.exchanges(origin, external_ref)
           catalog.exchange_values(origin, external_ref, flow, direction, termination=termination)
           catalog.exchange_values(origin, external_ref, ref_flow, exch_flow, direction,
                        termination=termination)
           catalog.ad(origin, external_ref, ref_flow)
           catalog.bf(origin, external_ref, ref_flow)
           catalog.lci(origin, external_ref, ref_flow)
           catalog.lcia(origin, external_ref, ref_flow, lcia_qty)

For each one, the first thing the catalog must do is resolve the origin to a static archive and determine whether the
entity is available.
"""

import re
import os
from shutil import copy2
import hashlib
# from collections import defaultdict

from lcatools.interfaces.iquery import CatalogQuery, EntityNotFound
from .lc_resolver import LcCatalogResolver
from .lc_resource import LcResource
from lcatools.providers.qdb import Qdb
from lcatools.flowdb.compartments import REFERENCE_INT  # reference intermediate flows
from lcatools.tables.flowables import FlowablesGrid
from lcatools.entities.editor import FragmentEditor


_protocol = re.compile('^(\w+)://')


class DuplicateEntries(Exception):
    pass


class LcCatalog(object):
    """
    Provides REST-style access to LCI information (exclusive of the flow-quantity relation)

    """
    @property
    def _resource_dir(self):
        return os.path.join(self._rootdir, 'resources')

    @property
    def _download_dir(self):
        return os.path.join(self._rootdir, 'downloads')

    @staticmethod
    def _source_hash_file(source):
        """
        Creates a stable filename from a source argument.  The source is the key found in the _archive dict, and
        corresponds to a single physical data source.  The filename is a sha1 hex-digest, .json.gz
        :param source:
        :return:
        """
        h = hashlib.sha1()
        h.update(source.encode('utf-8'))
        return '%s.json.gz' % h.hexdigest()

    @property
    def _index_dir(self):
        return os.path.join(self._rootdir, 'index')

    def _index_file(self, source):
        return os.path.join(self._index_dir, self._source_hash_file(source))

    @property
    def _cache_dir(self):
        return os.path.join(self._rootdir, 'cache')

    def cache_file(self, source):
        return os.path.join(self._cache_dir, self._source_hash_file(source))

    @property
    def _archive_dir(self):
        return os.path.join(self._rootdir, 'archives')

    @property
    def _entity_cache(self):
        return os.path.join(self._rootdir, 'entity_cache.json')

    @property
    def _reference_qtys(self):
        return os.path.join(self._rootdir, 'reference-quantities.json')

    @property
    def _compartments(self):
        return os.path.join(self._rootdir, 'local-compartments.json')

    @property
    def root(self):
        return self._rootdir

    def _make_rootdir(self):
        for x in (self._cache_dir, self._index_dir, self._resource_dir, self._archive_dir):
            os.makedirs(x, exist_ok=True)

    def __init__(self, rootdir, qdb=None):
        """
        Instantiates a catalog based on the resources provided in resource_dir
        :param rootdir: directory storing LcResource files.
        :param qdb: quantity database (default is the old FlowDB)
        """
        self._rootdir = rootdir
        self._make_rootdir()
        self._resolver = LcCatalogResolver(self._resource_dir)
        if not os.path.exists(self._compartments):
            copy2(REFERENCE_INT, self._compartments)
        if qdb is None:
            qdb = Qdb(source=self._reference_qtys, compartments=self._compartments)
        self._qdb = qdb
        """
        _archives := source -> archive
        _names :=  ref:interface -> source
        _nicknames := nickname -> source
        """
        self.ed = FragmentEditor(qdb=self._qdb, interactive=False)

        self._nicknames = dict()  # keep a collection of shorthands for sources

        self.add_existing_archive(qdb, interfaces=('index', 'quantity'), store=False)

        self._lcia_methods = set()

    @property
    def qdb(self):
        return self._qdb

    def quantities(self, **kwargs):
        return self._qdb.quantities(**kwargs)

    def _find_single_source(self, origin, interface, source=None):
        ress = [r for r in self._resolver.resolve(origin, interfaces=interface)]
        sources = set(r.source for r in ress)
        if len(sources) > 1:
            if source in sources:
                return source
            raise ValueError('Ambiguous resource specification %s:%s' % (origin, interface))
        if len(sources) == 0:
            raise KeyError('No source found.')
        found_source = sources.pop()
        if source is not None:
            if found_source == source:
                return source
            raise ValueError('Sources do not match:\n%s (provided)\n%s (found)' % (source, found_source))
        return found_source

    def new_resource(self, *args, store=True, **kwargs):
        """
        Create a new data resource by specifying its properties directly to the constructor
        :param args: reference, source, ds_type
        :param store: [True] permanently store this resource
        :param kwargs: interfaces=None, privacy=0, priority=0, static=False; **kwargs passed to archive constructor
        :return:
        """
        res = self._resolver.new_resource(*args, store=store, **kwargs)  # explicit store= for doc purposes

        if not store:
            if len([i for i in self._resolver.resolve(res.reference, 'index')]) == 0:  # if no index is resolvable
                self._register_index(res.source, 10)  # register indices if they exist and are not stored

            # self._ensure_resource(res)

    def add_resource(self, resource, store=True):
        """
        Add an existing LcResource to the catalog.
        :param resource:
        :param store: [True] permanently store this resource
        :return:
        """
        self._resolver.add_resource(resource, store=store)
        # self._ensure_resource(resource)

    def add_existing_archive(self, archive, interfaces=None, store=True, **kwargs):
        """
        Makes a resource record out of an existing archive.  by default, saves it in the catalog's resource dir
        :param archive:
        :param interfaces:
        :param store: [True] if False, don't save the record - use it for this session only
        :param kwargs:
        :return:
        """
        res = LcResource.from_archive(archive, interfaces, **kwargs)
        self._resolver.add_resource(res, store=store)

    def get_resource(self, name):
        """
        Retrieve a physical archive by nickname or ref:interface
        :param name: takes the form of ref:interface.  If the exact name is not specified, the catalog will find a
        source whose ref and interface start with name.
        :return: an LcArchive subclass
        """
        if name in self._nicknames:
            _gen_rs = self._resolver.resources_with_source(self._nicknames[name])
        else:
            parts = name.split(':')
            ref = parts[0]
            iface = None
            if len(parts) > 1:
                iface = parts[1]
            _gen_rs = self._resolver.resolve(ref, interfaces=iface)
        rs = [r for r in _gen_rs]
        if len(rs) == 1:
            return rs[0]
        elif len(rs) > 1:
            for k in rs:
                for i in k.interfaces:
                    print('%s:%s' % (k.reference, i))
            raise ValueError('Ambiguous reference %s refers to multiple sources' % name)
        elif len(rs) == 0:
            raise KeyError('%s not found.' % name)

    def privacy(self, ref, interfaces=None):
        res = next(r for r in self._resolver.resolve(ref, interfaces=interfaces))
        return res.privacy

    def flows_table(self, *args, **kwargs):
        """
        Creates a new flowables grid using the local Qdb and gives it to the user.
        :param args:
        :param kwargs:
        :return:
        """
        return FlowablesGrid(self._qdb, *args, **kwargs)

    @property
    def names(self):
        """
        List known references.
        :return:
        """
        for k, ifaces in self._resolver.references:
            for iface in ifaces:
                yield ':'.join([k, iface])
        for k in self._nicknames.keys():
            yield k

    def add_nickname(self, source, nickname):
        """
        quickly refer to a specific data source already present in the archive
        :param source:
        :param nickname:
        :return:
        """
        if self._resolver.known_source(source):
            self._nicknames[nickname] = source
        else:
            raise KeyError('Source %s not found' % source)

    @property
    def sources(self):
        for k in self._resolver.sources:
            yield k

    @property
    def references(self):
        for ref, ints in self._resolver.references:
            yield ref

    @property
    def interfaces(self):
        for ref, ints in self._resolver.references:
            for i in ints:
                yield ':'.join([ref, i])

    def show_interfaces(self):
        for ref, ints in self._resolver.references:
            print('%s [%s]' % (ref, ', '.join(ints)))

    def _index_source(self, source, force=False):
        inx_file = self._index_file(source)
        if os.path.exists(inx_file):
            if not force:
                print('Not overwriting existing index. force=True to override.')
                return
            print('Re-indexing %s' % source)
        res = next(r for r in self._resolver.resources_with_source(source))
        res.check(self)
        res.make_index(inx_file)

    def _register_index(self, source, priority):
        inx_file = self._index_file(source)
        if os.path.exists(inx_file):
            print('Registering index for %s' % source)
            for r in self._resolver.resources_with_source(source):
                store = self._resolver.is_permanent(r)
                self.new_resource(r.reference, inx_file, 'json', interfaces='index', priority=priority,
                                  store=store,
                                  static=True)

    def index_resource(self, origin, interface=None, source=None, priority=10, force=False):
        """
        Creates an index for the identified resource.  'origin' and 'interface' must resolve to one or more LcResources
        that all have the same source specification.  That source archive gets indexed, and index resources are created
        for all the LcResources that were returned.

        Performs load_all() on the source archive, writes the archive to a compressed json file in the local index
        directory, and creates a new LcResource pointing to the JSON file.   Aborts if the index file already exists
        (override with force=True).
        :param origin:
        :param interface: [None]
        :param source: find_single_source input
        :param priority: [10] priority setting for the new index
        :param force: [False] if True, create an index even if an interface already exists (will overwrite existing)
        :return:
        """
        source = self._find_single_source(origin, interface, source=source)
        self._index_source(source, force=force)
        self._register_index(source, priority)

    def create_source_cache(self, source, static=False):
        """
        Creates a cache of the named source's current contents, to speed up access to commonly used entities.
        source must be either a key present in self.sources, or a name or nickname found in self.names
        :param source:
        :param static: [False] create archives of a static archive (use to force archival of a complete database)
        :return:
        """
        res = next(r for r in self._resolver.resources_with_source(source))
        if res.static:
            if not static:
                print('Not archiving static resource %s' % res)
                return
            print('Archiving static resource %s' % res)
        res.make_cache(self.cache_file(source))

    def create_static_archive(self, archive_file, origin, interface=None, source=None, background=True, priority=90):
        """
        Creates a local replica of a static archive, usually for purposes of improving load time in computing
        background results.  Uses create_source_cache to generate a cache and then renames it to the target
        archive file (which should end in .json.gz) in the catalog's archive directory. Creates resources mapping
        the newly created archive file to the origin with the specified priority.

        The source file can be specified in one of two ways:
         * origin + interface (with interface defaulting to None), as long as it unambiguously identifies one source
         * origin + explicit source, which must correspond

        :param archive_file:
        :param origin:
        :param interface:
        :param source:
        :param background: [True] whether to include the background interface on the created resource
        :param priority: priority of the created resource
        :return:
        """
        source = self._find_single_source(origin, interface, source=source)
        self._index_source(source, force=True)
        if not os.path.isabs(archive_file):
            archive_file = os.path.join(self._archive_dir, archive_file)
        self.create_source_cache(source, static=True)
        os.rename(self.cache_file(source), archive_file)
        for res in self._resolver.resources_with_source(source):
            ifaces = set(i for i in res.interfaces)
            if background:
                ifaces.add('background')
            store = self._resolver.is_permanent(res)
            self.new_resource(res.reference, archive_file, 'JSON', interfaces=ifaces, priority=priority,
                              store=store,
                              static=True)

    def gen_interfaces(self, origin, itype):
        """
        :param origin:
        :param itype: single interface or iterable of interfaces
        :return:
        """
        if itype is None:
            itype = 'basic'  # fetch, get properties, uuid, reference
        for res in sorted(self._resolver.resolve(origin, interfaces=itype),
                          key=lambda x: (not x.is_loaded, x.reference != origin, x.priority)):
            res.check(self)
            for iface in res.interfaces:
                if iface == itype:
                    yield res.make_interface(self, itype)
        '''
        # no need for this because qdb is (a) listed in the resolver and (b) upstream of everything
        if 'quantity' in itype:
            yield self._qdb  # fallback to our own quantity db for Quantity Interface requests
            '''

    """
    public functions -- should these operate directly on a catalog ref instead? I think so but let's see about usage
    """
    def query(self, origin, **kwargs):
        next(self._resolver.resolve(origin))  # raises UnknownOrigin
        return CatalogQuery(origin, catalog=self, **kwargs)

    def lookup(self, origin, external_ref):
        """
        Attempts to secure an entity
        :param origin:
        :param external_ref:
        :return: The origin of the lowest-priority resource to match the query
        """
        for i in self.gen_interfaces(origin, None):
            if i.lookup(external_ref):
                return i.origin
        raise EntityNotFound('%s/%s' % (origin, external_ref))

    def fetch(self, origin, external_ref):
        org = self.lookup(origin, external_ref)
        return self.query(org).get(external_ref)

    """
    Qdb interaction
    """
    def is_elementary(self, flow):
        return self._qdb.c_mgr.is_elementary(flow)

    def load_lcia_factors(self, ref):
        if ref.link not in self._lcia_methods:
            for fb in ref.flowables():
                self._qdb.add_new_flowable(*filter(None, fb))
            for cf in ref.factors():
                self._qdb.add_cf(cf)
            self._lcia_methods.add(ref.link)

    def annotate(self, flow, quantity=None, factor=None, value=None, locale=None):
        """
        Adds a flow annotation to the Qdb.
        Two steps:
         - adds a characterization to the flow using the given data. Provide either a
         factor=Characterization, or qty + value + location.
         If locale is provided with factor, it only applies the factor applying to the given locale. (otherwise, all
         locations in the CF are applied to the flow)
         - adds the flow to the local qdb and saves to disk.
        """
        if factor is None:
            if locale is None:
                locale = 'GLO'
            if value is None:
                value = self._qdb.convert(flow, query=quantity, locale=locale)
            flow.add_characterization(quantity, value=value, origin=self._qdb.ref, location=locale)
        else:
            ref_conversion = self._qdb.convert_reference(flow, factor.flow.reference_entity, locale=locale)
            if locale is None:
                for l in factor.locations():
                    flow.add_characterization(factor.quantity, location=l, value=factor[l] * ref_conversion,
                                              origin=factor.origin(l))
            else:
                flow.add_characterization(factor.quantity, location=locale, value=factor[locale] * ref_conversion,
                                          origin=factor.origin(locale))

        self._qdb.add_entity_and_children(flow)
        for cf in flow.characterizations():
            self._qdb.add_cf(cf)
        self._qdb.save()

    def quantify(self, flowable, quantity, compartment=None):
        return [c for c in self._qdb.quantify(flowable, quantity, compartment=compartment)]

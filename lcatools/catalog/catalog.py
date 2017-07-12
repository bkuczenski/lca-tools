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

from .interfaces import QueryInterface, INTERFACE_TYPES
from .entity import IndexInterface
from .inventory import InventoryInterface
from .background import BackgroundInterface
from .quantity import QuantityInterface
from .lc_resolver import LcCatalogResolver
from .lc_resource import LcResource
from lcatools.tools import create_archive, update_archive  # archive_from_json, archive_factory
from lcatools.providers.qdb import Qdb
from lcatools.flowdb.compartments import REFERENCE_INT  # reference intermediate flows
from lcatools.tables import FlowablesGrid


_protocol = re.compile('^(\w+)://')


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

    def _cache_file(self, source):
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
        self._entities = dict()  # maps '/'.join(origin, external_ref) to entity
        self._qdb = qdb
        """
        _archives := source -> archive
        _names :=  ref:interface -> source
        _nicknames := nickname -> source
        """
        self._archives = dict()  # maps source to archive
        self._names = dict()  # maps reference to source
        self._nicknames = dict()  # keep a collection of shorthands for sources

        self._lcia_methods = set()

    def quantities(self, **kwargs):
        return self._qdb.quantities(**kwargs)

    def new_resource(self, *args, **kwargs):
        """
        Create a new data resource by specifying its properties directly to the constructor
        :param args: reference, source, ds_type
        :param store: [True] permanently store this resource
        :param kwargs: interfaces=None, privacy=0, priority=0, static=False; **kwargs passed to archive constructor
        :return:
        """
        res = self._resolver.new_resource(*args, **kwargs)
        self._ensure_resource(res)

    def add_resource(self, resource, store=True):
        """
        Add an existing LcResource to the catalog.
        :param resource:
        :param store: [True] permanently store this resource
        :return:
        """
        self._resolver.add_resource(resource, store=store)
        self._ensure_resource(resource)

    def add_archive(self, archive, interfaces=None, store=True, **kwargs):
        res = LcResource.from_archive(archive, interfaces, **kwargs)
        self._resolver.add_resource(res, store=store)
        self._archives[res.source] = archive
        for t in res.interfaces:
            for k in archive.catalog_names.keys():
                self._names[':'.join([k, t])] = res.source

    def get_archive(self, name):
        """
        Retrieve a physical archive by namespace and interface
        :param name: takes the form of ref:interface
        :return: an LcArchive subclass
        """
        if name in self._nicknames:
            return self._archives[self._nicknames[name]]
        return self._archives[self._names[name]]

    def privacy(self, ref):
        res = next(r for r in self._resolver.resolve(ref))
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
        List loaded references.
        :return:
        """
        for k in self._names.keys():
            yield k
        for k in self._nicknames.keys():
            yield k

    @property
    def sources(self):
        for k in self._archives.keys():
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

    def load_all(self, origin, interface=None, source=None):
        source = self._find_single_source(origin, interface, source=source)
        self._archives[source].load_all()

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
        :param priority: [10] priority setting for the new index
        :param force: [False] if True, create an index even if an interface already exists (will overwrite existing)
        :return:
        """
        source = self._find_single_source(origin, interface, source=source)
        self._index_source(source, force=force, priority=priority)

    def _index_source(self, source, force=False, priority=10):
        inx_file = self._index_file(source)
        if os.path.exists(inx_file):
            if not force:
                print('Not overwriting existing index. force=True to override.')
                return
            print('Re-indexing %s' % source)
        self._ensure_resource(next(r for r in self._resolver.resources_with_source(source)))
        archive = self._archives[source]
        archive.load_all()
        archive.write_to_file(inx_file, gzip=True, exchanges=False, characterizations=False, values=False)
        for r in self._resolver.resources_with_source(source):
            store = self._resolver.is_permanent(r)
            self.new_resource(r.reference, inx_file, 'json', interfaces='index', priority=priority,
                              store=store,
                              static=True)

    def create_source_cache(self, source, static=False):
        """
        Creates a cache of the named source's current contents, to speed up access to commonly used entities.
        source must be either a key present in self.sources, or a name or nickname found in self.names
        :param source:
        :param static: [False] create archives of a static archive (use to force archival of a complete database)
        :return:
        """
        if source not in self._archives:
            if source in self._nicknames:
                source = self._nicknames[source]
            else:
                source = self._names[source]
        archive = self._archives[source]
        if archive.static:
            if not static:
                print('Not archiving static resource %s' % archive)
                return
            print('Archiving static resource %s' % archive)
        cache_file = self._cache_file(source)
        archive.write_to_file(cache_file, gzip=True, exchanges=True, characterizations=True, values=True)
        print('Created archive of %s containing:' % archive)
        archive.check_counter()

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
        os.rename(self._cache_file(source), archive_file)
        for res in self._resolver.resources_with_source(source):
            ifaces = set(i for i in res.interfaces)
            if background:
                ifaces.add('background')
            store = self._resolver.is_permanent(res)
            self.new_resource(res.reference, archive_file, 'JSON', interfaces=ifaces, priority=priority,
                              store=store,
                              static=True)

    def add_nickname(self, source, nickname):
        """
        quickly refer to a specific data source already present in the archive
        :param source:
        :param nickname:
        :return:
        """
        if source in self._archives:
            self._nicknames[nickname] = source
        else:
            raise KeyError('Source %s not found' % source)

    def _ensure_resource(self, res):
        """
        create the archive requested. install qdb as upstream.
        :param res:
        :return:
        """
        if res.source not in self._archives:
            a = create_archive(res.source, res.ds_type, ref=res.reference, **res.init_args)
            if os.path.exists(self._cache_file(res.source)):
                update_archive(a, self._cache_file(res.source))
            if res.static and res.ds_type.lower() != 'json':
                a.load_all()  # static json archives are by convention saved in complete form
            self._archives[res.source] = a
            for t in res.interfaces:
                for k in a.catalog_names.keys():
                    self._names[':'.join([k, t])] = res.source
        return True

    def _check_entity(self, source, external_ref):
        ent = self._archives[source].retrieve_or_fetch_entity(external_ref)
        if ent is not None:
            self._entities[ent.link] = ent
        return ent

    def _dereference(self, origin, external_ref, interface=None):
        """
        Converts an origin + external_ref into an entity.  Under the current design, _dereference is used only for
        non-fragments.  foreground catalogs implement a _retrieve method that is used for fragments.
        :param origin:
        :param external_ref:
        :param interface:
        :return: resource.reference, entity
        """
        uname = '/'.join([origin, str(external_ref)])
        found_ref = None
        if uname in self._entities:
            return origin, self._entities[uname]
        if origin in self._nicknames:
            ent = self._check_entity(self._nicknames[origin], external_ref)
        else:
            ent = None
            for res in self._resolver.resolve(origin, interfaces=interface):
                try:
                    self._ensure_resource(res)
                except Exception as e:
                    print('Archive Instantiation for %s failed with %s' % (origin, e))
                    # TODO: try to get more specific with exceptions.  p.s.: no idea what happens to this logging info
                    # logging.info('Static archive for %s failed with %s' % (origin, e))
                    raise
                ent = self._check_entity(res.source, external_ref)
                if ent is not None:
                    found_ref = res.reference
                    break
        return found_ref, ent

    def _get_interfaces(self, origin, itype):
        """
        :param origin:
        :param itype: single interface or iterable of interfaces
        :return:
        """
        if isinstance(itype, str):
            itype = [itype]
        itype = set(itype)
        for res in sorted(self._resolver.resolve(origin, interfaces=itype),
                          key=lambda x: (x.reference != origin, x.priority)):
            if not self._ensure_resource(res):
                continue
            matches = itype.intersection(set(res.interfaces))
            if 'quantity' in matches:
                yield QuantityInterface(self._archives[res.source], self._qdb, catalog=self, privacy=res.privacy)
            if 'index' in matches:
                yield IndexInterface(self._archives[res.source], catalog=self, privacy=res.privacy)
            if 'inventory' in matches:
                yield InventoryInterface(self._archives[res.source], catalog=self, privacy=res.privacy)
            if 'background' in matches:
                yield BackgroundInterface(self._archives[res.source], self._qdb, catalog=self, privacy=res.privacy)
        if 'quantity' in itype:
            yield self._qdb  # fallback to our own quantity db for Quantity Interface requests

    def get_interface(self, origin, itype):
        for arch in self._get_interfaces(origin, itype):
            yield arch

    """
    public functions -- should these operate directly on a catalog ref instead? I think so but let's see about usage
    """
    def query(self, origin):
        next(self._resolver.resolve(origin))  # raises UnknownOrigin
        return QueryInterface(origin, catalog=self)

    def lookup(self, ref):
        """
        Attempts to secure an entity
        :param ref: a CatalogRef
        :return: The lowest-priority origin to contain the entity
        """
        found_ref, e = self._dereference(ref.origin, ref.external_ref, INTERFACE_TYPES)
        return found_ref

    def fetch(self, ref):
        if ref.is_entity:
            return ref
        _, e = self._dereference(ref.origin, ref.external_ref, ('inventory', 'background', 'quantity'))
        return e

    def entity_type(self, ref):
        return self.fetch(ref).entity_type

    """
    Qdb interaction
    """
    def is_elementary(self, flow):
        return self._qdb.c_mgr.is_elementary(flow)

    def is_loaded(self, lcia):
        return lcia in self._lcia_methods

    def load_lcia_factors(self, ref):
        lcia = self._qdb.get_canonical_quantity(self.fetch(ref))
        for fb in ref.flowables():
            self._qdb.add_new_flowable(*filter(None, fb))
        for cf in ref.factors():
            self._qdb.add_cf(cf)
        self._lcia_methods.add(lcia)

    def lcia(self, p_ref, q_ref, ref_flow=None, refresh=False):
        """
        Perform LCIA of a process (p_ref) with respect to a given LCIA quantity (q_ref).  Returns an LciaResult.
        :param p_ref: either a process, or a catalog_ref for a process
        :param q_ref: either an LCIA method (quantity with 'Indicator'), or a catalog_ref for an LCIA method. Only
         catalog refs have the capability to auto-load characterization factors.
        :param ref_flow: [None] if applicable, reference flow to be considered for the process
        :param refresh: [False] whether to re-calculate CFs
        :return:
        """
        q_e = self._qdb.get_canonical_quantity(self.fetch(q_ref))
        if not self.is_loaded(q_e):
            self.load_lcia_factors(q_ref)
        return self._qdb.do_lcia(q_e, p_ref.inventory(ref_flow=ref_flow), locale=p_ref['SpatialScope'], refresh=refresh)

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

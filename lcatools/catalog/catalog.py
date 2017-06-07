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

from .interfaces import QueryInterface, INTERFACE_TYPES
from .entity import IndexInterface
from .inventory import InventoryInterface
from .background import BackgroundInterface
from .quantity import QuantityInterface
from .lc_resolver import LcCatalogResolver
from lcatools.tools import create_archive  # archive_from_json, archive_factory
from lcatools.providers.qdb import Qdb
from lcatools.flowdb.compartments import REFERENCE_INT


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

    @property
    def _entity_cache(self):
        return os.path.join(self._rootdir, 'entity_cache.json')

    @property
    def _compartments(self):
        return os.path.join(self._rootdir, 'local-compartments.json')

    @property
    def root(self):
        return self._rootdir

    def __init__(self, rootdir, qdb=None):
        """
        Instantiates a catalog based on the resources provided in resource_dir
        :param rootdir: directory storing LcResource files.
        :param qdb: quantity database (default is the old FlowDB)
        """
        self._rootdir = rootdir
        self._resolver = LcCatalogResolver(self._resource_dir)
        if not os.path.exists(self._compartments):
            copy2(REFERENCE_INT, self._compartments)
        if qdb is None:
            qdb = Qdb(compartments=self._compartments)
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
        :param args: source, ds_type
        :param kwargs: interfaces=None, privacy=0, priority=0, static=False; **kwargs passed to archive constructor
        :return:
        """
        self._resolver.new_resource(*args, **kwargs)

    def add_resource(self, resource):
        """
        Add an existing LcResource to the catalog.
        :param resource:
        :return:
        """
        self._resolver.add_resource(resource)

    def get_archive(self, name):
        """
        Retrieve a physical archive by namespace and interface
        :param name: takes the form of ref:interface
        :return: an LcArchive subclass
        """
        return self._archives[self._names[name]]

    def privacy(self, ref):
        res = next(r for r in self._resolver.resolve(ref))
        return res.privacy

    @property
    def names(self):
        """
        List loaded references.
        :return:
        """
        return list(self._names.keys())

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
        if res.source not in self._archives:
            a = create_archive(res.source, res.ds_type, ref=res.reference, **res.init_args)
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
                    continue
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
        for res in sorted(self._resolver.resolve(origin, interfaces=itype), key=lambda x: x.priority):
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
        :return: a list of origins that contain the reference.
        """

        origins = set()
        for iface in INTERFACE_TYPES:
            found_ref, e = self._dereference(ref.origin, ref.external_ref, iface)
            if e is not None:
                origins.add(found_ref)
        return sorted(list(origins))

    def fetch(self, ref):
        if ref.is_entity:
            return ref
        _, e = self._dereference(ref.origin, ref.external_ref, INTERFACE_TYPES)
        return e

    def entity_type(self, ref):
        return self.fetch(ref).entity_type

    """
    Qdb interaction
    """
    def load_lcia_factors(self, ref):
        lcia = self._qdb.get_canonical_quantity(self.fetch(ref))
        for cf in ref.factors():
            self._qdb.add_cf(cf)
        self._lcia_methods.add(lcia)

    def lcia(self, p_ref, q_ref):
        """
        Perform LCIA of a process (p_ref) with respect to a given LCIA quantity (q_ref).  Returns an LciaResult.
        :param p_ref: either a process, or a catalog_ref for a process
        :param q_ref: either an LCIA method (quantity with 'Indicator'), or a catalog_ref for an LCIA method. Only
         catalog refs have the capability to auto-load characterization factors.
        :return:
        """
        q_e = self._qdb.get_canonical_quantity(self.fetch(q_ref))
        if not self._qdb.is_loaded(q_e):
            self.load_lcia_factors(q_ref)
        return self._qdb.do_lcia(q_e, p_ref.inventory(), locale=p_ref['SpatialScope'])

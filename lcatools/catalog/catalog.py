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

from .interfaces import NoInterface, INTERFACE_TYPES
from .entity import EntityInterface
from .foreground import ForegroundInterface
from .background import BackgroundInterface
from .quantity import QuantityInterface
from .lc_resolver import LcCatalogResolver
from lcatools.tools import create_archive  # archive_from_json, archive_factory
from lcatools.flowdb.flowdb import FlowDB

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
    def root(self):
        return self._rootdir

    def __init__(self, rootdir, qdb=None):
        """
        Instantiates a catalog based on the resources provided in resource_dir
        :param rootdir: directory storing LcResource files.
        :param qdb: quantity database (default is the old FlowDB)
        """
        if qdb is None:
            qdb = FlowDB()
        self._rootdir = rootdir
        self._resolver = LcCatalogResolver(self._resource_dir)
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

    def new_resource(self, *args, **kwargs):
        """
        Create a new data resource by specifying its properties directly to the constructor
        :param args:
        :param kwargs:
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
                for k, v in a.get_names().items():
                    self._names[':'.join([res.reference, t])] = v
        return True

    def _check_entity(self, source, external_ref):
        ent = self._archives[source].retrieve_or_fetch_entity(external_ref)
        if ent is not None:
            self._entities[ent.get_link()] = ent
        return ent

    def _dereference(self, origin, external_ref, interface=None):
        uname = '/'.join([origin, external_ref])
        if uname in self._entities:
            return self._entities[uname]
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
                    break
        return ent

    def _get_interfaces(self, origin, itype):
        """
        :param origin:
        :param itype: single interface or iterable of interfaces
        :return:
        """
        if isinstance(itype, str):
            itype = [itype]
        itype = set(itype)
        for res in self._resolver.resolve(origin, interfaces=itype):
            if not self._ensure_resource(res):
                continue
            matches = itype.intersection(set(res.interfaces))
            if 'quantity' in matches:
                yield QuantityInterface(self._archives[res.source], self._qdb.compartments, privacy=res.privacy)
            if 'entity' in matches:
                yield EntityInterface(self._archives[res.source], privacy=res.privacy)
            if 'foreground' in matches:
                yield ForegroundInterface(self._archives[res.source], privacy=res.privacy)
            if 'background' in matches:
                yield BackgroundInterface(self._archives[res.source], self._qdb, privacy=res.privacy)

    def get_interface(self, origin, itype):
        for arch in self._get_interfaces(origin, itype):
            yield arch

    """
    public functions -- should these operate directly on a catalog ref instead? I think so but let's see about usage
    """
    def lookup(self, origin, external_ref):
        """
        Attempts to secure an entity
        :param origin:
        :param external_ref:
        :return: a list of interfaces that can be secured for the reference.
        """

        results = []
        for iface in INTERFACE_TYPES:
            e = self._dereference(origin, external_ref, iface)
            if e is not None:
                results.append(iface)
        return results

    def fetch(self, origin, external_ref):
        return self._dereference(origin, external_ref, 'quantity') or \
               self._dereference(origin, external_ref, 'foreground') or \
               self._dereference(origin, external_ref, 'entity')

    def entity_type(self, origin, external_ref):
        return self.fetch(origin, external_ref).entity_type

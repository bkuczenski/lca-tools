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

import logging

from .interfaces import BasicInterface
from .entity import EntityInterface
from .foreground import ForegroundInterface
from .background import BackgroundInterface
from .static_archive import StaticArchive
from .lc_resolver import LcCatalogResolver
from lcatools.tools import archive_from_json, archive_factory


class LcCatalog(object):
    """
    Provides REST-style access to LCI information (exclusive of the flow-quantity relation)

    """
    def __init__(self, resource_dir, qdb):
        """
        Instantiates a catalog based on the resources provided in resource_dir
        :param resource_dir: directory storing LcResource files.
        :param qdb: quantity database
        """
        self._resolver = LcCatalogResolver(resource_dir)
        self._entities = dict()
        self._qdb = qdb
        self._archives = dict()
        self._nicknames = dict()  # keep a collection of shorthands

    def new_resource(self, *args, **kwargs):
        self._resolver.new_resource(*args, **kwargs)

    def add_resource(self, resource):
        self._resolver.add_resource(resource)

    def _make_static_archive(self, resource):
        if resource.ds_type.lower() == 'json':
            a = archive_from_json(resource.source, ref=resource.reference, **resource.args)
        else:
            a = archive_factory(resource.source, resource.ds_type, ref=resource.reference, **resource.args)
        self._archives[resource.source] = StaticArchive(a, ref=resource.reference)
        self._nicknames[resource.reference.split('.')[-1]] = resource.source

    def _ensure_resource(self, res):
        if res.source not in self._archives:
            self._make_static_archive(res)

    def _check_entity(self, source, external_ref):
        ent = self._archives[source][external_ref]
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
                    # TODO: try to get more specific with exceptions.  p.s.: no idea what happens to this logging info
                    logging.info('Static archive for %s failed with %s' % (origin, e))
                    continue
                ent = self._check_entity(res.source, external_ref)
                if ent is not None:
                    break
        return ent

    def _get_interfaces(self, origin, itype):
        for res in self._resolver.resolve(origin, interfaces=itype):
            if not self._ensure_resource(res):
                continue
            if itype == 'entity':
                yield EntityInterface(self._archives[res.source], privacy=res.privacy)
            elif itype == 'study':
                yield ForegroundInterface(self._archives[res.source], privacy=res.privacy)
            elif itype == 'background':
                yield BackgroundInterface(self._archives[res.source], self._qdb, privacy=res.privacy)

    def _next_i(self, origin, itype):
        try:
            arch = next(self._get_interfaces(origin, itype))
        except StopIteration:
            arch = BasicInterface(None)
        return arch

    """
    public functions
    """
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

    def lookup(self, origin, external_ref):
        """
        Returns a trio of booleans indicating whether the catalog knows how to perform entity, study, background
        lookups respectively.
        This function needs work- what exactly are we trying to do? shouldn't we return true if a source is found?
        :param origin:
        :param external_ref:
        :return:
        """
        results = [False, False, False]
        for i, iface in enumerate(('entity', 'study', 'background')):
            e = self._dereference(origin, external_ref, iface)
            if e is not None:
                results[i] = True
        return results

    def fetch(self, origin, external_ref):
        return self._dereference(origin, external_ref, 'study') or \
               self._dereference(origin, external_ref, 'entity')

    def entity_type(self, origin, external_ref):
        return self.fetch(origin, external_ref).entity_type

    """
    API functions - archive-wide
    """
    def processes(self, origin, **kwargs):
        arch = self._next_i(origin, 'entity')
        return arch.processes(**kwargs)

    def flows(self, origin, **kwargs):
        arch = self._next_i(origin, 'entity')
        return arch.processes(**kwargs)

    def quantities(self, origin, **kwargs):
        arch = self._next_i(origin, 'entity')
        return arch.processes(**kwargs)

    def foreground_flows(self, origin, search=None):
        arch = self._next_i(origin, 'background')
        return arch.foreground_flows(search=search)

    def background_flows(self, origin, search=None):
        arch = self._next_i(origin, 'background')
        return arch.background_flows(search=search)

    def exterior_flows(self, origin, direction=None, search=None):
        arch = self._next_i(origin, 'background')
        return arch.exterior_flows(direction=direction, search=search)

    def cutoffs(self, origin, direction=None, search=None):
        arch = self._next_i(origin, 'background')
        return arch.cutoffs(direction=direction, search=search)

    def emissions(self, origin, direction=None, search=None):
        arch = self._next_i(origin, 'background')
        return arch.cutoffs(direction=direction, search=search)

    """
    API functions- entity-specific
    entity interface
    """
    def get(self, origin, external_ref):
        arch = self._next_i(origin, 'entity')
        return arch.get(external_ref)

    def terminate(self, origin, external_ref, direction=None):
        arch = self._next_i(origin, 'entity')
        return arch.terminate(external_ref, direction=direction)

    def originate(self, origin, external_ref, direction=None):
        arch = self._next_i(origin, 'entity')
        return arch.originate(external_ref, direction=direction)

    def mix(self, origin, external_ref, direction):
        arch = self._next_i(origin, 'entity')
        return arch.mix(external_ref, direction)

    """
    study interface
    """
    def exchanges(self, origin, external_ref):
        arch = self._next_i(origin, 'study')
        return arch.exchanges(external_ref)

    def exchange_values(self, origin, external_ref, exch_flow, direction, termination=None):
        arch = self._next_i(origin, 'study')
        return arch.exchange_values(external_ref, exch_flow, direction, termination=termination)

    def exchange_relation(self, origin, process_ref, ref_flow, exch_flow, direction, termination=None):
        arch = self._next_i(origin, 'study')
        return arch.exchange_relation(process_ref, ref_flow, exch_flow, direction, termination=termination)

    """
    background interface
    """
    def ad(self, origin, external_ref, ref_flow):
        arch = self._next_i(origin, 'background')
        return arch.ad(external_ref, ref_flow)

    def bf(self, origin, external_ref, ref_flow):
        arch = self._next_i(origin, 'background')
        return arch.bf(external_ref, ref_flow)

    def lci(self, origin, external_ref, ref_flow):
        arch = self._next_i(origin, 'background')
        return arch.lci(external_ref, ref_flow)

    def lcia(self, origin, external_ref, ref_flow, lcia_qty):
        arch = self._next_i(origin, 'background')
        return arch.lcia(external_ref, lcia_qty, ref_flow)

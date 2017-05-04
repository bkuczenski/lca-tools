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
import re

from .interfaces import NoInterface
from .entity import EntityInterface
from .foreground import ForegroundInterface
from .background import BackgroundInterface
from .lc_resolver import LcCatalogResolver
from lcatools.tools import create_archive  # archive_from_json, archive_factory

_protocol = re.compile('^(\w+)://')


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
        self._entities = dict()  # maps '/'.join(origin, external_ref) to entity
        self._qdb = qdb
        self._archives = dict()  # maps source to archive
        self._names = dict()  # maps reference to source
        self._nicknames = dict()  # keep a collection of shorthands

    def new_resource(self, *args, **kwargs):
        self._resolver.new_resource(*args, **kwargs)

    def add_resource(self, resource):
        self._resolver.add_resource(resource)

    def _register_archive(self, archive):
        self._archives[archive.source] = archive
        self._names.update(archive.get_names())

    @property
    def names(self):
        return list(self._names.keys())

    def get_archive(self, name):
        return self._archives[self._names[name]]

    def _ensure_resource(self, res):
        if res.source not in self._archives:
            a = create_archive(res.source, res.ds_type, ref=res.reference, **res.args)
            self._register_archive(a)
            self._nicknames[res.reference.split('.')[-1]] = res.source

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

    def get_interface(self, origin, itype):
        try:
            arch = next(self._get_interfaces(origin, itype))
        except StopIteration:
            raise NoInterface('%s for origin %s' % (itype, origin))
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
        Attempts to secure an entity
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
        return self._dereference(origin, external_ref, 'quantity') or \
               self._dereference(origin, external_ref, 'foreground') or \
               self._dereference(origin, external_ref, 'entity')

    def entity_type(self, origin, external_ref):
        return self.fetch(origin, external_ref).entity_type

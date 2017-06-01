"""
Interim classes with useful building blocks
"""

from __future__ import print_function, unicode_literals

import uuid
from collections import defaultdict

import six

from lcatools.entities import LcFlow, LcProcess, LcQuantity, LcUnit, entity_types
from lcatools.exchanges import comp_dir
from lcatools.providers.interfaces import ArchiveInterface, to_uuid

if six.PY2:
    bytes = str
    str = unicode


class XlDict(object):
    """
    wrapper class for xlrd that exposes a simple pandas-like interface to access tabular spreadsheet data with iterrows.
    """
    @classmethod
    def from_sheetname(cls, workbook, sheetname):
        return cls(workbook.sheet_by_name(sheetname))

    def __init__(self, sheet):
        """

        :param sheet: an xlrd.sheet.Sheet
        """
        self._sheet = sheet

    def iterrows(self):
        """
        Using the first row as a list of headers, yields a dict for each subsequent row using the header names as keys.
        returning index, row for pandas compatibility
        :return:
        """
        _gen = self._sheet.get_rows()
        # grab first row
        d = dict((v.value, k) for k, v in enumerate(next(_gen)))
        index = 0
        for r in _gen:
            index += 1
            yield index, dict((k, r[v].value) for k, v in d.items())

    def unique_units(self, internal=False):
        """
                unitname = 'unit' if self.internal else 'unitName'
        units = set(_elementary[unitname].unique().tolist()).union(
            set(_intermediate[unitname].unique().tolist()))
        for u in units:
            self._create_quantity(u)

        :param internal:
        :return:
        """
        units = set()
        unitname = 'unit' if internal else 'unitName'
        for index, row in self.iterrows():
            units.add(row[unitname])
        return units


class OldJson(Exception):
    pass


class LcArchive(ArchiveInterface):
    """
    A class meant for building life cycle models. This takes the archive machinery and adds functions
    specific to processes, flows, and quantities. Also adds upstreaming capabilities, including (possibly
    in the future) upstream flow lookups by standardized key reference, as currently implemented in Ecoinvent lcia.

    Note: in lieu of having a @classmethod from_json, we have an archive factory that produces archives using the
    appropriate constructor given the data type.  This is found in lcatools.tools and calls the base entity_from_json
    method-- which itself should be offloaded to the entities wherever possible.

    """
    def __init__(self, source, ref=None, **kwargs):
        super(LcArchive, self).__init__(source, ref=ref, **kwargs)
        self._terminations = defaultdict(set)

    @classmethod
    def _create_unit(cls, unitstring):
        return LcUnit(unitstring), None

    def set_upstream(self, upstream):
        super(LcArchive, self).set_upstream(upstream)
        # create a dict of upstream flows
        for i in self._upstream.flows() + self._upstream.quantities():
            up_key = self._upstream_key(i)
            if up_key in self._upstream_hash:
                print('!!multiple upstream matches for %s!!' % up_key)
            else:
                self._upstream_hash[up_key] = i

    def add_entity_and_children(self, entity):
        try:
            self.add(entity)
        except KeyError:
            return
        if entity.entity_type == 'quantity':
            # reset unit strings- units are such a hack
            entity.reference_entity._external_ref = entity.reference_entity._unitstring
        elif entity.entity_type == 'flow':
            # need to import all the flow's quantities
            for cf in entity.characterizations():
                self.add_entity_and_children(cf.quantity)
        elif entity.entity_type == 'process':
            # need to import all the process's flows
            for x in entity.exchanges():
                self.add_entity_and_children(x.flow)
        elif entity.entity_type == 'fragment':
            self.add_entity_and_children(entity.flow)
            for c in entity.child_flows:
                self.add_entity_and_children(c)

    @staticmethod
    def _upstream_key(entity):
        if entity.entity_type == 'flow':
            return ', '.join(filter(None, [entity['Name']] + entity['Compartment']))
        elif entity.entity_type == 'quantity':
            return str(entity)
        else:
            return None

    def get_item(self, key, item):
        entity = self.retrieve_or_fetch_entity(key)
        if entity and entity.has_property(item):
            return entity[item]
        return None

    def get_reference(self, key):
        entity = self.retrieve_or_fetch_entity(key)
        if entity is None:
            return None
        if entity.entity_type == 'process':
            # need to get actual references with exchange values-- not the reference_entity
            return [x for x in entity.references()]
        return entity.reference_entity

    def get_uuid(self, key):
        return self._key_to_id(key)

    @staticmethod
    def _lcia_key(quantity):
        return ', '.join([quantity['Method'], quantity['Category'], quantity['Indicator']])

    def _check_upstream(self, key):
        """
        Method to check if a primitive entity, not yet distinguished by a UUID, is present in the upstream database.
        Uses a customized (DANGER!) type-dependent identifier that will have to do for now.

        How to use this: generate a type-dependent check-key that matches the output of _upstream_key.  These will
        be stored in a hash for flows and quantities (processes should never defer to upstream: the archive will always
        consider itself authoritative for processes it contains).  If the check-key is found in the hash, the upstream
        entity will be returned.  Otherwise returns None.
        :param key:
        :return:
        """
        if key in self._upstream_hash:
            return self._upstream_hash[key]
        return None

    def load_json(self, j):
        """
        Archives loaded from JSON files are considered static.
        :param j:
        :return:
        """
        self._static = True
        for e in j['quantities']:
            self.entity_from_json(e)
        for e in j['flows']:
            self.entity_from_json(e)
        for e in j['processes']:
            self.entity_from_json(e)
        self.check_counter()
        self._index_terminations()
        self._loaded = True

    def entity_from_json(self, e):
        """
        Create an LcEntity subclass from a json-derived dict

        this could use some serious refactoring
        :param e:
        :return:
        """
        if 'tags' in e:
            raise OldJson('This file type is no longer supported.')
        e_id = e.pop('entityId')
        ext_ref = e.pop('externalId')
        uid = self._key_to_id(e_id)
        etype = e.pop('entityType')
        origin = e.pop('origin')
        if etype == 'quantity':
            # can't move this to entity because we need _create_unit- so we wouldn't gain anything
            unit, _ = self._create_unit(e.pop('referenceUnit'))
            e['referenceUnit'] = unit
            entity = LcQuantity(uid, **e)
        elif etype == 'flow':
            if 'referenceQuantity' in e:
                e.pop('referenceQuantity')
            chars = e.pop('characterizations', [])
            entity = LcFlow(uid, **e)
            for c in chars:
                v = None
                q = self[c['quantity']]
                if q is None:
                    continue
                    # import json
                    # import sys
                    # print(ext_ref)
                    # json.dump(c, sys.stdout, indent=2)
                    # raise KeyError
                if 'value' in c:
                    v = c['value']
                if 'isReference' in c:
                    is_ref = c['isReference']
                else:
                    is_ref = False
                entity.add_characterization(q, reference=is_ref, value=v)
        elif etype == 'process':
            # note-- we are officially abandoning referenceExchange notation
            if 'referenceExchange' in e:
                e.pop('referenceExchange')
            exchs = e.pop('exchanges', [])
            entity = LcProcess(uid, **e)
            refs, nonrefs = [], []
            ref_x = dict()
            for x in exchs:
                if 'isReference' in x and x['isReference'] is True:
                    refs.append(x)
                else:
                    nonrefs.append(x)
            # first add reference exchanges
            for x in refs:
                # eventually move this to an exchange classmethod - which is why I'm repeating myself for now
                v = None
                f = self[x['flow']]
                d = x['direction']
                if 'value' in x:
                    v = x['value']
                ref_x[x['flow']] = entity.add_exchange(f, d, value=v)
                entity.add_reference(f, d)
            # then add ordinary [allocated] exchanges
            for x in nonrefs:
                t = None
                # is_ref = False
                f = self[x['flow']]
                d = x['direction']
                if 'termination' in x:
                    t = x['termination']
                if 'value' in x:
                    entity.add_exchange(f, d, value=x['value'], termination=t)

                if 'valueDict' in x:
                    for k, val in x['valueDict'].items():
                        drr, fuu = k.split(':')
                        try:
                            rx = ref_x[fuu]
                        except KeyError:
                            entity.show()
                            print('key: %s' % k)
                            print('flow: %s' % self[fuu])
                            raise
                        assert rx.direction == drr
                        entity.add_exchange(f, d, reference=rx, value=val, termination=t)
        else:
            raise TypeError('Unknown entity type %s' % e['entityType'])

        entity.origin = origin
        entity.set_external_ref(ext_ref)
        self.add(entity)

    def processes(self, **kwargs):
        return [p for p in self.search('process', **kwargs)]

    def flows(self, **kwargs):
        return [f for f in self.search('flow', **kwargs)]

    def quantities(self, **kwargs):
        return [q for q in self.search('quantity', **kwargs)]

    def lcia_methods(self, **kwargs):
        return [q for q in self.search('quantity', **kwargs) if q.is_lcia_method()]

    def _entities_by_type(self, entity_type):
        if entity_type not in entity_types:
            entity_type = {
                'p': 'process',
                'f': 'flow',
                'q': 'quantity'
            }[entity_type[0]]
        return super(LcArchive, self)._entities_by_type(entity_type)

    def _index_terminations(self):
        """
        Need some way to make this not have to happen for every query
        :return:
        """
        self._terminations = defaultdict(set)  # reset the index
        for p in self.processes():
            for rx in p.reference_entity:
                self._terminations[rx.flow.external_ref].add((rx.direction, p))

    def terminate(self, flow_ref, direction=None):
        """
        Generate processes in the archive that terminate a given exchange i.e. - have the same flow and a complementary
        direction.  If refs_only is specified, only report processes that terminate the exchange with a reference
        exchange.
        :param flow_ref: flow or flow's external key
        :param direction: [None] filter
        :return:
        """
        if isinstance(flow_ref, LcFlow):
            flow_ref = flow_ref.external_ref
        if not self.static:
            self._index_terminations()  # we don't really want to re-index *every time* but what is the alternative?
        for x in self._terminations[flow_ref]:  # defaultdict, so no KeyError
            if direction is None:
                yield x[1].trim()
            else:
                if comp_dir(direction) == x[0]:
                    yield x[1].trim()

    def originate(self, flow_ref, direction=None):
        if direction is not None:
            direction = comp_dir(direction)
        return self.terminate(flow_ref, direction)

    def mix(self, flow_ref, direction):
        if isinstance(flow_ref, LcFlow):
            flow_ref = flow_ref.external_ref
        terms = [t for t in self.terminate(flow_ref, direction=direction)]
        flow = self[flow_ref]
        p = LcProcess.new('Market for %s' % flow['Name'], Comment='Auto-generated')
        p.add_exchange(flow, comp_dir(direction), value=float(len(terms)))
        p.add_reference(flow, comp_dir(direction))
        for t in terms:
            p.add_exchange(flow, direction, value=1.0, termination=t.external_ref)
        return p

    def exchanges(self, flow, direction=None):
        """
        Generate exchanges that contain the given flow. Optionally limit to exchanges having the specified direction.
        Default is to include both inputs and outputs.
        :param flow:
        :param direction: [None]
        :return:
        """
        for p in self.processes():
            for x in p.exchanges():
                if x.flow == flow:
                    if direction is None:
                        yield x
                    else:
                        if x.direction == direction:
                            yield x

    def load_all(self, **kwargs):
        super(LcArchive, self).load_all(**kwargs)
        self._index_terminations()

    def serialize(self, exchanges=False, characterizations=False, values=False):
        """

        :param exchanges:
        :param characterizations:
        :param values:
        :return:
        """
        j = super(LcArchive, self).serialize()
        j['processes'] = sorted([p.serialize(exchanges=exchanges, values=values) for p in self.processes()],
                                key=lambda x: x['entityId'])
        j['flows'] = sorted([f.serialize(characterizations=characterizations, values=values) for f in self.flows()],
                            key=lambda x: x['entityId'])
        j['quantities'] = sorted([q.serialize() for q in self.quantities()], key=lambda x: x['entityId'])
        return j


class NsUuidArchive(LcArchive):
    """
    A class that generates UUIDs in a namespace using a supplied key
    """
    def __init__(self, source, ns_uuid=None, **kwargs):
        super(NsUuidArchive, self).__init__(source, **kwargs)

        # internal namespace UUID for generating keys

        if ns_uuid is None:
            if self._upstream is not None:
                if isinstance(self._upstream, NsUuidArchive):
                    ns_uuid = self._upstream._ns_uuid

        if ns_uuid is None:
            ns_uuid = uuid.uuid4()

        if not isinstance(ns_uuid, uuid.UUID):
            ns_uuid = uuid.UUID(ns_uuid)

        self._ns_uuid = ns_uuid
        self._serialize_dict['nsUuid'] = str(self._ns_uuid)

    def _key_to_id(self, key):
        """
        If the supplied key matches a uuid string, returns it.  Otherwise, creates a uuid3 using the internal namespace.
        :param key:
        :return:
        """
        if isinstance(key, int):
            key = str(key)
        u = to_uuid(key)
        if u is not None:
            return u
        if six.PY2:
            return str(uuid.uuid3(self._ns_uuid, key.encode('utf-8')))
        else:
            return str(uuid.uuid3(self._ns_uuid, key))


class XlsArchive(NsUuidArchive):
    """
    A specialization of NsUUID archive that has some nifty spreadsheet tools.
    """
"""
Interim classes with useful building blocks
"""

from __future__ import print_function, unicode_literals

import uuid

import six

from lcatools.entities import LcFlow, LcProcess, LcQuantity, LcUnit, entity_types
from lcatools.exchanges import comp_dir
from lcatools.providers.interfaces import ArchiveInterface, to_uuid

if six.PY2:
    bytes = str
    str = unicode


class OldJson(Exception):
    pass


class LcArchive(ArchiveInterface):
    """
    A class meant for building life cycle models. This takes the archive machinery and adds functions
    specific to processes, flows, and quantities. Also adds upstreaming capabilities, including (possibly
    in the future) upstream flow lookups by standardized key reference, as currently implemented in Ecoinvent lcia.

    """

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

    @staticmethod
    def _upstream_key(entity):
        if entity.entity_type == 'flow':
            return ', '.join(filter(None, [entity['Name']] + entity['Compartment']))
        elif entity.entity_type == 'quantity':
            return str(entity)
        else:
            return None

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
        for e in j['quantities']:
            self.entity_from_json(e)
        for e in j['flows']:
            self.entity_from_json(e)
        for e in j['processes']:
            self.entity_from_json(e)
        self.check_counter()

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
                    import json
                    import sys
                    print(ext_ref)
                    json.dump(c, sys.stdout, indent=2)

                    raise KeyError
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

    def processes(self):
        return [p for p in self._entities_by_type('process')]

    def flows(self):
        return [f for f in self._entities_by_type('flow')]

    def quantities(self):
        return [q for q in self._entities_by_type('quantity')]

    def lcia_methods(self):
        return [q for q in self._entities_by_type('quantity') if q.is_lcia_method()]

    def _entities_by_type(self, entity_type):
        if entity_type not in entity_types:
            entity_type = {
                'p': 'process',
                'f': 'flow',
                'q': 'quantity'
            }[entity_type[0]]
        return super(LcArchive, self)._entities_by_type(entity_type)

    def terminate(self, exchange, refs_only=False):
        """
        Generate processes in the archive that terminate a given exchange i.e. - have the same flow and a complementary
        direction.  If refs_only is specified, only report processes that terminate the exchange with a reference
        exchange.
        :param exchange:
        :param refs_only: [False] limit to reference exchanges
        :return:
        """
        for p in self.processes():
            if refs_only:
                try:
                    for x in p.references(exchange.flow):
                        if x.direction == comp_dir(exchange.direction):
                            yield p
                except StopIteration:
                    continue
            else:
                if p.has_exchange(exchange.flow, comp_dir(exchange.direction)):
                    yield p

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

    def fg_proxy(self, proxy):
        """
        A catalog service- to grab the 'native' process in the event that the locally-cached one is a stub.
        NOP by default (override in subclasses).
        :param proxy:
        :return:
        """
        return self[proxy]

    def bg_proxy(self, proxy):
        return self[proxy]

    def fg_lookup(self, process_id, ref_flow=None):
        """
        This is a template process that subclasses should override. By default, just returns the process's exchanges.
        :param process_id:
        :param ref_flow: for
        :return:
        """
        process = self.fg_proxy(process_id)
        return process.exchanges(ref_flow)

    def bg_lookup(self, process_id, ref_flow=None, reference=None, quantities=None, scenario=None, flowdb=None):
        """
        bg_lookup returns a flow representing the process's reference flow (must specify if the process is allocated)
        containing characterizations for the LCIA quantities specified
        :param process_id: the ID of the process
        :param ref_flow: the literally-specified reference flow
        :param reference: a keyword to use to find the reference flow among the process's exchanges
        :param quantities:
        :param scenario:
        :param flowdb:
        :return:
        """
        process = self.fg_proxy(process_id)
        if quantities is None:
            quantities = self.lcia_methods()
        if ref_flow is None:
            ref_flow = process.find_reference(reference).flow
        cfs_out = dict()
        for q in quantities:
            result = process.lcia(q, ref_flow=ref_flow, scenario=scenario, flowdb=flowdb)
            # ref_flow.add_characterization(q, value=result.total, location=process['SpatialScope'])
            cfs_out[q.get_uuid()] = result
        # return ref_flow
        return cfs_out

    def _quantities_with_unit(self, unitstring):
        """
        Generates a list of quantities that convert to/from the supplied unit string.
        not sure why this is useful, except I use it below to generate the first such quantity.
        :param unitstring:
        :return:
        """
        for q in self._entities_by_type('quantity'):
            if q.has_property('UnitConv'):
                if unitstring in q['UnitConv']:
                    yield q
            else:
                if q['referenceUnit'].unitstring() == unitstring:
                    yield q

    def quantity_with_unit(self, unitstring):
        """
        Just the first quantity encountered that has a given unitstring-- for locally created units only
        :param unitstring:
        :return:
        """
        return next((q for q in self._quantities_with_unit(unitstring) if q.origin == self.ref), None)

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
    def __init__(self, ref, ns_uuid=None, **kwargs):
        super(NsUuidArchive, self).__init__(ref, **kwargs)

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

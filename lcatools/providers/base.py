"""
Interim classes with useful building blocks
"""

from __future__ import print_function, unicode_literals

import six
import uuid

from lcatools.interfaces import ArchiveInterface, to_uuid
from lcatools.entities import LcFlow, LcProcess, LcQuantity, LcUnit  # , LcEntity
from lcatools.exchanges import Exchange

if six.PY2:
    bytes = str
    str = unicode


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
        for i in self._upstream.flows():
            up_key = self._upstream_flow_key(i)
            if up_key in self._upstream_hash:
                print('!!multiple upstream matches for %s!!' % up_key)
            else:
                self._upstream_hash[self._upstream_flow_key(i)] = i

    @staticmethod
    def _upstream_flow_key(flow):
        return ', '.join([flow['Name']] + flow['Compartment'])

    def _try_flow(self, uid, key):
        if self[uid] is not None:
            return self[uid]
        if key in self._upstream_hash:
            f = self._upstream_hash[key]
            self._print('Found upstream match: %s' % str(f))
            for c in f.characterizations():
                if self[c.quantity.get_uuid()] is None:
                    # this should never run, since retrieving the query should add it to the db automatically
                    print('\n quantity not found: %s.\n adding quantity %s' % (key, c.quantity))
                    self.add(c.quantity)
            try:
                self.add(f)
            except KeyError:
                if self[f.get_uuid()] is None:
                    print('upstream key: %s\next_ref: %s\n uid: %s\n get_uuid(): %s' %(key, f.get_external_ref(),
                                                                                       uid, f.get_uuid()))
                    raise ValueError('What the fuck is going on?')

                pass  # already there- fine-
            return f

    def entity_from_json(self, e):
        """
        Create an LcEntity subclass from a json-derived dict

        this could use some serious refactoring
        :param e:
        :return:
        """
        if 'tags' in e:
            self._entity_from_old_json(e)
            return
        chars = None
        exchs = None
        ext_ref = e.pop('entityId')
        uid = self._key_to_id(ext_ref)
        etype = e.pop('entityType')
        if etype == 'quantity':
            # can't move this to entity because we need _create_unit- so we wouldn't gain anything
            unit, _ = self._create_unit(e.pop('referenceUnit'))
            e['referenceUnit'] = unit
            entity = LcQuantity(uid, **e)
        elif etype == 'flow':
            try:
                e.pop('referenceQuantity')
            except KeyError:
                pass
            if 'characterizations' in e:
                chars = e.pop('characterizations')
            entity = LcFlow(uid, **e)
            if chars is not None:
                for c in chars:
                    v = None
                    q = self[c['quantity']]
                    if q is None:
                        import json, sys
                        print(ext_ref)
                        json.dump(c, sys.stdout, indent=2)

                        raise KeyError
                    if 'value' in c:
                        v = c['value']
                    if 'isReference' in c:
                        is_ref = True
                    else:
                        is_ref = False
                    entity.add_characterization(q, reference=is_ref, value=v)
        elif etype == 'process':
            # note-- we want to abandon referenceExchange notation, but we need to leave it in for backward compat
            try:
                rx = e.pop('referenceExchange')
            except KeyError:
                rx = None
            if 'exchanges' in e:
                exchs = e.pop('exchanges')
            entity = LcProcess(uid, **e)
            if exchs is not None:
                for x in exchs:
                    v = None
                    # is_ref = False
                    f = self[x['flow']]
                    d = x['direction']
                    if 'value' in x:
                        v = x['value']
                    if 'isReference' in x:
                        # is_ref = x['isReference']
                        entity.add_reference(f, d)
                    # TODO: handle allocations -- I think this will "just work" if v is a dict
                    entity.add_exchange(f, d, value=v)
                rx = None
            if rx is not None and rx != 'None':
                try:
                    direc, flow = rx.split(': ')
                    entity['referenceExchange'] = Exchange(process=entity, flow=self[flow], direction=direc)
                except AttributeError:
                    print('rx: [%s]' % rx)
                except ValueError:
                    pass
        else:
            raise TypeError('Unknown entity type %s' % e['entityType'])

        entity.set_external_ref(ext_ref)
        self.add(entity)

    def handle_old_exchanges(self, jx):
        for x in jx:
            p = self[x['process']]
            f = self[x['flow']]
            v = None
            if 'value' in x:
                v = x['value']
            d = x['direction']
            rx = p['referenceExchange']
            if rx is not None:
                is_ref = (rx.flow == f and rx.direction == d)
            else:
                is_ref = False
            p.add_exchange(f, d, reference=is_ref, value=v)

    def handle_old_characterizations(self, jc):
        for c in jc:
            f = self[c['flow']]
            q = self[c['quantity']]
            v = None
            if 'value' in c:
                v = c['value']
            rq = f['referenceQuantity']
            if rq is not None:
                is_ref = rq == q
            else:
                is_ref = False
            f.add_characterization(q, reference=is_ref, value=v)

    def _entity_from_old_json(self, e):
        d = e['tags']
        uid = self._key_to_id(e['entityId'])
        if e['entityType'] == 'quantity':
            unit, _ = self._create_unit(e['referenceUnit'])
            d['referenceUnit'] = unit
            entity = LcQuantity(uid, **d)
        elif e['entityType'] == 'flow':
            try:
                d['referenceQuantity'] = self[e['referenceQuantity']]
            except TypeError:
                pass  # allow referenceQuantity to be None
            entity = LcFlow(uid, **d)
        elif e['entityType'] == 'process':
            entity = LcProcess(uid, **d)
            try:
                direc, flow = e['referenceExchange'].split(': ')
                entity['referenceExchange'] = Exchange(process=entity, flow=self[flow], direction=direc)
            except ValueError:
                pass  # allow referenceExchange to be None
        else:
            raise TypeError('Unknown entity type %s' % e['entityType'])

        entity.set_external_ref(e['entityId'])
        self.add(entity)

    def processes(self, dataframe=False, **kwargs):
        p = self._entities_by_type('process', **kwargs)
        if dataframe:
            pass  # return self._to_pandas(p, LcProcess)
        return p

    def flows(self, dataframe=False, **kwargs):
        f = self._entities_by_type('flow', **kwargs)
        if dataframe:
            pass  # return self._to_pandas(f, LcFlow)
        return f

    def quantities(self, dataframe=False, **kwargs):
        q = self._entities_by_type('quantity', **kwargs)
        if dataframe:
            pass  # return self._to_pandas(q, LcQuantity)
        return q

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
        return next((q for q in self._quantities_with_unit(unitstring) if q['origin'] == self.ref), None)

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

        ns_uuid = to_uuid(ns_uuid)  # if it's already a uuid, keep it; if it's a string, find it; else None

        self._ns_uuid = uuid.uuid4() if ns_uuid is None else ns_uuid
        self._serialize_dict['nsUuid'] = str(self._ns_uuid)

    def _key_to_id(self, key):
        """
        Converts Ecospold01 "number" attributes to UUIDs using the internal UUID namespace.
        :param key:
        :return:
        """
        if isinstance(key, int):
            key = str(key)
        u = to_uuid(key)
        if u is not None:
            return u
        if six.PY2:
            return uuid.uuid3(self._ns_uuid, key.encode('utf-8'))
        else:
            return uuid.uuid3(self._ns_uuid, key)

"""
Interim classes with useful building blocks
"""

from __future__ import print_function, unicode_literals

import uuid

import six

from lcatools.entities import LcEntity, LcFlow, LcProcess, LcQuantity, LcUnit
from lcatools.implementations import InventoryImplementation, BackgroundImplementation, ConfigureImplementation

from lcatools.providers.interfaces import ArchiveInterface, to_uuid

if six.PY2:
    bytes = str
    str = unicode


class OldJson(Exception):
    pass


'''
LcArchive Stored Configuration.

add these objects with archive.add_config()
applied in sequence with archive.apply_config()
'''


class NsUuidArchive(ArchiveInterface):
    """
    A class that generates UUIDs in a namespace using a supplied key.  Activate this functionality by providing
    an ns_uuid argument during init.
    """
    def __init__(self, source, ns_uuid=None, **kwargs):
        super(NsUuidArchive, self).__init__(source, **kwargs)

        self._ns_uuid = None

        # internal namespace UUID for generating keys
        if ns_uuid is not None:
            if not isinstance(ns_uuid, uuid.UUID):
                ns_uuid = uuid.UUID(ns_uuid)

            self._ns_uuid = ns_uuid
            self._serialize_dict['nsUuid'] = str(self._ns_uuid)

    def _key_to_nsuuid(self, key):
        if self._ns_uuid is None:
            return None
        if isinstance(key, int):
            key = str(key)
        if six.PY2:
            return str(uuid.uuid3(self._ns_uuid, key.encode('utf-8')))
        else:
            return str(uuid.uuid3(self._ns_uuid, key))

    def _key_to_id(self, key):
        """
        If the supplied key matches a uuid string, returns it.  Otherwise, creates a uuid3 using the internal namespace.
        :param key:
        :return:
        """
        u = to_uuid(key)
        if u is None:
            return self._key_to_nsuuid(key)
        return u

    def __getitem__(self, item):
        """
        Override base to skip upstream lookup if a numeric key is being requested
        :param item:
        :return:
        """
        if isinstance(item, int) and self._ns_uuid is not None:
            return self._get_entity(self._key_to_nsuuid(item))
        return super(NsUuidArchive, self).__getitem__(item)


class BasicArchive(NsUuidArchive):
    """
    Adds on basic functionality to the archive interface: add new entities; deserialize entities
    """
    _entity_types = {'quantity', 'flow'}

    def add(self, entity):
        if entity.entity_type not in self._entity_types:
            raise ValueError('%s is not a valid entity type' % entity.entity_type)
        self._add(entity)

    def _add_children(self, entity):
        if entity.entity_type == 'quantity':
            # reset unit strings- units are such a hack
            entity.reference_entity._external_ref = entity.reference_entity.unitstring
        elif entity.entity_type == 'flow':
            # need to import all the flow's quantities
            for cf in entity.characterizations():
                self.add_entity_and_children(cf.quantity)

    def add_entity_and_children(self, entity):
        try:
            self.add(entity)
        except KeyError:
            return
        self._add_children(entity)

    @staticmethod
    def _upstream_key(entity):
        if entity.entity_type == 'quantity':
            return str(entity)  # needs to be something indexed by the Qdb
        else:
            return None

    def _create_unit(self, unitstring):
        """
        This returns two things: an LcUnit having the given unit string, and a dict of conversion factors
        (or None if the class doesn't support it).  The dict should have unit strings as keys, and the values should
        have the property that each key-value pair has the same real magnitude.  In other words, the [numeric] values
        should report the number of [keys] that is equal to the reference unit.  e.g. for a reference unit of 'kg',
        the UnitConversion entry for 'lb' should have the value 2.2046... because 2.2046 lb = 1 kg

        In many cases, this will require the supplied conversion value to be inverted.

        The conversion dict should be stored in the Quantity's UnitConversion property.  See IlcdArchive for an
        example implementation.
        :param unitstring:
        :return:
        """
        return LcUnit(unitstring), None

    def _quantity_from_json(self, entity_j, uid):
        # can't move this to entity because we need _create_unit- so we wouldn't gain anything
        unit, _ = self._create_unit(entity_j.pop('referenceUnit'))
        entity_j['referenceUnit'] = unit
        quantity = LcQuantity(uid, **entity_j)
        return quantity

    def _flow_from_json(self, entity_j, uid):
        if 'referenceQuantity' in entity_j:
            entity_j.pop('referenceQuantity')
        chars = entity_j.pop('characterizations', [])
        flow = LcFlow(uid, **entity_j)
        for c in chars:
            v = None
            q = self._get_entity(c['quantity'])
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
            flow.add_characterization(q, reference=is_ref, value=v)

        return flow

    def _make_entity(self, e, etype, uid):
        if etype == 'quantity':
            entity = self._quantity_from_json(e, uid)
        elif etype == 'flow':
            entity = self._flow_from_json(e, uid)
        else:
            raise TypeError('Unknown entity type %s' % etype)
        return entity

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

        entity = self._make_entity(e, etype, uid)

        entity.origin = origin
        self.add(entity)
        if self[ext_ref] is entity:
            entity.set_external_ref(ext_ref)
        else:
            print('## skipping bad external ref %s for uuid %s' % (ext_ref, uid))

    def load_json(self, j, _check=True):
        """
        Archives loaded from JSON files are considered static.
        :param j:
        :param _check:
        :return:
        """
        if 'quantities' in j:
            for e in j['quantities']:
                self.entity_from_json(e)
        if 'flows' in j:
            for e in j['flows']:
                self.entity_from_json(e)
        if _check:
            self.check_counter()


class LcArchive(BasicArchive):
    """
    A class meant for building life cycle models. This takes the archive machinery and adds functions
    specific to processes, flows, and quantities. Creates an upstream lookup for quantities but no other entity types.

    Note: in lieu of having a @classmethod from_json, we have an archive factory that produces archives using the
    appropriate constructor given the data type.  This is found in lcatools.tools and calls the base entity_from_json
    method-- which itself should be offloaded to the entities wherever possible.

    """
    _entity_types = {'quantity', 'flow', 'process'}

    @classmethod
    def from_dict(cls, j):
        """
        LcArchive factory from minimal dictionary.  Must include at least one of 'dataSource' or 'dataReference' fields
        and 0 or more processes, flows, or quantities; but note that any flow present must have its reference quantities
        included, and any process must have its exchanged flows included.
        :param j:
        :return:
        """
        source = j.pop('dataSource', None)
        try:
            ref = j.pop('dataReference')
        except KeyError:
            if source is None:
                print('Dictionary must contain at least a dataSource or a dataReference specification.')
                return None
            else:
                ref = None
        ar = cls(source, ref=ref)
        ar.load_json(j)
        return ar

    def __init__(self, source, **kwargs):
        """
        gotta get rid of that upstream cruft at some point
        :param source:
        :param kwargs:
        """
        self._upstream_hash = dict()  # deprecated ## for lookup use later
        super(LcArchive, self).__init__(source, **kwargs)

    def __getitem__(self, item):
        """
        Note: this user-friendliness check adds 20% to the execution time of getitem-- so avoid it if possible
        (use _get_entity directly -- especially now that upstream is now deprecated)

        :param item:
        :return:
        """
        if isinstance(item, LcEntity):
            return self._get_entity(item.uuid)
        return super(LcArchive, self).__getitem__(item)

    def make_interface(self, iface, privacy=None):
        if iface == 'inventory':
            return InventoryImplementation(self, privacy=privacy)
        elif iface == 'background':
            return BackgroundImplementation(self, privacy=privacy)
        elif iface == 'configure':
            return ConfigureImplementation(self, privacy=privacy)
        else:
            return super(LcArchive, self).make_interface(iface, privacy=privacy)

    def set_upstream(self, upstream):
        super(LcArchive, self).set_upstream(upstream)
        # create a dict of upstream quantities
        self._upstream_hash = dict()  # clobber old dict
        for i in self._upstream.entities_by_type('quantity'):
            if not i.is_entity:
                continue
            up_key = self._upstream_key(i)
            if up_key in self._upstream_hash:
                print('!!multiple upstream matches for %s!!' % up_key)
            elif up_key is None:
                continue
            else:
                self._upstream_hash[up_key] = i

    def _add_children(self, entity):
        if entity.entity_type == 'process':
            # need to import all the process's flows
            for x in entity.exchanges():
                self.add_entity_and_children(x.flow)
        else:
            super(LcArchive, self)._add_children(entity)

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

    def load_json(self, j, _check=True):
        """
        Archives loaded from JSON files are considered static.
        :param j:
        :param _check:
        :return:
        """
        super(LcArchive, self).load_json(j, _check=False)
        if 'processes' in j:
            for e in j['processes']:
                self.entity_from_json(e)
        if _check:
            self.check_counter()

    def _process_from_json(self, entity_j, uid):
        # note-- we are officially abandoning referenceExchange notation
        if 'referenceExchange' in entity_j:
            entity_j.pop('referenceExchange')
        exchs = entity_j.pop('exchanges', [])
        process = LcProcess(uid, **entity_j)
        refs, nonrefs = [], []
        ref_x = dict()
        for i, x in enumerate(exchs):
            if 'isReference' in x and x['isReference'] is True:
                refs.append(i)
            else:
                nonrefs.append(i)
        # first add reference exchanges
        for i in refs:
            x = exchs[i]
            # eventually move this to an exchange classmethod - which is why I'm repeating myself for now
            v = None
            f = self._get_entity(x['flow'])
            d = x['direction']
            if 'value' in x:
                v = x['value']
            ref_x[x['flow']] = process.add_exchange(f, d, value=v)
            process.add_reference(f, d)
        # then add ordinary [allocated] exchanges
        for i in nonrefs:
            x = exchs[i]
            t = None
            # is_ref = False
            f = self._get_entity(x['flow'])
            d = x['direction']
            if 'termination' in x:
                t = x['termination']
            if 'value' in x:
                process.add_exchange(f, d, value=x['value'], termination=t)

            if 'valueDict' in x:
                for k, val in x['valueDict'].items():
                    drr, fuu = k.split(':')
                    try:
                        rx = ref_x[fuu]
                    except KeyError:
                        process.show()
                        print('key: %s' % k)
                        print('flow: %s' % self[fuu])
                        raise
                    # assert rx.direction == drr
                    process.add_exchange(f, d, reference=rx, value=val, termination=t)

        return process

    def _make_entity(self, e, etype, uid):
        if etype == 'process':
            return self._process_from_json(e, uid)
        return super(LcArchive, self)._make_entity(e, etype, uid)

    def entities_by_type(self, entity_type):
        if entity_type not in self._entity_types:
            entity_type = {
                'p': 'process',
                'f': 'flow',
                'q': 'quantity'
            }[entity_type[0]]
        return super(LcArchive, self).entities_by_type(entity_type)

    def serialize(self, exchanges=False, characterizations=False, values=False):
        """

        :param exchanges:
        :param characterizations:
        :param values:
        :return:
        """
        j = super(LcArchive, self).serialize()
        j['processes'] = sorted([p.serialize(exchanges=exchanges, values=values)
                                 for p in self.entities_by_type('process')],
                                key=lambda x: x['entityId'])
        j['flows'] = sorted([f.serialize(characterizations=characterizations, values=values)
                             for f in self.entities_by_type('flow')],
                            key=lambda x: x['entityId'])
        j['quantities'] = sorted([q.serialize()
                                  for q in self.entities_by_type('quantity')],
                                 key=lambda x: x['entityId'])
        return j

    def _serialize_all(self, **kwargs):
        return self.serialize(exchanges=True, characterizations=True, values=True)

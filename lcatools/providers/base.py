"""
Interim classes with useful building blocks
"""

from __future__ import print_function, unicode_literals

import re

import six

from lcatools.entities import LcEntity, LcFlow, LcProcess, LcQuantity, LcUnit
from lcatools.implementations import BasicImplementation, IndexImplementation, QuantityImplementation

from lcatools.implementations import InventoryImplementation, BackgroundImplementation, ConfigureImplementation

from lcatools.providers.interfaces import ArchiveInterface

if six.PY2:
    bytes = str
    str = unicode


class OldJson(Exception):
    pass


class EntityExists(Exception):
    pass

'''
LcArchive Stored Configuration.

add these objects with archive.add_config()
applied in sequence with archive.apply_config()
'''


class BasicArchive(ArchiveInterface):
    """
    Adds on basic functionality to the archive interface: add new entities; deserialize entities.

    The BasicArchive should be used for all archives that only contain flows and quantities (and contexts in the future)

    """
    _entity_types = {'quantity', 'flow'}

    def _check_key_unused(self, key):
        """
        If the key is unused, return the UUID. Else raise EntityExists
        :param key:
        :return:
        """
        u = self._key_to_nsuuid(key)
        try:
            e = self._get_entity(u)
        except KeyError:
            return u
        raise EntityExists(str(e))

    def new_quantity(self, name, ref_unit, **kwargs):
        u = self._check_key_unused(name)
        q = LcQuantity(u, ref_unit=LcUnit(ref_unit), Name=name, origin=self.ref, external_ref=name, **kwargs)
        self.add(q)
        return q

    def new_flow(self, name, ref_qty, CasNumber='', **kwargs):
        u = self._check_key_unused(name)
        f = LcFlow(u, Name=name, ReferenceQuantity=ref_qty, CasNumber=CasNumber, origin=self.ref, external_ref=name,
                   **kwargs)
        self.add_entity_and_children(f)
        return f

    def make_interface(self, iface, privacy=None):
        if iface == 'basic':
            return BasicImplementation(self, privacy=privacy)
        elif iface == 'quantity':
            return QuantityImplementation(self, privacy=privacy)
        elif iface == 'index':
            return IndexImplementation(self, privacy=privacy)

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

    @staticmethod
    def _narrow_search(entity, **kwargs):
        """
        Narrows a result set using sequential keyword filtering
        :param entity:
        :param kwargs:
        :return: bool
        """
        def _recurse_expand_subtag(tag):
            if tag is None:
                return ''
            elif isinstance(tag, str):
                return tag
            else:
                return ' '.join([_recurse_expand_subtag(t) for t in tag])
        keep = True
        for k, v in kwargs.items():
            if k not in entity.keys():
                return False
            if isinstance(v, str):
                v = [v]
            for vv in v:
                keep = keep and bool(re.search(vv, _recurse_expand_subtag(entity[k]), flags=re.IGNORECASE))
        return keep

    def search(self, etype=None, upstream=False, **kwargs):
        """
        Find entities by search term, either full or partial uuid or entity property like 'Name', 'CasNumber',
        or so on.
        :param etype: optional first argument is entity type
        :param upstream: (False) if upstream archive exists, search there too
        :param kwargs: regex search through entities' properties as named in the kw arguments
        :return: result set
        """
        if etype is None:
            if 'entity_type' in kwargs.keys():
                etype = kwargs.pop('entity_type')
        if etype is not None:
            for ent in self.entities_by_type(etype):
                if self._narrow_search(ent, **kwargs):
                    yield ent
        else:
            for ent in self._entities.values():
                if self._narrow_search(ent, **kwargs):
                    yield ent
        if upstream and self._upstream is not None:
            self._upstream.search(etype, upstream=upstream, **kwargs)

    def serialize(self, characterizations=False, values=False):
        """

        :param characterizations:
        :param values:
        :return:
        """
        j = super(BasicArchive, self).serialize()
        j['flows'] = sorted([f.serialize(characterizations=characterizations, values=values)
                             for f in self.entities_by_type('flow')],
                            key=lambda x: x['entityId'])
        j['quantities'] = sorted([q.serialize()
                                  for q in self.entities_by_type('quantity')],
                                 key=lambda x: x['entityId'])
        return j

    def _serialize_all(self, **kwargs):
        return self.serialize(characterizations=True, values=True)


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

    def __getitem__(self, item):
        """
        Note: this user-friendliness check adds 20% to the execution time of getitem-- so avoid it if possible
        (use _get_entity directly -- especially now that upstream is now deprecated)

        :param item:
        :return:
        """
        if isinstance(item, LcEntity):
            item = item.uuid
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
        return j

    def _serialize_all(self, **kwargs):
        return self.serialize(exchanges=True, characterizations=True, values=True)

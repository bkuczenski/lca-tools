import re
from collections import defaultdict
from .entity_store import EntityStore, SourceAlreadyKnown
from ..interfaces import to_uuid
from ..implementations import BasicImplementation, IndexImplementation, QuantityImplementation
from lcatools.entities import LcQuantity, LcUnit, LcFlow
from lcatools import from_json


class OldJson(Exception):
    pass


class EntityExists(Exception):
    pass


class InterfaceError(Exception):
    pass


class ArchiveError(Exception):
    pass



BASIC_ENTITY_TYPES = ('quantity', 'flow')


'''
LcArchive Stored Configuration.

add these objects with archive.add_config()
applied in sequence with archive.apply_config()
'''


class BasicArchive(EntityStore):
    """
    Adds on basic functionality to the archive interface: add new entities; deserialize entities.

    The BasicArchive should be used for all archives that only contain flows and quantities (and contexts in the future)

    """
    _entity_types = set(BASIC_ENTITY_TYPES)

    _drop_fields = defaultdict(list)  # dict mapping entity type to fields that should be omitted from serialization

    @classmethod
    def from_file(cls, filename):
        """
        BasicArchive factory from minimal dictionary.  Must include at least one of 'dataSource' or 'dataReference'
        fields and 0 or more flows or quantities; but note that any flow present must have its reference
        quantities included. The method is inherited by LcArchives which permit processes as well; any process must
        have its exchanged flows (and their respective quantities) included.
        :param filename: The name of the file to be loaded
        :return:
        """
        j = from_json(filename)
        try:
            ref = j['dataReference']
        except KeyError:
            ref = None
        init_args = j.pop('initArgs', {})
        ns_uuid = j.pop('nsUuid', None)  # this is for opening legacy files
        if ns_uuid is None:
            ns_uuid = init_args.pop('ns_uuid', None)
        ar = cls(filename, ref=ref, ns_uuid=ns_uuid, **init_args)
        ar.load_from_dict(j, jsonfile=filename)
        return ar

    @classmethod
    def from_already_open_file(cls, j, filename, ref=None, **kwargs):
        """
        This is an in-between function that should probably be refactored away / folded into archive_from_json (which
        is the only place it's used)
        :param j:
        :param filename:
        :param ref:
        :param kwargs:
        :return:
        """
        if ref is None:
            try:
                ref = j['dataReference']
            except KeyError:
                if filename is None:
                    print('At least one of source filename or ref kwarg or dataReference must be specified')
                    return None
                else:
                    ref = None
        init_args = j.pop('initArgs', {})
        ns_uuid = j.pop('nsUuid', None)  # this is for opening legacy files
        if ns_uuid is None:
            ns_uuid = init_args.pop('ns_uuid', None)
        kwargs.update(init_args)
        ar = cls(filename, ref=ref, ns_uuid=ns_uuid, static=True, **kwargs)
        ar.load_from_dict(j, jsonfile=filename)
        return ar

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

    def make_interface(self, iface):
        if iface == 'basic':
            return BasicImplementation(self)
        elif iface == 'quantity':
            return QuantityImplementation(self)
        elif iface == 'index':
            return IndexImplementation(self)
        else:
            raise InterfaceError('Unable to create interface %s' % iface)

    def add(self, entity):
        if entity.entity_type not in self._entity_types:
            raise ValueError('%s is not a valid entity type' % entity.entity_type)
        self._add(entity, entity.uuid)

    def _add_children(self, entity):
        if entity.entity_type == 'quantity':
            # reset unit strings- units are such a hack
            if isinstance(entity.reference_entity, LcUnit):
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
        entity_j.pop('externalId')  # TODO
        # can't move this to entity because we need _create_unit- so we wouldn't gain anything
        unit, _ = self._create_unit(entity_j.pop('referenceUnit'))
        entity_j['referenceUnit'] = unit
        quantity = LcQuantity(uid, **entity_j)
        return quantity

    def _flow_from_json(self, entity_j, uid):
        entity_j.pop('externalId')  # TODO
        if 'referenceQuantity' in entity_j:
            entity_j.pop('referenceQuantity')
        chars = entity_j.pop('characterizations', [])
        flow = LcFlow(uid, **entity_j)
        for c in chars:
            v = None
            q = self[c['quantity']]  # this is required because of foreground; _process_from_json unaffected
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
        uid = e.pop('entityId', None)
        ext_ref = e['externalId']  # TODO: need this still present
        if uid is None:
            uid = to_uuid(ext_ref)
            if uid is None:
                raise OldJson('This entity has no UUID and an invalid external ref')
        etype = e.pop('entityType')
        e['origin'] = e.pop('origin', self.ref)

        entity = self._make_entity(e, etype, uid)

        self.add(entity)
        if self[ext_ref] is entity:
            entity.set_external_ref(ext_ref)
        else:
            print('## skipping bad external ref %s for uuid %s' % (ext_ref, uid))

    def load_from_dict(self, j, _check=True, jsonfile=None):
        """
        Archives loaded from JSON files are considered static.
        :param j:
        :param _check: whether to run check_counter to print out statistics at the end
        :param jsonfile: [None] if present, add to the list of sources for the canonical ref
        :return:
        """

        if 'catalogNames' in j:
            for ref, l in j['catalogNames'].items():
                if isinstance(l, str) or l is None:
                    self._add_name(ref, l)
                else:
                    for s in l:
                        self._add_name(ref, s)

        if 'dataReference' in j:
            # new style
            source = j['dataSource']
            ref = j['dataReference']
            try:
                self._add_name(ref, source)
            except SourceAlreadyKnown:
                self._add_name(ref, None)

        if jsonfile is not None:
            self._add_name(self.ref, jsonfile, rewrite=True)

        if 'quantities' in j:
            for e in j['quantities']:
                self.entity_from_json(e)
        if 'flows' in j:
            for e in j['flows']:
                self.entity_from_json(e)

        if 'loaded' in j:
            self._loaded = j['loaded']

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

    def serialize(self, characterizations=False, values=False, domesticate=False):
        """

        :param characterizations:
        :param values:
        :param domesticate: [False] if True, omit entities' origins so that they will appear to be from the new archive
         upon serialization
        :return:
        """
        j = super(BasicArchive, self).serialize()
        j['flows'] = sorted([f.serialize(characterizations=characterizations, values=values,
                                         domesticate=domesticate, drop_fields=self._drop_fields['flow'])
                             for f in self.entities_by_type('flow')],
                            key=lambda x: x['entityId'])
        j['quantities'] = sorted([q.serialize(domesticate=domesticate, drop_fields=self._drop_fields['quantity'])
                                  for q in self.entities_by_type('quantity')],
                                 key=lambda x: x['entityId'])
        return j

    def _serialize_all(self, **kwargs):
        return self.serialize(characterizations=True, values=True, **kwargs)

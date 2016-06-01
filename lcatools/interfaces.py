"""
This file defines a set of abstract classes that act as interfaces for the data providers.

The data providers are expected to inherit from this class, which will reply "not implemented"
for all the interface methods. Then the data providers can override whichever ones do get implemented.

On the calling side, the interface definitions function as documentation-only, since python uses
duck typing and doesn't require strict interface definitions.
"""
from __future__ import print_function, unicode_literals

import uuid
import re
import json
import gzip as gz
from lcatools.entities import LcFlow, LcProcess, LcQuantity, LcEntity, LcUnit
from lcatools.exchanges import Exchange
from lcatools.characterizations import CharacterizationSet, Characterization
from collections import defaultdict

import pandas as pd

uuid_regex = re.compile('([0-9a-f]{8}.?([0-9a-f]{4}.?){3}[0-9a-f]{12})')


def to_uuid(_in):
    if isinstance(_in, uuid.UUID):
        return _in
    if _in is None:
        return _in
    try:
        g = uuid_regex.search(_in)
        if g is not None:
            try:
                _out = uuid.UUID(g.groups()[0])
            except ValueError:
                _out = None
        else:
            _out = None
    except TypeError:
        _out = None
    return _out


class ArchiveInterface(object):
    """
    An abstract interface has nothing but a reference

    """

    def __init__(self, ref, quiet=False, upstream=None):
        self.ref = ref
        self._entities = {}  # uuid-indexed list of known entities
        self._exchanges = set()  # set of exchanges among the entities
        self._characterizations = CharacterizationSet()  # set of flow characterizations among the entities

        self._quiet = quiet  # whether to print out a message every time a new entity is added / deleted / modified

        self._counter = defaultdict(int)
        if upstream is not None:
            assert isinstance(upstream, ArchiveInterface)
        self._upstream = upstream

    def __getitem__(self, item):
        return self._get_entity(item)

    def add(self, entity):
        key = entity.get_external_ref()
        u = self.key_to_id(key)
        if u is None:
            raise ValueError('Key must be a valid UUID')

        if u in self._entities:
            raise KeyError('Entity already exists')

        if entity.validate():
            if self._quiet is False:
                print('Adding %s entity with %s: %s' % (entity.entity_type, u, entity['Name']))
            self._entities[u] = entity
            self._counter[entity.entity_type] += 1

        else:
            raise ValueError('Entity fails validation.')

    def check_counter(self, entity_type=None):
        if entity_type is None:
            [self.check_counter(entity_type=k) for k in ('process', 'flow', 'quantity')]
        else:
            print('%d new %s entities added (%d total)' % (self._counter[entity_type], entity_type,
                                                           len(self._entities_by_type(entity_type))))
            self._counter[entity_type] = 0

    @classmethod
    def _create_unit(cls, unitstring):
        return LcUnit(unitstring), None

    @classmethod
    def key_to_id(cls, key):
        """
        in the base class, the key is the uuid
        :param key:
        :return:
        """
        return to_uuid(key)

    def entity_from_json(self, e):
        """
        Create an LcEntity subclass from a json-derived dict
        :param e:
        :return:
        """
        d = e['tags']
        uid = self.key_to_id(e['entityId'])
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

    def _add_exchange(self, exchange):
        if exchange.entity_type == 'exchange':
            self._exchanges.add(exchange)

    def add_exchanges(self, jx):
        """
        jx is a list of json-derived exchange dictionaries
        :param jx:
        :return:
        """
        for x in jx:
            self._add_exchange(Exchange(process=self[x['process']],
                               flow=self[x['flow']],
                               direction=x['direction']))

    def _add_characterization(self, characterization):
        if characterization.entity_type == 'characterization':
            self._characterizations.add(characterization)

    def _get_entity(self, key):
        """
        Retrieve an exact entity by UUID specification- either a uuid.UUID or a string that can be
        converted to a valid UUID.

        If the UUID is not found, returns None. handle this case in client code/subclass.
        :param key: something that maps to a literal UUID via key_to_id
        :return: the LcEntity or None
        """
        if key is None:
            return None
        entity = self.key_to_id(key)
        if entity in self._entities:
            e = self._entities[entity]
            if 'origin' not in e.keys():
                e['origin'] = self.ref
            return e
        elif self._upstream is not None:
            return self._upstream[key]
        else:
            return None

    @staticmethod
    def _narrow_search(result_set, **kwargs):
        """
        Narrows a result set using sequential keyword filtering
        :param result_set:
        :param kwargs:
        :return:
        """
        def _recurse_expand_subtag(tag):
            if isinstance(tag, str):
                return tag
            else:
                return ' '.join([_recurse_expand_subtag(t) for t in tag])
        for k, v in kwargs.items():
            result_set = [r for r in result_set if bool(re.search(v,
                                                                  _recurse_expand_subtag(r[k]),
                                                                  flags=re.IGNORECASE))]
        return result_set

    def search(self, *args, **kwargs):
        uid = None if len(args) == 0 else args[0]
        if uid is not None:
            # search on uuids
            result_set = [self._get_entity(k) for k in self._entities.keys()
                          if bool(re.search(uid, str(k), flags=re.IGNORECASE))]
        else:
            if 'entity_type' in kwargs.keys():
                result_set = self._entities_by_type(kwargs.pop('entity_type'))
            else:
                result_set = [self._get_entity(k) for k in self._entities.keys()]
        if len(result_set) > 0:
            return self._narrow_search(result_set, **kwargs)
        elif self._upstream is not None:
            return self._upstream.search(*args, **kwargs)

    def _fetch(self, entity, **kwargs):
        """
        Dummy function to fetch from archive. MUST be overridden.
        Can't fetch from upstream.
        :param entity:
        :return:
        """
        raise NotImplemented

    def retrieve_or_fetch_entity(self, *args, **kwargs):
        """
        Client-facing function to retrieve entity by ID, first locally, then in archive.

        Input is flexible-- could be a UUID or partial UUID (

        :param args: the identifying string (uuid or partial uuid)
        :param kwargs: used to filter search results on the local archive
        :return:
        """
        if len(args) > 0:
            uid = args[0]
        else:
            uid = None

        entity = self._get_entity(uid)  # this checks upstream if it exists
        if entity is not None:
            # retrieve
            return entity

        # search locally
        result_set = self.search(uid, **kwargs)
        if len(result_set) == 1:
            return result_set[0]
        elif len(result_set) > 1:
            return result_set

        # fetch
        return self._fetch(uid, **kwargs)

    def validate_entity_list(self):
        count = 0
        for k, v in self._entities.items():
            valid = True
            # 1: confirm key is a UUID
            if not isinstance(k, uuid.UUID):
                print('Key %s is not a valid UUID.' % k)
                valid = False
            # 2: confirm entity's external key maps to its uuid
            if self.key_to_id(v.get_external_key()) != k:
                print("%s: Key doesn't match UUID!" % v.get_external_key())
                valid = False

            # confirm entity is dict-like with keys() and with a set of common keys
            try:
                valid = valid & v.validate()
            except AttributeError:
                print('Key %s: not a valid LcEntity (no validate() method)' % k)
                valid = False

            if valid:
                count += 1
        print('%d entities validated out of %d' % (count, len(self._entities)))
        return count

    def _load_all(self):
        """
        Must be overridden in subclass
        :return:
        """
        raise NotImplemented

    def load_all(self):
        print('Loading %s' % self.ref)
        self._load_all()

    def _entities_by_type(self, entity_type, **kwargs):
        result_set = [self._get_entity(k) for k, v in self._entities.items() if v['EntityType'] == entity_type]
        return self._narrow_search(result_set, **kwargs)

    @staticmethod
    def _to_pandas(entities, EntityClass=LcEntity, **kwargs):
        """
        Creates an entity-type-specific DataFrame of entities.  Kind of funky and special purpose.
        :param entities:
        :param EntityClass: LcEntity subclass (used for determining signature fields)
        :param kwargs:
        :return:
        """
        sig = [p.get_signature() for p in entities]
        index = [p.get_uuid() for p in entities]
        df = pd.DataFrame(sig, index=index, columns=[i for i in EntityClass.signature_fields()], **kwargs)
        df.index.name = 'UUID'
        return df

    def processes(self, dataframe=False, **kwargs):
        p = self._entities_by_type('process', **kwargs)
        if dataframe:
            return self._to_pandas(p, LcProcess)
        else:
            return p

    def flows(self, dataframe=False, **kwargs):
        f = self._entities_by_type('flow', **kwargs)
        if dataframe:
            return self._to_pandas(f, LcFlow)
        else:
            return f

    def quantities(self, dataframe=False, **kwargs):
        q = self._entities_by_type('quantity', **kwargs)
        if dataframe:
            return self._to_pandas(q, LcQuantity)
        else:
            return q

    def exchanges(self, dataframe=False):
        x = [ex for ex in self._exchanges]
        if dataframe:
            return self._to_pandas(x, Exchange)
        else:
            return x

    def characterizations(self, dataframe=False):
        x = [ex for ex in self._characterizations]
        if dataframe:
            return self._to_pandas(x, Characterization)
        else:
            return x

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
        Just the first quantity encountered that has a given unitstring
        :param unitstring:
        :return:
        """
        return next((q for q in self._quantities_with_unit(unitstring)), None)

    def serialize(self, exchanges=False, characterizations=False):
        return {
            'dataSourceType': self.__class__.__name__,
            'dataSourceReference': self.ref,
            'processes': sorted([p.serialize() for p in self.processes()], key=lambda x: x['entityId']),
            'flows': sorted([f.serialize() for f in self.flows()], key=lambda x: x['entityId']),
            'quantities': sorted([q.serialize() for q in self.quantities()], key=lambda x: x['entityId']),
            'exchanges': [] if exchanges is False else sorted([x.serialize() for x in self.exchanges()],
                                                              key=lambda x: x['flow']),
            'characterizations': [] if characterizations is False else
            sorted([x.serialize() for x in self.characterizations()], key=lambda x: x['flow'])
        }

    def write_to_file(self, filename, gzip=False, **kwargs):
        s = self.serialize(**kwargs)
        if gzip is True:
            if not bool(re.search('\.gz$', filename)):
                filename += '.gz'
            try:  # python3
                with gz.open(filename, 'wt') as fp:
                    json.dump(s, fp, indent=2, sort_keys=True)
            except ValueError:  # python2
                with gz.open(filename, 'w') as fp:
                    json.dump(s, fp, indent=2, sort_keys=True)
        else:
            with open(filename, 'w') as fp:
                json.dump(s, fp, indent=2, sort_keys=True)


class CatalogInterface(object):
    """
    A catalog is a container for a set of distinct archives, and provides useful services for accessing information
    within them.  Catalog functionality is TODO but will include:

     * maintain a dict of archives with convenient keys (default is archive.ref)
     * retrieve entity by UUID or by external ref
     * store synonyms
    """

    pass


class ProcessFlowInterface():
    """

    def list_processes(self):
        r = []
        for k, v in self.catalogs.items():
            if v['EntityType'] == 'process':
                r.append(v.get_signature())
        return sorted(r)
    """


class FlowQuantityInterface(object):
    """
    A Flow-Quantity service stores linked observations of flows and quantities with "factors" which report the
     magnitude of the quantity, in proportion to the flow's reference quantity (which is implicitly mass in
     the ecoinvent LCIA spreadsheet).

    The flow-quantity interface allows the following:

      * add_cf : register a link between a flow and a quantity having a particular factor

      * lookup_cf : specify characteristics to match and return a result set

      *

      * report characterizations that link one flow with one quantity.




    """
    pass

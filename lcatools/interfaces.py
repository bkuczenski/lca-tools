"""
This file defines a set of abstract classes that act as interfaces for the data providers.

The data providers are expected to inherit from this class, which will reply "not implemented"
for all the interface methods. Then the data providers can override whichever ones do get implemented.

On the calling side, the interface definitions function as documentation-only, since python uses
duck typing and doesn't require strict interface definitions.
"""
import uuid
import re
from lcatools.entities import *
from lcatools.exchanges import Exchange

import pandas as pd

uuid_regex = re.compile('([0-9a-f]{8}.?([0-9a-f]{4}.?){3}.?[0-9a-f]{12})')


def _to_uuid(_in):
    if isinstance(_in, uuid.UUID):
        return _in
    if uuid_regex.match(_in):
        try:
            _out = uuid.UUID(_in)
        except ValueError:
            _out = None
    else:
        _out = None
    return _out


class BasicInterface(object):
    """
    An abstract interface has nothing but a reference

    """

    def __init__(self, ref):
        self.ref = ref
        self._entities = {}  # uuid-indexed list of known entities
        self._exchanges = set()  # set of exchanges among the entities
        self._characterizations = set()  # set of flow characterizations among the entities

    def __getitem__(self, item):
        return self._get_entity(item)

    def __setitem__(self, key, value):
        u = _to_uuid(key)
        if u is None:
            raise ValueError('Key must be a valid UUID')

        if u in self._entities:
            raise KeyError('Entity already exists')

        if value.validate(u):
            print('Adding %s entity with %s: %s' % (value.entity_type, u, value['Name']))
            self._entities[u] = value

        else:
            raise ValueError('Entity fails validation.')

    def _get_entity(self, entity):
        """
        We need to accommodate whatever input the user gives us and map it to a UUID
         in our private _entities list.  If it's an actual UUID, do the lookup. If it's a string
         that corresponds to a valid uuid, do the conversion and then the lookup.
         Otherwise, [extend future semantic capabilities]
         Note - future extensions happen in subclasses, not in the BasicInterface. So no errors caught.
        :param entity:
        :return:
        """
        if entity is None:
            return None
        entity = _to_uuid(entity)
        if entity in self._entities:
            return self._entities[entity]
        else:
            return None

    @staticmethod
    def _narrow_search(result_set, **kwargs):
        for k, v in kwargs.items():
            result_set = [r for r in result_set if bool(re.search(v, r[k], flags=re.IGNORECASE))]
        return result_set

    def _add_exchange(self, exchange):
        if exchange.entity_type == 'exchange':
            self._exchanges.add(exchange)

    def _add_characterization(self, characterization):
        if characterization.entity_type == 'characterization':
            self._characterizations.add(characterization)

    def search(self, *args, **kwargs):
        uid = None if len(args) == 0 else args[0]
        if uid is not None:
            # search on uuids
            result_set = [v for k, v in self._entities.items()
                          if bool(re.search(uid, str(k), flags=re.IGNORECASE))]
        else:
            result_set = [v for v in self._entities.values()]

        return self._narrow_search(result_set, **kwargs)

    def _fetch(self, entity, **kwargs):
        """
        Dummy function to fetch from archive. MUST be overridden
        :param entity:
        :return:
        """
        raise NotImplemented

    def retrieve_or_fetch_entity(self, *args, **kwargs):
        """
        Client-facing function to retrieve entity by ID, first locally, then in archive

        :param args: the identifying string (uuid or partial uuid)
        :param kwargs: used to filter search results on the local archive
        :return:
        """
        if len(args) > 0:
            uid = args[0]
        else:
            uid = None

        entity = self._get_entity(uid)
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

    def list_properties(self, entity):
        e = self._get_entity(entity)
        if e is not None:
            return e.properties()
        return e

    def get_properties(self, entity):
        """

        :param entity: a uuid
        :return:
        """
        d = dict()
        e = self._get_entity(entity)
        if e is not None:
            for i in e.properties():
                d[i] = e[i]
        return d

    def validate_entity_list(self):
        count = 0
        for k, v in self._entities.items():
            valid = True
            # 1: confirm key is a UUID
            if not isinstance(k, uuid.UUID):
                print('Key %s is not a valid UUID.' % k)
                valid = False

            # confirm entity is dict-like with keys() and with a set of common keys
            try:
                valid = valid & v.validate(k)
            except AttributeError:
                print('Key %s: not a valid LcEntity (no validate() method)' % k)
                valid = False

            if valid:
                count += 1
        print('%d entities validated out of %d' % (count, len(self._entities)))
        return count

    def _entities_by_type(self, entity_type, **kwargs):
        result_set = [v for v in self._entities.values() if v['EntityType'] == entity_type]
        return self._narrow_search(result_set, **kwargs)

    def _to_pandas(self, entities, EntityClass=LcEntity, **kwargs):
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

    def exchanges(self, dataframe=False, **kwargs):
        x = self._narrow_search([x for x in self._exchanges], **kwargs)
        if dataframe:
            return self._to_pandas(x, Exchange)
        else:
            return x

    def _quantities_with_unit(self, unitstring):
        for q in self._entities_by_type('quantity'):
            if q.has_property('UnitConv'):
                if unitstring in q['UnitConv']:
                    yield q
            else:
                if q['ReferenceUnit'].unitstring() == unitstring:
                    yield q

    def quantity_with_unit(self, unitstring):
        return next((q for q in self._quantities_with_unit(unitstring)), None)

    def serialize(self, exchanges=False):
        return {
            'dataSourceType': self.__class__.__name__,
            'dataSourceReference': self.ref,
            'processes': [p.serialize() for p in self.processes()],
            'flows': [f.serialize() for f in self.flows()],
            'quantities': [q.serialize() for q in self.quantities()],
            'exchanges': [] if exchanges is False else [x.serialize() for x in self.exchanges()]
        }


class ProcessFlow(BasicInterface):
    """

    """
    def list_processes(self):
        r = []
        for k, v in self._entities.items():
            if v['EntityType'] == 'process':
                r.append(v.get_signature())
        return sorted(r)


class FlowQuantity(BasicInterface):
    """

    """
    pass

"""
This file defines a set of abstract classes that act as interfaces for the data providers.

The data providers are expected to inherit from this class, which will reply "not implemented"
for all the interface methods. Then the data providers can override whichever ones do get implemented.

On the calling side, the interface definitions function as documentation-only, since python uses
duck typing and doesn't require strict interface definitions.
"""
import uuid
import re
from lcatools.entities import LcEntity

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

    def __getitem__(self, item):
        return self._get_entity(item)

    def __setitem__(self, key, value):
        u = _to_uuid(key)
        if u is None:
            raise ValueError('Key must be a valid UUID')

        if u in self._entities:
            raise KeyError('Entity already exists')

        if value.validate(u):
            print('Adding %s entity with %s' % (value.entity_type, u))
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
            if k in LcEntity.signature_fields():
                result_set = [r for r in result_set if bool(re.search(v, r[k], flags=re.IGNORECASE))]
        return result_set

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

        :param args:
        :param kwargs:
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

    def processes(self, **kwargs):
        return self._entities_by_type('process', **kwargs)

    def flows(self, **kwargs):
        return self._entities_by_type('flow', **kwargs)

    def quantities(self, **kwargs):
        return self._entities_by_type('quantity', **kwargs) 


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

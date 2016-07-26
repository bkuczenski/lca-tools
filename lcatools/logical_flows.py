from __future__ import print_function, unicode_literals

from collections import namedtuple
# from lcatools.exchanges import Exchange
import uuid

ExchangeRef = namedtuple('ExchangeRef', ('index', 'exchange'))
CharacterizationRef = namedtuple('FactorRef', ('index', 'characterization'))



class CatalogMismatchError(Exception):
    pass


class TypeMismatchError(Exception):
    pass


class DuplicateEntityError(Exception):
    pass


class GhostEntityError(Exception):
    pass


class LogicalSet(object):
    """
    this is getting pretty semiotic..
    a logical set is a mapping of (many) to (fewer), with merge
    """
    def __init__(self, type_class):
        self._type = type_class
        self._mapping = dict()
        self._set = set()

    def check(self):
        """
        make sure all values and all names are accounted for
        :return: bool
        """
        chk = True
        if not all([v in self._set for v in self._mapping.values()]):
            print('Mapped values missing')
            chk = False
        for v in self._set:
            if not all([self._mapping[n] is v for n in v.names()]):
                print('Names missing %s' % v)
                chk = False
        return chk

    def add(self, item):
        if type(item) != self._type:
            raise TypeMismatchError('item: %s, self-type: %s' % (type(item), self._type))
        for n in item.names():
            if n in self._mapping:
                print('Duplicate entry- specify merge to append')
                raise DuplicateEntityError('%s' % n)
        if item in self._set:
            raise DuplicateEntityError('itself??? %s' % item)
        self._set.add(item)
        self._map_item(item)

    def _map_item(self, item):
        for n in item.names():
            self._mapping[n] = item

    def keys(self):
        return self._mapping.keys()

    def items(self):
        for i in self._set:
            yield i

    def add_ref(self, existing, new):
        ex = self[existing]
        ex.add_ref(new)
        self._map_item(ex)

    def mergewith(self, name, item):
        """
        self.mergewith(name, item): merge item into entry with name. remove item if it is in the set.
        :param name:
        :param item:
        :return:
        """
        entry = self[name]
        entry.merge(item)
        self._map_item(item)
        if item in self._set:
            self._set.remove(item)

    def __getitem__(self, item):
        e = self._mapping[item]
        if e in self._set:
            return e
        raise GhostEntityError

    def serialize(self):
        return [v.serialize() for v in self._set]


class Logical(object):
    """
    a concatenated list of references to the same type of thing
    """

    @classmethod
    def create(cls, ref):
        logical = cls(ref.entity().entity_type, ref.catalog)
        logical.add_ref(ref)
        return logical

    def __init__(self, entity_type, catalog):
        self._uuid = uuid.uuid4()  # need a hashable

        self._catalog = catalog
        self._type = entity_type
        self._entities = []
        self._observations = set()

    def __iter__(self):
        for i in self._entities:
            yield i

    def __getitem__(self, item='Name'):
        for e in iter(self):
            yield e[item]

    def names(self):
        for e in iter(self):
            for n in e.names():
                yield n

    def __eq__(self, other):
        return self._uuid == other._uuid

    def __hash__(self):
        return hash(self._uuid)

    def merge(self, other):
        """
        other becomes deprecated
        :param other:
        :return:
        """
        if self._type != other._type:
            raise TypeMismatchError
        if not (self._catalog is other._catalog):
            raise CatalogMismatchError
        self._entities.extend(other._entities)
        self._observations = self._observations.union(other._observations)

    def add_ref(self, catalog_ref):
        """
        associates a particular entity (referenced via CatalogRef namedtuple) with the logical flow.
        Does not automatically populate the exchange list, as that is cpu-intensive.
        To do so manually, call self.pull_exchanges()
        :param catalog_ref:
        :return: bool - True if ref is new and added; False if ref already exists
        """
        if catalog_ref in self._entities:
            # print('%s' % catalog_ref)
            # raise KeyError('Entity already exists')
            return False
        if catalog_ref.entity_type != self._type:
            raise TypeError('Reference %s is not a %s entity!' % (catalog_ref.entity_type, self._type))
        catalog_ref.validate(self._catalog)
        self._entities.append(catalog_ref)
        return True

    def _add_obs(self, cat_ref, obs):
        """
        cat_ref should be in the ref list
        :param cat_ref:
        :param obs:
        :return:
        """
        if cat_ref in self._entities:
            self._observations.add(obs)

    def serialize(self):
        """
        just a list of catalog refs
        :return:
        """
        return [x.serialize() for x in self._entities]


class LogicalFlow(Logical):
    """
    A LogicalFlow is a notional "indefinite" flow that may correspond to flow instances in several catalogs.
    """
    def flows(self):
        return iter(self)

    def add_exchange(self, cat_ref, exch):
        assert exch.entity_type == 'exchange', 'Not an exchange!'
        assert cat_ref.entity() == exch.flow
        self._add_obs(cat_ref, ExchangeRef(cat_ref.index, exch))

    def exchanges(self):
        for exch in sorted(self._observations, key=lambda x: (x.index, x.exchange.direction)):
            yield exch

    def characterizations(self):
        for flow in self._entities:
            for char in flow.entity().characterizations():
                yield CharacterizationRef(flow.index, char)

    def profile(self):
        print('Exchanges:')
        for e in sorted(self.exchanges(), key=lambda x: x[0].index):
            print('(%s) %s' % (e[0].catalog.name(e[0].index), e[1].f_view()))
        for e in self._entities:
            print('%s :' % e)
            e.entity().profile()
            print('\n')


class LogicalQuantity(Logical):
    """
    A LogicalQuantity is the same thing for catalogs, back-tracking characterizations.
    """
    def quantities(self):
        return iter(self)

    def add_cf(self, cat_ref, cf):
        """
        cat_ref should be in the flow's ref list
        We don't want duplicates of the same observations- though redundant observations are okay [although
        I don't know how we'll deal with them yet]
        But for now, if the cfs are the same
        :param cat_ref:
        :param cf:
        :return:
        """
        assert cf.entity_type == 'characterization', 'Not a cf!'
        assert cat_ref.entity() == cf.quantity
        if cat_ref in self._entities:

            self._add_obs(cat_ref, (CharacterizationRef(cat_ref.index, cf)))

    def cfs(self):
        for cf in self._observations:
            yield cf

    def profile(self):
        for cf in sorted(self.cfs, key=lambda x: x[0].index):
            print('(%s) %s' % (cf[0].catalog.name(cf[0].index), cf[1].q_view()))



import uuid

from .entities import LcEntity
from .flows import LcFlow
from .exchanges import Exchange, ExchangeValue, AllocatedExchange, DuplicateExchangeError

from lcatools.lcia_results import LciaResult, LciaResults


class NoReferenceFound(Exception):
    pass


class MultipleReferencesFound(Exception):
    pass


class LcProcess(LcEntity):

    _ref_field = 'referenceExchange'
    _new_fields = ['SpatialScope', 'TemporalScope']

    @classmethod
    def new(cls, name, ref_exchange, **kwargs):
        """
        :param name: the name of the process
        :param ref_exchange: the reference exchange
        :return:
        """
        return cls(uuid.uuid4(), Name=name, ReferenceExchange=ref_exchange, **kwargs)

    def __init__(self, entity_uuid, **kwargs):
        self._exchanges = set()
        super(LcProcess, self).__init__('process', entity_uuid, **kwargs)
        if self.reference_entity is None:
            self.reference_entity = set()
        else:
            self.reference_entity = {self.reference_entity}

        if 'SpatialScope' not in self._d:
            self._d['SpatialScope'] = 'GLO'
        if 'TemporalScope' not in self._d:
            self._d['TemporalScope'] = '0'

    def __str__(self):
        return '%s [%s]' % (self._d['Name'], self._d['SpatialScope'])

    def add_scenario(self, scenario, reference):
        """
        Here's how scenarios work:
        a scenario acts equivalently to an alternative reference flow in an allocation exchange.
        When a user wants to define a new scenario parameter- first the process needs to know it is parameterized
        within that scenario- and what reference to use for un-parameterized exchanges. that's what add_scenario is for.
        Then, the user can parameterize a specific exchange-> the exchange gets upgraded to an AllocatedExchange
        if it is not already so, the scenario id is used as the reference flow, and the param value is installed
        as the exchange value.
        When a user makes an exchange query, they specify a reference flow (or no reference).  If the reference flow
        shows up in the list of scenarios, then the internal exchange answering mechanism switches gears:
         - if the scenario is a reference in the exchange, it uses that;
         - otherwise it uses the reference pointed to by the process's scenarios dict (set here)

        implication is that an allocated exchange should be earlier in inheritance than an exchange value- but the
        current workaround is satisfactory for ff params and pf params excluding dissipation (dissipation
        would inherit from exchangevalue). LCIA params (as
        previously observed) should / could be new quantities- flow property params in general need a reworking
         of the current [location] spec- which is the same as the param interface- but not today.

         reference: fragment traversal- TODO
        :param scenario:
        :param reference:
        :return:
        """
        if reference in self.reference_entity:
            self._scenarios[scenario] = reference

    def _validate_reference(self, ref_set):
        for x in ref_set:
            if super(LcProcess, self)._validate_reference(x):
                if isinstance(x, AllocatedExchange):
                    x._check_ref()
            else:
                return False
        return True

    def _print_ref_field(self):
        return 'see exchanges'

    def _set_reference(self, ref_entity):
        """
        is it a problem that there's no way to un-set reference exchanges? my feeling is no, at least at present.
        :param ref_entity:
        :return:
        """
        self._validate_reference({ref_entity})

        self.reference_entity.add(ref_entity)

    def find_reference_by_string(self, term, strict=False):
        """
        Select a reference based on a search term--- check against flow.  test get_uuid().startswith, or ['Name'].find()
        If multiple results found- if strict, return None; if strict=False, return first
        :param term:
        :param strict: [False] raise error if ambiguous search term; otherwise return first
        :return: the exchange entity
        """
        hits = [None] * len(self.reference_entity)
        for i, e in enumerate(self.reference_entity):
            if e.flow.get_uuid().startswith(term):
                hits[i] = e
            elif e.flow['Name'].find(term) >= 0:
                hits[i] = e
        hits = list(filter(None, hits))
        if strict:
            if len(hits) > 1:
                raise MultipleReferencesFound('process:%s key: %s' % (self, term))
        if len(hits) == 0:
            raise NoReferenceFound('process:%s key: %s' % (self, term))
        return hits[0]

    def inventory(self, reference=None):
        print('%s' % self)
        out = []
        if reference is None:
            it = self.exchanges()
        else:
            it = self.allocated_exchanges(reference)
        for i in it:
            print('%2d %s' % (len(out), i))
            out.append(i)
        return out

    def exchange(self, flow):
        if isinstance(flow, LcFlow):
            for x in self._exchanges:
                if x.flow == flow:
                    yield x
        elif flow in self._scenarios:
            for x in self._exchanges:
                if x.flow == self._scenarios[flow]:
                    yield x
        else:
            raise TypeError('LcProcess.exchange input %s %s' % (flow, type(flow)))

    def exchanges(self):
        for i in sorted(self._exchanges, key=lambda x: x.direction):
            yield i

    def find_reference(self, reference, strict=False):
        if reference is None:
            if len(self.reference_entity) > 1:
                raise NoReferenceFound('Must specify reference!')
            ref = list(self.reference_entity)[0].flow
        elif isinstance(reference, str):
            x = self.find_reference_by_string(reference, strict=strict)
            ref = x.flow
        elif isinstance(reference, LcFlow):
            ref = reference
        elif isinstance(reference, Exchange):
            ref = reference.flow
        else:
            raise NoReferenceFound('Unintelligible reference %s' % reference)
        return ref

    def allocated_exchanges(self, reference, strict=False):
        # need to disambiguate the reference
        in_scenario = False
        if isinstance(reference, LcFlow):
            ref = self.find_reference(reference, strict=strict)
        elif reference in self._scenarios.keys():
            in_scenario = True
            ref = self._scenarios[reference]
        else:
            try:
                ref = self.find_reference(reference, strict=strict)
            except NoReferenceFound:
                pass  # will fail if any exchanges are allocated

        for i in sorted(self._exchanges, key=lambda t: t.direction):
            if isinstance(i, AllocatedExchange):
                if in_scenario:
                    yield ExchangeValue.from_scenario(i, reference, ref)
                else:
                    yield ExchangeValue.from_allocated(i, ref.get_uuid())
            else:
                yield i

    def add_reference(self, flow, dirn):
        rx = Exchange(self, flow, dirn)
        self._set_reference(rx)
        return rx

    def add_exchange(self, flow, dirn, reference=None, value=None, add_dups=False, **kwargs):
        """
        This is used to create Exchanges and ExchangeValues and AllocatedExchanges.

        If the flow+dirn is already in the exchange set:
            if no reference is specified and/or no value is specified- nothing to do
            otherwise (if reference and value are specified):
                upgrade the exchange to an allocatedExchange and add the new reference exch val
        otherwise:
            if reference is specified, create an AllocatedExchange
            otherwise create an Exchange / ExchangeValue

        :param flow:
        :param dirn:
        :param reference:
        :param value:
        :param add_dups: (False) set to true to handle "duplicate exchange" errors by cumulating their values
        :return:
        """
        _x = Exchange(self, flow, dirn, **kwargs)
        if _x in self._exchanges:
            if value is None or value == 0:
                return None
            e = [x for x in self._exchanges if x == _x][0]
            if reference is None:
                if isinstance(e, AllocatedExchange):
                    try:
                        e.value = value  # this will catch already-set errors
                    except DuplicateExchangeError:
                        if add_dups:
                            e.add_to_value(value)
                        else:
                            print('Duplicate exchange in process %s:\n%s' % (self.get_uuid(), e))
                            raise
                    return e
                else:
                    try:
                        exch = ExchangeValue.from_exchange(e, value=value)  # this will catch already-set errors
                        self._exchanges.remove(e)
                        self._exchanges.add(exch)
                        return exch
                    except DuplicateExchangeError:
                        if add_dups:
                            e.add_to_value(value)
                            return e
                        else:
                            print('Duplicate exchange in process %s:\n%s' % (self.get_uuid(), e))
                            raise

            else:
                exch = AllocatedExchange.from_exchange(e)
                if isinstance(value, dict):
                    exch.update(value)
                else:
                    try:
                        exch[reference] = value  # this will catch already-set errors
                    except DuplicateExchangeError:
                        if add_dups:
                            exch.add_to_value(value, reference=reference)
                        else:
                            print('Duplicate exchange in process %s:\n%s' % (self.get_uuid(), e))
                            raise

                self._exchanges.remove(e)
                self._exchanges.add(exch)
                return exch

        else:
            if value is None or value == 0:
                e = _x
            elif isinstance(value, float):
                if reference is None:
                    e = ExchangeValue(self, flow, dirn, value=value, **kwargs)
                else:
                    if reference not in self.reference_entity:
                        raise KeyError('Specified reference is not registered with process: %s' % reference)
                    e = AllocatedExchange(self, flow, dirn, value=value, **kwargs)
                    e[reference] = value

            elif isinstance(value, dict):
                e = AllocatedExchange.from_dict(self, flow, dirn, value=value, **kwargs)
            else:
                raise TypeError('Unhandled value type %s' % type(value))
            if e in self._exchanges:
                raise KeyError('Exchange already present')
            self._exchanges.add(e)
            return e

    def lcias(self, quantities, **kwargs):
        results = LciaResults(entity=self)
        for q in quantities:
            results[q.get_uuid()] = self.lcia(q, **kwargs)
        return results

    def lcia(self, quantity, ref_flow=None, scenario=None):
        result = LciaResult(quantity, scenario)
        result.add_component(self.get_uuid(), entity=self)
        for ex in self.allocated_exchanges(scenario or ref_flow):
            factor = ex.flow.factor(quantity)
            result.add_score(self.get_uuid(), ex, factor, self['SpatialScope'])
        return result

    def merge(self, other):
        raise NotImplemented('This should be done via fragment construction + aggregation')

    def serialize(self, exchanges=False, **kwargs):
        j = super(LcProcess, self).serialize()
        j.pop(self._ref_field)  # reference reported in exchanges
        if exchanges:
            # if exchanges is true, report all exchanges
            j['exchanges'] = sorted([x.serialize(**kwargs) for x in self._exchanges],
                                    key=lambda x: (x['direction'], x['flow']))
        else:
            # if exchanges is false, only report reference exchanges
            j['exchanges'] = sorted([x.serialize(**kwargs) for x in self._exchanges
                                     if x in self.reference_entity],
                                    key=lambda x: (x['direction'], x['flow']))
        return j

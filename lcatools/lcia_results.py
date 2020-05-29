"""
This object replaces the LciaResult types spelled out in Antelope-- instead, it serializes to an LCIA result directly.

"""
from lcatools.interfaces import comp_dir
from lcatools.exchanges import ExchangeValue, DissipationExchange
from lcatools.autorange import AutoRange
from numbers import Number
from math import isclose
# from lcatools.interfaces import to_uuid


from collections import defaultdict


'''
def get_entity_uuid(item):
    if to_uuid(item) is not None:
        return item
    if hasattr(item, 'get_uuid'):
        return item.get_uuid()
    raise TypeError('Don\'t know how to get ID from %s' % type(item))
'''


class InconsistentQuantity(Exception):
    pass


class InconsistentScenario(Exception):
    pass


class DuplicateResult(Exception):
    pass


class InconsistentScores(Exception):
    pass


class InconsistentSummaries(Exception):
    pass


def number(val):
    try:
        return '%10.3g' % val
    except TypeError:
        return '%10.10s' % '----'


class DetailedLciaResult(object):
    """
    Contains exchange, factor, result
    """
    def __init__(self, lc_result, exchange, qrresult):
        """

        :param lc_result:
        :param exchange:
        :param qrresult: meets the QRResult spec: has properties 'flowable', 'ref', 'query', 'context', 'locale',
        'origin', 'value'
        """
        if exchange.flow.unit != qrresult.ref.unit and qrresult.value != 0.0:
            print('%s: Inconsistent qty\nexch: %s\nqrr:  %s' % (self.__class__.__name__, exchange.flow.reference_entity, qrresult))
            #  raise InconsistentQuantity('%s\n%s' % (exchange.flow.reference_entity, qrresult))
        self._exchange = exchange
        self._qr = qrresult
        self._lc = lc_result

    @property
    def exchange(self):
        return self._exchange

    @property
    def factor(self):
        return self._qr

    @property
    def _dirn_adjust(self):
        if self._qr.context.sense is None:
            return 1.0
        elif comp_dir(self._qr.context.sense) == self._exchange.direction:
            return 1.0
        return -1.0

    @property
    def is_null(self):
        return self.result == 0

    @property
    def flow(self):
        return self._exchange.flow

    @property
    def direction(self):
        return self._exchange.direction

    @property
    def flowable(self):
        return str(self._qr.flowable)

    @property
    def context(self):
        return str(self._qr.context)

    @property
    def content(self):
        if isinstance(self._exchange, DissipationExchange):
            return self._exchange.content()
        return None

    @property
    def value(self):
        if self._exchange.value is None:
            return 0.0
        return self._exchange.value * self._lc.scale

    @property
    def result(self):
        if self._qr.value is None:
            return 0.0
        return self.value * self._dirn_adjust * self._qr.value

    def __hash__(self):
        return hash((self._exchange.process.external_ref, self._exchange.direction, self._qr.flowable, self._qr.context))

    def __eq__(self, other):
        if not isinstance(other, DetailedLciaResult):
            return False
        return (self.exchange.process.external_ref == other.exchange.process.external_ref and
                self.flowable == other.flowable and
                self.direction[0] == other.direction[0] and
                self.context == other.context)

    def __str__(self):
        if self._dirn_adjust == -1:
            dirn_mod = '*'
        else:
            dirn_mod = ' '
        return '%s%s = %-s  x %-s [%s] %s, %s' % (dirn_mod,
                                                  number(self.result * self._lc.autorange),
                                                  number(self._qr.value * self._lc.autorange),
                                                  number(self.value),
                                                  self._qr.locale,
                                                  self.flowable,
                                                  self.context)


class SummaryLciaResult(object):
    """
    like a DetailedLciaResult except omitting the exchange and factor information.  This makes them totally static.
    """
    def __init__(self, lc_result, entity, node_weight, unit_score):
        """
        :param lc_result: who "owns" you. scale report by their scale.
        entity_id must either have get_uuid() or be hashable
        :param entity: a hashable identifier
        :param node_weight: stand-in for exchange value
        :param unit_score: stand-in for factor value
        """
        self.entity = entity
        self._node_weight = node_weight
        if isinstance(unit_score, Number):
            self._static_value = unit_score
            self._internal_result = None
        else:
            self._static_value = None
            self._internal_result = unit_score

        self._lc = lc_result

    def update_parent(self, lc):
        self._lc = lc

    @property
    def static(self):
        return self._static_value is not None

    @property
    def is_null(self):
        if self.static:
            if self._static_value == 0:
                return True
            return False
        return self._internal_result.is_null

    @property
    def node_weight(self):
        return self._node_weight * self._lc.scale

    @property
    def unit_score(self):
        if self.static:
            return self._static_value
        else:
            return self._internal_result.total()

    @property
    def cumulative_result(self):
        return self.node_weight * self.unit_score

    def components(self):
        if self.static:
            yield self
        else:
            for x in self._internal_result.components():
                yield x

    def __hash__(self):
        return hash(self.entity)

    def __eq__(self, other):
        if not isinstance(other, SummaryLciaResult):
            return False
        return self.entity == other.entity

    def __str__(self):
        return '%s = %-s x %-s %s' % (number(self.cumulative_result * self._lc.autorange), number(self.node_weight),
                                      number(self.unit_score * self._lc.autorange),
                                      self.entity)

    def show(self):
        if self.static:
            print('%s' % self)
        else:
            self._internal_result.show()

    def details(self):
        if self.static:
            yield self
        else:
            for k in self._internal_result.keys():
                yield self._internal_result[k]

    def show_detailed_result(self):
        if self.static:
            self.show()
        else:
            self._internal_result.show_components()

    def flatten(self):
        if self.static:
            return self
        return self._internal_result.flatten(_apply_scale=self.node_weight)

    def __add__(self, other):
        """
        Add two summary LCIA results together.  This only works under the following circumstances:
         * different instances of the same entity are being added (e.g. two instances of the same flow).
           In this case, the two summaries' entities must compare as equal and their unit scores must be equal.
           The node weights are added.  Scale is ignored (scale is inherited from the primary summary)

         * Two static-valued summaries are added together.  In this case, either the scores must be equal (in which case
           the node weights are summed) or the node weights must be equal, and the unit scores are summed.

        This is the sort of garbage that should be unittested.

        :param other:
        :return:
        """
        if not isinstance(other, SummaryLciaResult):
            raise TypeError('Can only add SummaryLciaResults together')
        if self._lc is not other._lc:
            raise InconsistentSummaries('These summaries do not belong to the same LciaResult')
        if self.static:
            if other.static:
                # either the node weights or the unit scores must be equal
                if self.unit_score == other.unit_score:  # make node weights add preferentially
                    unit_score = self.unit_score
                    _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
                elif self.node_weight == other.node_weight:
                    _node_weight = self._node_weight
                    unit_score = self._static_value + other.unit_score
                else:
                    raise InconsistentScores('These summaries do not add together:\n%s\n%s' % (self, other))
            else:
                if self.unit_score == other.unit_score:
                    unit_score = other._internal_result
                    _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
                else:
                    raise InconsistentScores('These summaries have different unit scores')
        elif self.unit_score == other.unit_score:
            unit_score = self._internal_result
            if other.static:
                _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
            elif self._internal_result is other._internal_result:
                # this only works because terminations cache unit scores
                # just sum the node weights, ignoring our local scaling factor (DWR!)
                if self.entity == other.entity:
                    # WARNING: FragmentFlow equality does not include magnitude or node weight
                    _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
                else:
                    print("entities do not match\n self: %s\nother: %s" % (self.entity, other.entity))
                    raise InconsistentSummaries
            else:
                """
                This situation is cropping up in the CalRecycle model but it appears to be kosher. I propose the 
                following test: if the two summaries are both nonstatic and (a) have the same set of components and (b) 
                have the same unit scores, then treat them as the same.
                """
                if set(k.entity for k in self.components()) == set(k.entity for k in other.components()):
                    _node_weight = self._node_weight + (other.node_weight / self._lc.scale)
                else:
                    raise InconsistentSummaries('Components differ between non-static summaries')
        else:
            print('\n%s' % self)
            print(other)
            raise InconsistentSummaries('At least one not static, and unit scores do not match')
        return SummaryLciaResult(self._lc, self.entity, _node_weight, unit_score)


class AggregateLciaScore(object):
    """
    contains an entityId which should be either a process or a fragment (fragment stages show up as fragments??)
    The Aggregate score is constructed either from individual LCIA Details (exchange value x characterization factor)
    or from summary results
    """
    static = True

    def __init__(self, lc_result, entity):
        self.entity = entity
        self._lc = lc_result
        self.LciaDetails = []  # what exactly was having unique membership protecting us from??

    def update_parent(self, lc_result):
        self._lc = lc_result

    @property
    def cumulative_result(self):
        if len(self.LciaDetails) == 0:
            return 0.0
        return sum([i.result for i in self.LciaDetails])

    @property
    def is_null(self):
        for i in self.LciaDetails:
            if not i.is_null:
                return False
        return True

    def _augment_entity_contents(self, other):
        """
        when duplicate fragmentflows are aggregated, their node weights and magnitudes should be added together
        :return:
        """
        self.entity = self.entity + other

    def add_detailed_result(self, exchange, qrresult):
        d = DetailedLciaResult(self._lc, exchange, qrresult)
        '''
        if d in self.LciaDetails:  # process.uuid, direction, flow.uuid are the same
            if factor[location] != 0:
                other = next(k for k in self.LciaDetails if k == d)
                raise DuplicateResult('exchange: %s\n  factor: %s\nlocation: %s\nconflicts with %s' %
                                      (exchange, factor, location, other.factor))
            else:
                # do nothing
                return
        '''
        self.LciaDetails.append(d)

    def show(self, **kwargs):
        self.show_detailed_result(**kwargs)

    def details(self):
        """
        generator of nonzero detailed results
        :return:
        """
        for d in self.LciaDetails:
            if d.result != 0:
                yield d

    def show_detailed_result(self, key=lambda x: x.result, show_all=False):
        for d in sorted(self.LciaDetails, key=key, reverse=True):
            if d.result != 0 or show_all:
                print('%s' % d)
        # print('=' * 60)
        # print('             Total score: %g ' % self.cumulative_result)

    def __str__(self):
        return '%s  %s' % (number(self.cumulative_result * self._lc.autorange), self.entity)


def show_lcia(lcia_results):
    """
    Takes in a dict of uuids to lcia results, and summarizes them in a neat table
    :param lcia_results:
    :return:
    """
    print('LCIA Results\n%s' % ('-' * 60))
    for r in lcia_results.values():
        print('%10.5g %s' % (r.total(), r.quantity))


class LciaResult(object):
    """
    An LCIA result object contains a collection of LCIA results for a related set of entities, called components.  Each
     component is an AggregateLciaScore, which itself is a collection of either detailed LCIA results or summary scores.

    Each component which is a FragmentFlow represents a specific traversal scenario and is thus static.

    Each component which is a process will contain actual exchanges and factors, which are scenario-sensitive, and
     so is (theoretically) dynamic. This is not yet useful in practice.  LCIA Results are in sharp need of testing /
     refactoring.
    """
    '''# Let's leave this out until we hahve a clear use case
    @classmethod
    def from_cfs(cls, fragment, cfs, scenario=None, location=None):
        """
        for foreground-terminated elementary flows
        returns a dict of LciaResults
        :param fragment:
        :param cfs: a dict of q-uuid to cf sets???
        :param scenario:
        :param location:
        :return:
        """
        results = LciaResults(fragment)
        exch = ExchangeValue(fragment, fragment.flow, fragment.direction, value=1.0)
        for q, cf in cfs.items():
            qu = q.get_uuid()
            results[qu] = cls(q, scenario=scenario)
            if isinstance(cf, Characterization):
                results[qu].add_component(fragment.get_uuid(), entity=fragment)
                results[qu].add_score(fragment.get_uuid(), exch, cf)

        return results
    '''

    def __init__(self, quantity, scenario=None, private=False, scale=1.0):
        """
        If private, the LciaResult will not return any unaggregated results
        :param quantity:
        :param scenario:
        :param private:
        """
        self.quantity = quantity
        self.scenario = scenario
        self._scale = scale
        self._LciaScores = dict()
        self._cutoffs = []
        self._errors = []
        self._zeros = []

        self._private = private
        self._autorange = None
        self._failed = []

    @property
    def is_null(self):
        for i in self._LciaScores.values():
            if not i.is_null:
                return False
        return True

    def set_autorange(self, value=True):
        """
        Update the AutoRange object. Should be done before results are presented, if auto-ranging is in use.

        Auto-ranging affects the following outputs:
         * any show() or printed string
         * the results of contrib_new()
        No other outputs are affected.
        :param value:
        :return:
        """
        assert isinstance(value, bool), 'cannot set autorange to %s of type %s' % (value, type(value))
        if value:
            self._autorange = AutoRange(self.range())
        else:
            self._autorange = None

    def unset_autorange(self):
        self.set_autorange(False)

    @property
    def autorange(self):
        if self._autorange is None:
            return 1.0
        else:
            return self._autorange.scale

    @property
    def unit(self):
        if self._autorange is not None:
            return self._autorange.adj_unit(self.quantity.unit)
        else:
            return self.quantity.unit

    @property
    def scale(self):
        return self._scale

    def scale_result(self, scale):
        """
        why is this a function?
        """
        self._scale *= scale

    def _match_key(self, item):
        """
        whaaaaaaaaaat is going on here
        :param item:
        :return:
        """
        if item in self._LciaScores:
            yield self._LciaScores[item]
        else:
            for k, v in self._LciaScores.items():
                if item == v.entity:
                    yield v
                # elif str(v.entity).startswith(str(item)):
                #     yield v
                elif str(k).startswith(str(item)):
                    yield v

    def __getitem__(self, item):
        try:
            return next(self._match_key(item))
        except StopIteration:
            raise KeyError('%s' % item)

    '''
    def __getitem__(self, item):
        if isinstance(item, int):
            return
    '''

    def aggregate(self, key=lambda x: x.fragment['StageName'], entity_id=None):
        """
        returns a new LciaResult object in which the components of the original LciaResult object are aggregated into
        static values according to a key.  The key is a lambda expression that is applied to each AggregateLciaScore
        component's entity property (components where the lambda fails will all be grouped together).

        The special key '*' will aggregate all components together.  'entity_id' argument is required in this case to
        provide a distinguishing key for the result (falls back to "aggregated result").

        :param key: default: lambda x: x.fragment['StageName'] -- assuming the payload is a FragmentFlow
        :param entity_id: a descriptive string for the entity, to allow the aggregation to be distinguished in
         subsequent aggregations.  Use 'None' at your peril
        :return:
        """
        agg_result = LciaResult(self.quantity, scenario=self.scenario, private=self._private, scale=self._scale)
        if key == '*':
            if entity_id is None:
                entity_id = 'aggregated result'
            agg_result.add_summary(entity_id, entity_id, 1.0, self.total())
        else:
            for v in self._LciaScores.values():
                keystring = 'other'
                try:
                    keystring = key(v.entity)
                finally:
                    # use keystring AS entity
                    agg_result.add_summary(keystring, keystring, 1.0, v.cumulative_result)
        return agg_result

    def show_agg(self, **kwargs):
        self.aggregate(**kwargs).show_components()  # deliberately don't return anything- or should return grouped?

    def flatten(self, _apply_scale=1.0):
        """
        Return a new LciaResult in which all groupings have been replaced by a set of AggregatedLciaScores, one
         per elementary flow.
        Performs some inline testing via equality assertions, but this still deserves unit testing
        :param: _apply_scale: [1.0] apply a node weighting to the components
        :return:
        """
        flat = LciaResult(self.quantity, scenario=self.scenario, private=self._private, scale=1.0)
        recurse = []  # store flattened summary scores to handle later
        for k, c in self._LciaScores.items():
            if isinstance(c, SummaryLciaResult):
                if c.static:
                    flat.add_summary(k, c.entity, c.node_weight * _apply_scale, c.unit_score)
                else:
                    recurse.append(c.flatten())
            else:
                for d in c.details():
                    flat.add_component(d.flow.external_ref, d.flow)
                    # create a new exchange that has already had scaling applied
                    exch = ExchangeValue(d.exchange.process, d.flow, d.exchange.direction, value=d.value * _apply_scale)
                    flat.add_score(d.flow.external_ref, exch, d.factor)

        for r in recurse:
            for k in r.keys():
                c = r[k]
                if isinstance(c, SummaryLciaResult):
                    # guaranteed to be static since r is a flattened LciaResult
                    if not c.static:
                        raise InconsistentSummaries(c)
                    try:
                        flat.add_summary(k, c.entity, c.node_weight * _apply_scale, c.unit_score)
                    except InconsistentScores:
                        print('for key %s' % k)
                        raise
                else:
                    for d in c.details():
                        flat.add_component(d.flow.external_ref, d.flow)
                        exch = ExchangeValue(d.exchange.process, d.flow, d.exchange.direction,
                                             value=d.value * _apply_scale)
                        flat.add_score(k, exch, d.factor)

        scaled_total = self.total() * _apply_scale
        if not isclose(scaled_total, flat.total(), rel_tol=1e-10):
            print(' LciaResult: %10.4g' % scaled_total)
            print('Flat result: %10.4g' % flat.total())
            print('Difference: %10.4g @ %10.4g' % (flat.total() - scaled_total, _apply_scale))
            if not isclose(scaled_total, flat.total(), rel_tol=1e-6):
                raise ValueError('Total differs by greater than 1e-6! (applied scaling=%10.4g)' % _apply_scale)
        return flat

    @property
    def is_private(self):
        return self._private

    def total(self):
        return sum([i.cumulative_result for i in self._LciaScores.values()])

    def range(self):
        return sum([abs(i.cumulative_result) for i in self._LciaScores.values()])

    def add_component(self, key, entity=None):
        if entity is None:
            entity = key
        if key not in self._LciaScores.keys():
            self._LciaScores[key] = AggregateLciaScore(self, entity)

    def add_score(self, key, exchange, qrresult):
        if qrresult.query != self.quantity:
            raise InconsistentQuantity('%s\nqrresult.quantity: %s\nself.quantity: %s' % (qrresult,
                                                                                         qrresult.query,
                                                                                         self.quantity))
        if key not in self._LciaScores.keys():
            self.add_component(key)
        self._LciaScores[key].add_detailed_result(exchange, qrresult)

    def add_summary(self, key, entity, node_weight, unit_score):
        if key in self._LciaScores.keys():
            # raise DuplicateResult('Key %s is already present' % key)
            '''
            tgt = self._LciaScores[key]
            if isinstance(unit_score, LciaResult):
                uss = unit_score.total()
            else:
                uss = unit_score
            print('Key %s [%s] (%10.4g x %10.4g) adding %s (%10.4g x %10.4g)' % (key,
                                                                                 tgt.entity,
                                                                                 tgt.node_weight, tgt.unit_score,
                                                                                 entity,
                                                                                 node_weight, uss))
            '''
            try:
                self._LciaScores[key] += SummaryLciaResult(self, entity, node_weight, unit_score)
            except InconsistentSummaries:
                self._failed.append(SummaryLciaResult(self, entity, node_weight, unit_score))
        else:
            self._LciaScores[key] = SummaryLciaResult(self, entity, node_weight, unit_score)

    @property
    def failed_summaries(self):
        """
        A list of Summary results that failed to be added to an existing summary. This is mainly diagnostic and should
        be removed soon.
        Note the difiference from self.errors(), which is meant to store input exchanges that could not be converted
        to the query quantity during LCIA.
        :return:
        """
        return self._failed

    def add_cutoff(self, exchange):
        self._cutoffs.append(exchange)

    def cutoffs(self):
        """
        Generates exchanges for which no factor was found during LCIA.
        :return:
        """
        for x in self._cutoffs:
            yield x

    def add_error(self, x, qr):
        self._errors.append(DetailedLciaResult(self, x, qr))

    def errors(self):
        """
        generates exchanges that could not be converted to the target quantity due to a conversion error.
        Note the difference from self.failed_summaries, which reports summary scores that could not be added.
        :return:
        """
        for x in self._errors:
            yield x

    def add_zero(self, x, qr):
        self._zeros.append(DetailedLciaResult(self, x, qr))

    def zeros(self):
        for x in self._zeros:
            yield x

    def details(self):
        for c in self.components():
            for d in c.details():
                yield d

    def keys(self):
        if self._private:
            for k in ():
                yield k
        else:
            for k in self._LciaScores.keys():
                yield k

    def components(self):
        if not self._private:
            for v in self._LciaScores.values():
                yield v

    def component_entities(self):
        if self._private:
            return [None]
        return [k.entity for k in self._LciaScores.values()]

    def _header(self):
        print('%s %s' % (self.quantity, self.unit))
        if self._autorange:
            self.set_autorange()  # update AutoRange object
            print('Auto-ranging: x %g' % self.autorange)
        print('-' * 60)
        if self._scale != 1.0:
            print('%10.4gx %s' % (self._scale, 'scale'))

    def show(self):
        self._header()
        print('%s' % self)

    def show_components(self):
        self._header()
        if not self._private:
            for v in sorted(self._LciaScores.values(), key=lambda x: x.cumulative_result, reverse=True):
                print('%s' % v)
            print('==========')
        print('%s' % self)

    def show_details(self, key=None, **kwargs):
        """
        Sorting by parts is not ideal but it will have to do.
        :param key:
        :param kwargs:
        :return:
        """
        self._header()
        if not self._private:
            if key is None:
                for e in sorted(self._LciaScores.keys(),
                                key=lambda x: self._LciaScores[x].cumulative_result,
                                reverse=True):
                    try:
                        print('\n%s:' % self._LciaScores[e].entity)
                    except TypeError:
                        print('\n%s:' % str(self._LciaScores[e].entity))
                    self._LciaScores[e].show_detailed_result(**kwargs)
            else:
                self._LciaScores[key].show_detailed_result(**kwargs)
        print('%s' % self)

    def __add__(self, other):
        if self.quantity != other.quantity:
            raise InconsistentQuantity
        if self.scenario != other.scenario:
            raise InconsistentScenario
        s = LciaResult(self.quantity, self.scenario)
        for k, v in self._LciaScores.items():
            s._LciaScores[k] = v
            v.update_parent(s)
        for k, v in other._LciaScores.items():
            if k in s._LciaScores:
                if v.entity is s._LciaScores[k].entity:
                    s._LciaScores[k] += v  # this is not implemented yet
                else:
                    s._LciaScores['_%s' % k] = v
            else:
                s._LciaScores[k] = v
        return s

    def __str__(self):
        return '%s %s' % (number(self.total()), self.quantity)

    # charts
    def contrib_query(self, stages=None):
        """
        returns a list of scores
        :param stages: [None] a list of stages to query, or None to return all components.
         Specify '*' to return a 1-item list containing just the total.

        :return:
        """
        if stages == '*':
            return [self.total()]
        elif stages is None:
            stages = self.component_entities()

        data = []
        for c in stages:
            try:
                data.append(self._LciaScores[c].cumulative_result)
            except KeyError:
                data.append(0)
        if not isclose(sum(data), self.total(), rel_tol=1e-6):
            print('Contributions do not equal total [ratio: %.10f]' % (sum(data) / self.total()))
        return data

    def contrib_new(self, *args, autorange=None):
        """
        re-implement contrib query with a better spec.

        Queries are specified as entries from self.keys(). One way to get the keys to be more legible is to first
        perform an aggregation using self.aggregate().

        The current __getitem__ method, which uses a fuzzy match (self._match_keys()) is not currently used.

        :param args: A sequential list of components to query.  The special component '*' can be used to select the
        balance of results.
        :param autorange: [None] do not alter autorange settings.  [True / False]: activate or deactivate auto-ranging.
        :return: a 2-tuple: results, balance where results is a list having the same length as the number of arguments,
         and balance is a float reporting the remainder.  sum(results, balance) == self.total().  If '*' is specified as
         one of the queries, balance will always be 0.
        """
        if autorange is not None:
            self.set_autorange(autorange)
        elif self._autorange is not None:
                self.set_autorange()

        bal_idx = None
        results = []
        for i, query in enumerate(args):
            if query == '*':
                bal_idx = i  # save for later
                results.append(0.0)
            else:
                try:
                    results.append(self._LciaScores[query].cumulative_result * self.autorange)
                except KeyError:
                    results.append(0.0)

        balance = self.total() * self.autorange - sum(results)

        if bal_idx is not None:
            results[bal_idx] = balance
            return results, 0.0
        else:
            return results, balance


class LciaResults(dict):
    """
    A dict of LciaResult objects, with some useful attachments.  The dict gets added to in the normal way, but
    also keeps track of the keys added in sequence, so that they can be retrieved by numerical index.

    The LciaResults object keys should be quantity links


    """
    def __init__(self, entity, *args, **kwargs):
        super(LciaResults, self).__init__(*args, **kwargs)
        self.entity = entity
        self._scale = 1.0
        self._indices = []

    def __getitem__(self, item):
        """
        __getitem__ can either be used as a numerical index
        :param item:
        :return:
        """
        try:
            return super(LciaResults, self).__getitem__(item)
        except KeyError:
            try:
                int(item)
                return super(LciaResults, self).__getitem__(self._indices[item])
            except (ValueError, TypeError):
                try:
                    return super(LciaResults, self).__getitem__(next(k for k in self.keys() if k.startswith(item)))
                except StopIteration:
                    return LciaResult(None)  #

    def __setitem__(self, key, value):
        assert isinstance(value, LciaResult)
        value.scale_result(self._scale)
        super(LciaResults, self).__setitem__(key, value)
        if key not in self._indices:
            self._indices.append(key)

    def add(self, value):
        # TODO: add should cumulate components if LciaResult is already present
        assert isinstance(value, LciaResult)
        self.__setitem__(value.quantity.link, value)

    def indices(self):
        for i in self._indices:
            yield i

    def to_list(self):
        return [self.__getitem__(k) for k in self._indices]

    def scale(self, factor):
        if factor == self._scale:
            return
        self._scale = factor
        for k in self._indices:
            self[k].scale_result(factor)

    def show(self):
        print('LCIA Results\n%s\n%s' % (self.entity, '-' * 60))
        if self._scale != 1.0:
            print('%60s: %10.4g' % ('scale', self._scale))
        for i, q in enumerate(self._indices):
            r = self[q]
            r.scale_result(self._scale)
            print('[%2d] %.3s  %10.5g %s' % (i, q, r.total(), r.quantity))

    def apply_weighting(self, weights, quantity, **kwargs):
        """
        Create a new LciaResult object containing the weighted sum of entries in the current object.

        We want the resulting LciaResult to still be aggregatable. In order to accomplish this, we need to maintain
        all the individual _LciaScores entities in the weighting inputs, and compute their weighted scores here. Then
        we need to log the weighted scores as the *node weights* and use *unit* values of unit scores, because
        SummaryLciaResults are only allowed to be further aggregated if they have the same unit score.

        This feels a bit hacky and may turn out to be a terrible idea.  But there is a certain harmony in making the
        quantity's unit THE unit for a weighting computation. So I think it will work for now.

        :param weights: a dict mapping quantity UUIDs to numerical weights
        :param quantity: EITHER an LcQuantity OR a string to use as the name of in LcQuantity.new()
        :param kwargs: passed to LciaResult
        :return:
        """
        weighted_result = LciaResult(quantity, **kwargs)

        component_list = dict()  # dict maps keys entities
        component_score = defaultdict(float)  # maps keys to weighted scores
        for method, weight in weights.items():
            try:
                result = self.__getitem__(method)  # an LciaResult
            except KeyError:
                continue
            for comp in result.keys():
                if comp in component_list.keys():
                    if result[comp].entity != component_list[comp]:
                        raise DuplicateResult('Key %s matches different entities:\n%s\n%s' % (comp,
                                                                                              result[comp],
                                                                                              component_list[comp]))
                else:
                    component_list[comp] = result[comp].entity
                component_score[comp] += (weight * result[comp].cumulative_result)

        for comp, ent in component_list.items():
            weighted_result.add_component(comp, entity=ent)
            weighted_result.add_summary(comp, ent, component_score[comp], 1.0)

        return weighted_result

    def clear(self):
        super(LciaResults, self).clear()
        self._indices = []

    def update(self, *args, **kwargs):
        super(LciaResults, self).update(*args, **kwargs)
        self._indices = list(self.keys())


class LciaWeighting(object):
    def __init__(self, quantity, weighting):
        """

        :param quantity: a new LcQuantity to represent the weighting
        :param weighting: a weighting dict as defined in apply_weighting
        """
        self._q = quantity
        self._w = weighting

    def weigh(self, res, **kwargs):
        return res.apply_weighting(self._w, self._q, **kwargs)

    def q(self):
        return self._q.link


def traversal_to_lcia(ffs):
    """
    This function takes in a list of fragment flow records and aggregates their ScoreCaches into a set of LciaResults.
    The function is surprisingly slow, because AggregateLciaScore objects contain sets, so there is a lot of container
    checking. (I think that's why, anyway...)
    :param ffs:
    :return: dict of quantity uuid to LciaResult -> suitable for storing directly into a new term scorecache
    """
    results = LciaResults(ffs[0].fragment)
    for i in ffs:
        if not i.term.is_null:
            for q, v in i.term.score_cache_items():
                node_weight = i.node_weight
                quantity = v.quantity

                if q not in results.keys():
                    results[q] = LciaResult(quantity, scenario=v.scenario)

                if v.total() * node_weight == 0:
                    continue

                if i.term.direction == i.fragment.direction:
                    # if the directions collide (rather than complement), the term is getting run in reverse
                    node_weight *= -1

                results[q].add_summary(i.fragment.uuid, i, node_weight, v)
    return results

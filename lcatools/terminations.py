"""
Flow Terminations are model-defined links between a particular flow and a process that terminates it.

They originated as part of LcFragments but in fact they are more general. A FlowTermination is actually the same
as a ProductFlow in lca-matrix, although the FlowTermination is more powerful.  It should be easy to construct
either one from the other.
"""

from lcatools.exchanges import comp_dir, ExchangeValue, NoReferenceFound
from lcatools.interfaces import InventoryRequired, PrivateArchive
from lcatools.lcia_results import LciaResult, LciaResults


# from lcatools.catalog_ref import NoCatalog
# from lcatools.interact import parse_math


class FlowConversionError(Exception):
    pass


class SubFragmentAggregation(Exception):
    pass


class NonConfigurableInboundEV(Exception):
    """
    only foreground terminations may have their inbound exchange values explicitly specified
    """
    pass


class FlowTermination(object):
    """
    these are stored by scenario in a dict on the mainland

    A fragment can have the following types of terminations:
     * None - the termination is null- the flow enters the foreground and becomes an i/o
     * parent - the fragment's termination is the fragment itself.  The fragment flow  enters a foreground node.
       The node can have children but only has LCIA impacts based on the terminating flow, which have to be looked up
       in the database. fg-terminated nodes don't have scenarios (e.g. the scenarios are in the exchange values).
       Note: term_flows can be different from parent flows, and unit conversions will occur normally (e.g. "sulfur
       content" converted to "kg SO2")
     * Process - the flow enters a process referenced by CatalogRef.  The node's LCIA impacts are fg_lcia. The
       node's children are the process's non-term intermediate exchanges. The node can also have other children.
       (created with terminate or term_from_exch)
     * Fragment - the flow enters a sub-fragment.  The sub-fragment must be traversable. The node's children are
       the fragment's non-term io flows. The node cannot have other children.  If the sub-fragment is background,
       then the background fragment flow supplants the foreground one during traversal.

    LCIA results are always cached in the terminations, and are not (by default) persistent across instantiations.
    """
    @classmethod
    def from_json(cls, fragment, fg, scenario, j):
        if len(j) == 0:
            return cls.null(fragment)
        origin = j.pop('source', None) or j.pop('origin')
        if origin == 'foreground':
            origin = fg.ref

        external_ref = j['externalId']

        # handle term flow
        tf_ref = j.pop('termFlow', None)
        if tf_ref is None:
            term_flow = fragment.flow
        elif isinstance(tf_ref, dict):
            term_flow = fg.catalog_ref(tf_ref['origin'], tf_ref['externalId'], entity_type='flow')
        else:
            if origin == fg.ref:
                term_flow = fg[tf_ref]
            else:
                term_flow = fg.catalog_ref(origin, tf_ref, entity_type='flow')

        # handle term_node
        if origin == fg.ref:
            term_node = fg[external_ref]
        else:
            term_node = fg.catalog_ref(origin, external_ref, entity_type='process')

        direction = j.pop('direction', None)
        descend = j.pop('descend', None)
        inbound_ev = j.pop('inboundExchangeValue', None)
        term = cls(fragment, term_node, direction=direction, term_flow=term_flow, descend=descend,
                   inbound_ev=inbound_ev)
        if 'scoreCache' in j.keys():
            term._deserialize_score_cache(fg, j['scoreCache'], scenario)
        return term

    '''
    @classmethod
    def from_exchange_ref(cls, fragment, exchange_ref):
        return cls(fragment, exchange_ref.process_ref, direction=exchange_ref.direction,
                   term_flow=exchange_ref.exchange.flow, inbound_ev=exchange_ref.exchange.value)

    @classmethod
    def from_term(cls, fragment, term):
        return cls(fragment, term.term_node, direction=term.direction, term_flow=term.term_flow,
                   descend=term.descend, inbound_ev=term.inbound_exchange_value)
    '''

    @classmethod
    def null(cls, fragment):
        return cls(fragment, None)

    def __init__(self, fragment, entity, direction=None, term_flow=None, descend=True, inbound_ev=None):
        """
        reference can be None, an entity or a catalog_ref.  It only must have origin, external_ref, and entity_type.
        To use an exchange, use FlowTermination.from_exchange()
         * None - to create a foreground IO / cutoff flow
         * fragment (same as parent) - to create a foreground node.  Must satisfy 'fragment is entity'
         * catalog ref for process - to link the fragment to a process inventory (uses self.is_background to determine
           foreground or background lookup)
         * catalog ref for flow - to create a foreground emission (subs parent for term, subs flow for term_flow)

        :param fragment:
        :param entity:
        :param direction:
        :param term_flow:
        :param descend:
        :param inbound_ev:
        """

        self._parent = fragment
        if entity is not None:
            if entity.entity_type == 'flow':
                if term_flow is not None:
                    raise ValueError('Conflicting termination and term_flow')
                term_flow = entity
                entity = fragment
        self._term = entity  # this must have origin, external_ref, and entity_type, and be operable (if ref)
        self._descend = True
        self.term_flow = None
        self._cached_ev = 1.0
        self._score_cache = LciaResults(fragment)
        if direction is None:
            self.direction = comp_dir(fragment.direction)
        else:
            self.direction = direction

        self.descend = descend
        self.set_term_params(term_flow, inbound_ev)
        if self._cached_ev is None and not self.is_null:
            raise ValueError('%s\n%s' % (self._parent, self.term_node))
        self.validate_flow_conversion()

    def matches(self, exchange):
        """
        returns True if the exchange specifies the same process and flow as the term's process_ref and term_flow
        :param exchange:
        :return:
        """
        if self.is_null:
            return False
        if self.term_node.entity_type != 'process':
            return False
        return (self._term.external_ref == exchange.process.external_ref) and (self.term_flow.match(exchange.flow))

    def terminates(self, exchange):
        """
        Returns True if the exchange's termination matches the term's term_node, and the flows also match, and the
        directions are complementary.
        If the exchange does not specify a termination, returns True if the flows match and directions are comp.
        :param exchange:
        :return:
        """
        if self.term_flow.match(exchange.flow) and self.direction == comp_dir(exchange.direction):
            if exchange.termination is None:
                return True
            else:
                if self.is_null:
                    return False
                if self.term_node.entity_type != 'process':
                    return False
                if exchange.termination == self._term.external_ref:
                    return True
        return False

    def to_exchange(self):
        if self.is_null:
            return None
        return ExchangeValue(self.term_node, self.term_flow, self.direction, value=self.inbound_exchange_value)

    @property
    def is_local(self):
        """
        Fragment and termination have the same origin
        :return:
        """
        if self.is_null:
            return False
        return self._parent.origin == self.term_node.origin

    @property
    def is_frag(self):
        """
        Termination is a fragment
        :return:
        """
        return (not self.is_null) and (self.term_node.entity_type == 'fragment')

    @property
    def is_process(self):
        """
        termination is a process
        :return:
        """
        return (not self.is_null) and (self.term_node.entity_type == 'process')

    @property
    def is_fg(self):
        """
        Termination is parent
        :return:
        """
        return (not self.is_null) and (self.term_node is self._parent)

    @property
    def is_bg(self):
        """
        parent is marked background, or termination is a background fragment
        :return:
        """
        return self._parent.is_background

    @property
    def term_is_bg(self):
        """
        Termination is local and background
        :return:
        """
        return self.is_local and self.term_node.is_background

    @property
    def is_subfrag(self):
        """
        Termination is a non-background, non-self fragment.
        Controversy around whether expression should be:
        self.is_frag and not (self.is_fg or self.is_bg or self.term_is_bg)  [current] or
        self.is_frag and (not self.is_fg) and (not self.is_bg)  [old; seems wrong]

        :return:
        """
        return self.is_frag and not (self.is_fg or self.is_bg or self.term_is_bg)

    @property
    def is_null(self):
        return self._term is None

    @property
    def descend(self):
        return self._descend

    @descend.setter
    def descend(self, value):
        if value is None:
            return
        if isinstance(value, bool):
            self._descend = value
            if value is True:
                self.clear_score_cache()  # if it's descend, it should have no score_cache
                # if it's not descend, the score cache gets set during traversal
        else:
            raise ValueError('Descend setting must be True or False')

    def self_terminate(self, term_flow=None):
        self._term = self._parent
        self.set_term_params(term_flow)
        self.clear_score_cache()

    @property
    def term_node(self):
        return self._term

    @property
    def flow_conversion(self):
        """
        express the parent's flow in terms of the quantity of the term flow
        if the flows are equal, skip-- paranoid would bypass this step
         if the quantities are equal, skip
         else, ask the flow for a cf
         else, ask the quantity for a conversion
         what if the quantity is not a ref? how do we find our local catalog and qdb?
          - unless qdb is upstream of entities themselves
          - perhaps for quantities that makes sense
          - qty query will always fallback to qdb

        how to deal with scenario cfs? tbd
        problem is, the term doesn't know its own scenario
        :return:
        """
        if self._parent.flow == self.term_flow:
            return 1.0
        parent_qty = self._parent.flow.reference_entity
        tgt_qty = self.term_flow.reference_entity
        if parent_qty == tgt_qty:
            return 1.0
        if self._parent.flow.cf(tgt_qty) == 0:
            if self.term_flow.cf(parent_qty) == 0:
                '''
                print('term flow')
                self.term_flow.show()
                self.term_flow.profile()
                '''
                print('\nfragment flow %s' % self._parent)
                self._parent.flow.show()
                self._parent.flow.profile()
                raise FlowConversionError('Missing cf\nfrom: %s %s\n  to: %s %s' % (parent_qty.uuid, parent_qty,
                                                                                    tgt_qty.uuid, tgt_qty))
            else:
                return 1.0 / self.term_flow.cf(parent_qty)
        return self._parent.flow.cf(tgt_qty)

    def validate_flow_conversion(self):
        return self.flow_conversion  # deal with it when an error comes up

    @property
    def id(self):
        if self.is_null:
            return None
        else:
            return self._term.external_ref

    @property
    def inbound_exchange_value(self):
        if self.is_bg:
            return 1.0
        return self._cached_ev

    @inbound_exchange_value.setter
    def inbound_exchange_value(self, val):
        if self.is_fg:
            self._cached_ev = val
        else:
            raise NonConfigurableInboundEV

    @property
    def node_weight_multiplier(self):
        return self.flow_conversion / self.inbound_exchange_value

    @property
    def unit(self):
        if self.is_null:
            return '--'
        if self.term_node.entity_type == 'fragment':  # fg, bg, or subfragment
            return '%4g unit' % self.inbound_exchange_value
        return '%4g %s' % (self.inbound_exchange_value, self.term_flow.unit())  # process

    def set_term_params(self, term_flow=None, inbound_ev=None):
        """
        Sets the term_flow and cached exchange value by finding a reference exchange that matches the specified flow.

        Direction is as specified in the constructor, but gets overwritten by a successful exchange lookup.

        If both flow and ev are specified, quell catalog lookup and use them.

        If no flow is specified, use the parent's flow. If that fails, use the node's reference flow.

        If the term is a subfragment, the cached ev is always 1.0; default flow is same as child's flow.

        If a reference is found, the direction and cached_ev are set based on the reference, overriding the
        instantiation parameters.

        :param term_flow: if None, autodetect
        :param inbound_ev: if None, autodetect
        :return:
        """
        if self.is_null:
            term_flow = self._parent.flow
        elif self._term.entity_type == 'fragment':
            if term_flow is None:
                # let's try relaxing this
                # term flow must be sub-fragment's reference flow
                term_flow = self.term_node.flow
            # set direction of term to be direction of flow relative to term node
            if self.is_subfrag:
                inbound_ev = 1.0
                if self.term_node.reference_entity is None:
                    self.direction = comp_dir(self.term_node.direction)
                else:
                    self.direction = self.term_node.direction
        else:
            if inbound_ev is None or term_flow is None:
                try:
                    if term_flow is None:
                        r_e = self._term.reference(self._parent.flow)
                    else:
                        r_e = self._term.reference(term_flow)
                except NoReferenceFound:
                    r_e = self._term.reference()  # will raise AmbiguousReferenceError if multiple
                self.direction = r_e.direction
                inbound_ev = r_e.value
                if term_flow is None:
                    term_flow = r_e.flow

        self.term_flow = term_flow
        self._cached_ev = inbound_ev or 1.0

    '''
    def flowdb_results(self, lcia_results):
        self._score_cache = lcia_results

    def set_score_cache(self, lcia, quantities):
        """

        :param lcia: a lambda that takes as input a process ref and a ref flow and a list of quantities, and
        returns a dict of LciaResults
        fragment LCIA results are not cached, but instead are computed on demand. we'll see if that works for highly
        nested models. we will have to cache traversals somewhere- but I think that can be done by the manager.
        :param quantities:
        :return:
        """
        if self.is_null:
            return
        q_run = []
        for q in quantities:
            if q.uuid not in self._score_cache.keys():
                q_run.append(q)
        if len(q_run) != 0:
            if self.is_fg or self.term_node.entity_type == 'process':
                results = lcia(self.term_node, self.term_flow, q_run)
                self._score_cache.update(results)
    '''

    def _unobserved_exchanges(self):
        """
        Generator which yields exchanges from the term node's inventory that are not found among the child flows, for
          LCIA purposes
        :return:
        """
        if self.is_fg:
            x = ExchangeValue(self._parent, self._parent.flow, self._parent.direction,
                              value=self.node_weight_multiplier)
            yield x
        elif self.term_is_bg:
            for x in []:
                yield x
        # elif self.is_frag:  # fragments can have unobserved exchanges too!
        #     for x in []:
        #         yield x
        else:
            children = set()
            children.add((self.term_flow, self.direction))
            for c in self._parent.child_flows:
                children.add((c.flow, c.direction))
            if self.is_bg:
                iterable = self.term_node.lci(ref_flow=self.term_flow)
            else:
                if len(self.term_node.reference_entity) > 1:
                    # TODO: figure out a better solution for this
                    # print('WARNING: see _unobserved_exchanges')
                    '''
                    This will cause a wrong result in the following situation:
                     * termination is built with a multi-output process 
                     * inbound_exchange_value was set to a non-unity value, either manually or in set_term_params
                     
                    '''
                    iterable = self.term_node.inventory(self.term_flow)
                else:
                    iterable = self.term_node.inventory()
            for x in iterable:
                if (x.flow, x.direction) not in children:
                    yield x

    def compute_unit_score(self, quantity_ref, **kwargs):
        """
        four different ways to do this.
        0- we are a subfragment-- throw exception: use subfragment traversal results contained in the FragmentFlow
        1- parent is bg: ask catalog to give us bg_lcia (process or fragment)
        2- get fg lcia for unobserved exchanges

        If
        :param quantity_ref:
        :return:
        """
        if self.is_subfrag:
            if self.descend:
                return LciaResult(quantity_ref)  # null result for subfragments that are explicitly followed
            else:
                raise SubFragmentAggregation  # to be caught

        if self.is_bg and self.is_frag:
            # need bg_lcia method for FragmentRefs
            # this is probably not currently supported
            return self.term_node.bg_lcia(lcia_qty=quantity_ref, ref_flow=self.term_flow.external_ref, **kwargs)

        try:
            locale = self.term_node['SpatialScope']
        except KeyError:
            locale = 'GLO'
        try:
            res = quantity_ref.do_lcia(self._unobserved_exchanges(), locale=locale, **kwargs)
        except PrivateArchive:
            if self.is_bg:
                print('terminations.compute_unit_score UNTESTED for private bg archives!')
                res = self.term_node.bg_lcia(lcia_qty=quantity_ref, ref_flow=self.term_flow.external_ref, **kwargs)
            else:
                res = self.term_node.fg_lcia(quantity_ref, ref_flow=self.term_flow.external_ref, **kwargs)
                print('terminations.compute_unit_score UNTESTED for private fg archives!')
                # res.set_scale(self.inbound_exchange_value)
        return res

    def score_cache(self, quantity=None, **kwargs):
        if quantity is None:
            return self._score_cache
        if quantity.uuid in self._score_cache:
            return self._score_cache[quantity.uuid]
        else:
            res = self.compute_unit_score(quantity, **kwargs)
            self._score_cache[quantity.uuid] = res
            return res

    def score_cache_items(self):
        return self._score_cache.items()

    def lcia(self):
        for k, v in self.score_cache_items():
            print('%s' % v)

    def clear_score_cache(self):
        self._score_cache.clear()

    def _serialize_score_cache(self):
        """
        Score cache contains an LciaResults object, which works as a dict.
        serialization should preserve order, which prohibits using a simple dict
        :return: a list to be serialized directly
        """
        score_cache = []
        for q in self._score_cache.indices():
            res = self._score_cache[q]
            score_cache.append({'quantity': {'origin': res.quantity.origin,
                                             'externalId': res.quantity.external_ref},
                                'score': res.total()})
        return score_cache

    def _deserialize_score_cache(self, fg, sc, scenario):
        self._score_cache = LciaResults(self._parent)
        for i in sc:
            q = fg.catalog_ref(i['quantity']['origin'], i['quantity']['externalId'], entity_type='quantity')
            res = LciaResult(q, scenario=scenario)
            res.add_summary(self._parent.uuid, self._parent, 1.0, i['score'])
            self._score_cache.add(res)

    def serialize(self, save_unit_scores=False):
        if self.is_null:
            return {}
        j = {
            'origin': self._term.origin,
            'externalId': self._term.external_ref
        }
        if self.term_flow != self._parent.flow:
            if self.term_flow.origin == self.term_node.origin:
                j['termFlow'] = self.term_flow.external_ref
            else:
                j['termFlow'] = {
                    'origin': self.term_flow.origin,
                    'externalId': self.term_flow.external_ref
                }
        if self.direction != comp_dir(self._parent.direction):
            j['direction'] = self.direction
        if self._descend is False:
            j['descend'] = False
        if self._cached_ev != 1.0:
            j['inboundExchangeValue'] = self._cached_ev
        if self._parent.is_background and save_unit_scores and len(self._score_cache) > 0:
            j['scoreCache'] = self._serialize_score_cache()
        return j

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, FlowTermination):
            return False
        if self.is_null:
            if other.is_null:
                return True
            return False
        return (self.term_node.external_ref == other.term_node.external_ref and
                self.term_flow == other.term_flow and
                self.direction == other.direction)

    def __str__(self):
        """

        :return:
          '---:' = fragment I/O
          '-O  ' = foreground node
          '-*  ' = process
          '-#  ' - sub-fragment (aggregate)
          '-#::' - sub-fragment (descend)
          '-B ' - terminated background
          '--C ' - cut-off background
        """
        if self.is_null:
            term = '---:'  # fragment IO
        elif self.is_fg:
            term = '-O  '
        elif self.term_node.entity_type == 'process':
            if self.is_bg:
                term = '-B* '
            else:
                term = '-*  '
        elif self.term_node.entity_type == 'fragment':
            if self.term_is_bg:
                # TODO: Broken! needs to be scenario-aware
                if self.term_node.term.is_null:
                    term = '--C '
                else:
                    term = '-B  '
            else:
                if self.descend:
                    term = '-#::'
                else:
                    term = '-#  '
        else:
            raise TypeError('I Do not understand this term for frag %.7s' % self._parent.uuid)
        return term

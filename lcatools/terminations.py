"""
Flow Terminations are model-defined links between a particular flow and a process that terminates it.

They originated as part of LcFragments but in fact they are more general. A FlowTermination is actually the same
as a ProductFlow in lca-matrix, although the FlowTermination is more powerful.  It should be easy to construct
either one from the other.
"""

from lcatools.lcia_results import LciaResult, LciaResults, traversal_to_lcia
from lcatools.exchanges import comp_dir, ExchangeValue, MissingReference
from lcatools.interact import pick_one, parse_math


class FlowConversionError(Exception):
    pass


class FlowTermination(object):
    """
    these are stored by scenario in a dict on the mainland

    A fragment can have the following types of terminations:
     * None - the termination is null- the flow enters the foreground and becomes an i/o
     * Self - the flow enters a foreground node. The node can have children but only has LCIA impacts based on
       the terminating flow, which have to be looked up in the database. fg-terminated nodes don't have scenarios
       (e.g. the scenarios are in the exchange values)
       (created with the yet-unwritten add child flow function if the node is null)
     * Process - the flow enters a process referenced by CatalogRef.  The node's LCIA impacts are fg_lcia. The
       node's children are the process's non-term intermediate exchanges. The node can also have other children.
       (created with terminate or term_from_exch)
     * Fragment - the flow enters a sub-fragment.  The sub-fragment must be traversable. The node's children are
       the fragment's non-term io flows. The node cannot have other children.  If the sub-fragment is background,
       then the background fragment flow supplants the foreground one during traversal.

    LCIA results are always cached in the terminations, and are not (by default) persistent across instantiations.
    """
    @classmethod
    def from_json(cls, fragment, catalog, scenario, j):
        if len(j) == 0:
            return cls.null(fragment)
        if j['source'] == 'foreground':
            index = 0
        else:
            index = catalog.index_for_source(j['source'])
        process_ref = catalog.ref(index, j['entityId'])
        if process_ref.entity_type == 'fragment':
            process_ref = process_ref.entity()
        term_flow = j.pop('termFlow', None)
        if term_flow is not None:
            term_flow = catalog.ref(index, term_flow).entity()
        direction = j.pop('direction', None)
        descend = j.pop('descend', None)
        term = cls(fragment, process_ref, direction=direction, term_flow=term_flow, descend=descend)
        if 'scoreCache' in j.keys():
            term._deserialize_score_cache(catalog, j['scoreCache'], scenario)
        return term

    @classmethod
    def from_exchange_ref(cls, fragment, exchange_ref):
        return cls(fragment, exchange_ref.process_ref, direction=exchange_ref.direction,
                   term_flow=exchange_ref.exchange.flow, inbound_ev=exchange_ref.exchange.value)

    @classmethod
    def from_term(cls, fragment, term):
        return cls(fragment, term.term_node, direction=term.direction, term_flow=term.term_flow,
                   descend=term.descend, inbound_ev=term.inbound_exchange_value)

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
         * catalog ref for flow - to create a foreground emission

        :param fragment:
        :param entity:
        :param direction:
        :param term_flow:
        :param descend:
        :param inbound_ev:
        """

        self._parent = fragment
        self._process_ref = entity  # this must have origin, external_ref, and entity_type
        self._descend = True
        self.term_flow = None
        self._cached_ev = 1.0
        self._score_cache = LciaResults(fragment)
        if direction is None:
            self.direction = comp_dir(fragment.direction)
        else:
            self.direction = direction

        self.descend = descend
        self.set_term_flow(term_flow)
        self._set_inbound_ev(inbound_ev)
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
        return (self._process_ref.id == exchange.process.get_uuid()) and (self.term_flow.match(exchange.flow))

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
                if exchange.termination == self._process_ref.id:
                    return True
        return False

    def to_exchange(self):
        if self.is_null:
            return None
        elif self.term_node.entity_type == 'fragment':
            return ExchangeValue(self.term_node, self.term_flow, self.direction, value=self._cached_ev)
        return ExchangeValue(self.term_node.entity(), self.term_flow, self.direction, value=self._cached_ev)

    @property
    def is_frag(self):
        return (not self.is_null) and (self.term_node.entity_type == 'fragment')

    @property
    def is_fg(self):
        return self.is_frag and (self.term_node is self._parent)

    @property
    def is_bg(self):
        return self.is_frag and self.term_node.is_background

    @property
    def is_subfrag(self):
        return self.is_frag and (not self.is_fg) and (not self.is_bg)

    @property
    def is_null(self):
        return self._process_ref is None

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
        self._process_ref = self._parent
        self.set_term_flow(term_flow)
        self.clear_score_cache()
        self._cached_ev = 1.0

    @property
    def index(self):
        if self._process_ref is None:
            return 0
        elif self._process_ref.entity_type == 'fragment':
            return 0
        return self._process_ref.index

    @property
    def term_node(self):
        return self._process_ref

    @property
    def flow_conversion(self):
        """
        this gets computed at query time- raises an issue about parameterization (scenario must be known?)
        # TODO: figure out flow conversion params
        flow conversion info must be saved in fragment's flow because it is guaranteed to live in foreground
        :return:
        """
        # TODO: this should belong to the flow
        tgt_qty = self.term_flow.reference_entity
        if self._parent.flow.cf(tgt_qty) == 0:
            '''
            print('term flow')
            self.term_flow.show()
            self.term_flow.profile()
            '''
            print('\nfragment flow')
            self._parent.flow.show()
            self._parent.flow.profile()
            raise FlowConversionError('Missing cf for %s' % tgt_qty)
        return self._parent.flow.convert(1.0, to=tgt_qty)

    def validate_flow_conversion(self):
        # TODO: this should belong to the flow
        try:
            a = self.flow_conversion
            if a == 42:
                print('you are so lucky!')
        except FlowConversionError:
            print('Flow %s ' % self._parent.flow)
            print('Provide conversion factor %s (fragment) to %s (termination)' % (self._parent.flow.unit(),
                                                                                   self.term_flow.unit()))
            cf = parse_math(input('Enter conversion factor: '))
            self._parent.flow.add_characterization(self.term_flow.reference_entity, value=cf)

            # this is surely a hack!
            if self.term_node.entity_type != 'fragment':
                # if it's a fragment, then its flow's quantities are already in catalog[0]
                self._process_ref.catalog[0].add(self.term_flow.reference_entity)
            # funny, it doesn't look like that bad of a hack.

    def _set_inbound_ev(self, inbound_ev):
        if self.is_fg:
            # foreground nodes can't have inbound EVs since there is no where to serialize them
            self._cached_ev = 1.0
            return
        if inbound_ev is None:
            if self._process_ref is None:
                inbound_ev = 1.0
            elif self.term_node.entity_type == 'process':
                process = self._process_ref.fg()
                try:
                    ex = next(x for x in process.exchange(self.term_flow, direction=self.direction))
                    try:
                        inbound_ev = ex[self.term_flow]
                    except MissingReference:
                        inbound_ev = ex.value
                except StopIteration:
                    inbound_ev = 1.0
            elif self.term_node.entity_type == 'fragment':
                inbound_ev = 1.0  # the inbound ev must be applied at traversal time;
            else:
                raise TypeError('How did we get here??? %s' % self._process_ref)
        self._cached_ev = inbound_ev

    @property
    def id(self):
        if self.is_null:
            return None
        elif self._process_ref.entity_type == 'process':
            return self._process_ref.id
        else:
            return self._process_ref.get_uuid()

    @property
    def inbound_exchange_value(self):
        return self._cached_ev

    @property
    def node_weight_multiplier(self):
        return self.flow_conversion / self.inbound_exchange_value

    @property
    def unit(self):
        if self.term_node.entity_type == 'fragment':  # fg, bg, or subfragment
            return '%4g unit' % self._cached_ev
        return '%4g %s' % (self._cached_ev, self.term_flow.unit())  # process

    def set_term_flow(self, flow):
        """
        flow must have an exchange with process ref
        :param flow:
        :return:
        """
        if self.is_null:
            flow = self._parent.flow
        elif self._process_ref.entity_type == 'fragment':
            if flow is None:
                # let's try relaxing this
                # term flow must be sub-fragment's reference flow
                flow = self.term_node.flow
        else:
            if flow is None:
                flow = self._parent.flow
            try:
                next(self._process_ref.fg().exchange(flow, direction=self.direction))
            except StopIteration:
                r_e = self._process_ref.fg().reference_entity
                if len(r_e) == 1:
                    r_e = list(r_e)[0]
                    flow = r_e.flow
                    self.direction = r_e.direction
                elif len(r_e) > 0:
                    r_e = pick_one(list(r_e))
                    flow = r_e.flow
                    self.direction = r_e.direction
                else:
                    # instead of throwing exception, just tolerate a no-reference-flow node using _parent.flow
                    pass
                    # raise MissingFlow('%s missing flow %s\nAND no reference exchange' % (self._process_ref, flow))

            except TypeError:
                print('Fragment: %s\nprocess_ref: %s' % (self._parent, self._process_ref))
                raise
        self.term_flow = flow

    def aggregate_subfragments(self, subfrags):
        """
        Performs an aggregation of the subfragment score caches to compute a fragment score cache. use with caution!
        :param subfrags:
        :return:
        """
        self._score_cache = traversal_to_lcia(subfrags)

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
            if q.get_uuid() not in self._score_cache.keys():
                q_run.append(q)
        if len(q_run) != 0:
            if self.is_fg or self.term_node.entity_type == 'process':
                results = lcia(self.term_node, self.term_flow, q_run)
                self._score_cache.update(results)

    def score_cache(self, quantity=None):
        if quantity is None:
            return self._score_cache
        if quantity.get_uuid() in self._score_cache:
            return self._score_cache[quantity.get_uuid()]
        return None

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
        return [{"quantity": q, "score": self._score_cache[q].total()} for q in self._score_cache.indices()]

    def _deserialize_score_cache(self, catalog, sc, scenario):
        self._score_cache = LciaResults(self._parent)
        for i in sc:
            res = LciaResult(catalog[0][i["quantity"]], scenario=scenario)
            res.add_summary(self._parent.get_uuid(), self._parent, 1.0, i['score'])
            self._score_cache.add(res)

    def serialize(self):
        if self._process_ref is None:
            return {}
        if self.index == 0:
            source = 'foreground'
        else:
            source = self._process_ref.catalog.source_for_index(self._process_ref.index)
        j = {
            'source': source,
            'entityId': self.id
        }
        if self.term_flow != self._parent.flow:
            j['termFlow'] = self.term_flow.get_uuid()
        if self.direction != comp_dir(self._parent.direction):
            j['direction'] = self.direction
        if self._descend is False:
            j['descend'] = False
        if self._parent.is_background:
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
        return (self.term_node.get_uuid() == other.term_node.get_uuid() and
                self.term_flow == other.term_flow and
                self.direction == other.direction)

    def __str__(self):
        """

        :return:
          '---:' = fragment I/O
          '-O  ' = foreground node
          '-*  ' = process
          '-#  ' - sub-fragment (aggregate)
          '-#: ' - sub-fragment (descend)
          '-B ' - terminated background
          '--C ' - cut-off background
        """
        if self.is_null:
            term = '---:'  # fragment IO
        elif self.is_fg:
            term = '-O  '
        elif self.term_node.entity_type == 'process':
            term = '-*  '
        elif self.term_node.entity_type == 'fragment':
            if self.term_node.is_background:
                if self.term_node.term.is_null:
                    term = '--C '
                else:
                    term = '-B  '
            else:
                if self.descend:
                    term = '-#: '
                else:
                    term = '-#  '
        else:
            raise TypeError('I Do not understand this term for frag %.7s' % self._parent.get_uuid())
        return term

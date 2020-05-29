from .abstract_query import AbstractQuery
from .iindex import check_direction


class ExchangeRequired(Exception):
    pass


_interface = 'exchange'


class ExchangeRef(object):
    """
    Codifies the information required to define an exchange.  The supplied information could be either object or
    reference/link; it isn't specified.
    """
    is_reference = False

    def __init__(self, process, flow, direction, value=0, unit=None, termination=None, reference=None, **kwargs):
        """

        :param process:
        :param flow:
        :param direction:
        :param value:
        :param unit:
        :param termination:
        """
        self._node = process
        self._flow = flow
        self._dir = check_direction(direction)
        self._val = value
        if unit is None:
            if hasattr(self._flow, 'unit'):
                unit = self._flow.unit()
            else:
                unit = ''
        self._unit = unit
        self._term = termination
        self.args = kwargs
        if reference is not None:
            self.is_reference = bool(reference)

    @property
    def process(self):
        return self._node

    @property
    def flow(self):
        return self._flow

    @property
    def direction(self):
        return self._dir

    @property
    def value(self):
        return self._val

    @property
    def termination(self):
        return self._term

    @property
    def unit(self):
        return self._unit

    def __getitem__(self, item):
        if isinstance(self._val, dict):
            return self._val[item]
        else:
            return 0.0

    def __str__(self):
        ds = {'Input': '<--',
              'Output': '==>'}[self._dir]
        if self._term is None:
            tt = ''
        else:
            tt = ' %s' % self._term
        if isinstance(self._val, dict):
            v = '{ #%d# }' % len(self._val)
        else:
            v = '%.3g' % self.value
        return '[ %s ] %s %s (%s) %s%s' % (self.process, ds, v, self.flow, self.unit, tt)


class ExchangeInterface(AbstractQuery):
    """
    InventoryInterface core methods: individual processes, quantitative data.

    Need to do some thinking here-- the list of methods is very short. In particular there is no way to do any of
    the following:
     * retrieve reference exchanges
     * retrieve cutoff exchanges or only terminated exchanges
     * retrieve only intermediate or only elementary exchanges [[ pending context refactor :(:( ]]
       = this is frankly not possible to do, even after the context refactor, because there is no bulletproof way to
         determine whether a termination is a context or a process or some non-elementary compartment, until you
         introduce the Lcia Engine with its reference set of contexts, UNLESS you include the reference elementary set
         in ALL context managers.  Which is feasible- since it's a short list- but still, not done as yet.
    There's also no access to information from the original source that is not part of the data model, e.g. uncertainty
    information, arbitrary XML queries, etc.
    """
    def exchanges(self, process, **kwargs):
        """
        Retrieve process's full exchange list, without values
        :param process:
        :return:
        """
        return self._perform_query(_interface, 'exchanges',
                                   ExchangeRequired('No access to exchange data'), process, **kwargs)

    def exchange_values(self, process, flow, direction=None, termination=None, reference=None, **kwargs):
        """
        Return a list of exchanges with values matching the specification
        :param process:
        :param flow:
        :param direction: [None] if none,
        :param termination: [None] if none, return all terminations
        :param reference: [None] if True, only find reference exchanges. If false- maybe omit reference exchanges?
        :return:
        """
        return self._perform_query(_interface, 'exchange_values',
                                   ExchangeRequired('No access to exchange data'),
                                   process, flow, direction=direction, termination=termination, reference=reference,
                                   **kwargs)

    def inventory(self, process, ref_flow=None, scenario=None, **kwargs):
        """
        Return a list of exchanges with values. If no reference is supplied, return all unallocated exchanges, including
        reference exchanges.

        If a reference flow is supplied, expected behavior depends on a number of factors.
         - If the supplied reference flow is part of the process's reference entity, the inventory should return all
         non-reference exchanges, appropriately allocated to the specified flow, and normalized to a unit of the
         specified flow.
         - If the supplied reference flow is not part of the reference entity, NO allocation should be performed.
         Instead, the inventory should return ALL exchanges except for the specified flow, un-allocated, normalized to
         a unit of the specified flow.  This query is only valid if the specified flow is a cut-off (i.e. un-terminated)
         exchange.
         - If the supplied reference flow is a non-reference, non-cutoff flow (i.e. it is a terminated exchange), then
         the appropriate behavior is undefined. The default implementation raises an ExchangeError.

        Note: if this is called on a fragment, the signature is the same but the 'ref_flow' argument is ignored and
        the alternative 'scenario' kwarg is accepted
        :param process:
        :param ref_flow: used only for processes
        :param scenario: used only for fragments
        :return:
        """
        return self._perform_query(_interface, 'inventory', ExchangeRequired('No access to exchange data'),
                                   process, ref_flow=ref_flow, scenario=scenario, **kwargs)

    def exchange_relation(self, process, ref_flow, exch_flow, direction, termination=None, **kwargs):
        """
        Always returns a single float.

        :param process:
        :param ref_flow:
        :param exch_flow:
        :param direction:
        :param termination:
        :return:
        """
        return self._perform_query(_interface, 'exchange_relation', ExchangeRequired('No access to exchange data'),
                                   process, ref_flow, exch_flow, direction, termination=termination, **kwargs)

    def lcia(self, process, ref_flow, quantity_ref, **kwargs):
        """
        Perform process foreground LCIA for the given quantity reference.
        :param process:
        :param ref_flow:
        :param quantity_ref:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'lcia', ExchangeRequired('No access to exchange data'),
                                   process, ref_flow, quantity_ref, **kwargs)

    def traverse(self, fragment, scenario=None, **kwargs):
        """
        Traverse the fragment (observed) according to the scenario specification and return a list of FragmentFlows
        :param fragment:
        :param scenario:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'traverse', ExchangeRequired('No access to fragment data'),
                                   fragment, scenario, **kwargs)

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, **kwargs):
        """
        Perform fragment LCIA by first traversing the fragment to determine node weights, and then combining with
        unit scores.
        :param fragment:
        :param quantity_ref:
        :param scenario:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'fragment_lcia', ExchangeRequired('No access to fragment data'),
                                   fragment, quantity_ref, scenario, **kwargs)

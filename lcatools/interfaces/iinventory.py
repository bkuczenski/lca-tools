from .abstract_query import AbstractQuery


class InventoryRequired(Exception):
    pass


_interface = 'inventory'


class InventoryInterface(AbstractQuery):
    """
    InventoryInterface core methods: individual processes, quantitative data.
    """
    def exchanges(self, process, **kwargs):
        """
        Retrieve process's full exchange list, without values
        :param process:
        :return:
        """
        return self._perform_query(_interface, 'exchanges',
                                   InventoryRequired('No access to exchange data'), process, **kwargs)

    def exchange_values(self, process, flow, direction, termination=None, **kwargs):
        """
        Return a list of exchanges with values matching the specification
        :param process:
        :param flow:
        :param direction:
        :param termination: [None] if none, return all terminations
        :return:
        """
        return self._perform_query(_interface, 'exchange_values',
                                   InventoryRequired('No access to exchange data'),
                                   process, flow, direction, termination=termination, **kwargs)

    def inventory(self, process, ref_flow=None, **kwargs):
        """
        Return a list of exchanges with values. If no reference is supplied, return all unallocated exchanges, including
        reference exchanges. If a reference is supplied, return allocated (but not normalized) exchanges, excluding
        reference exchanges.

        Note: if this is called on a fragment, the signature is the same but the 'ref_flow' argument is interpreted
        as a 'scenario' specification instead, inclusive of the fragment's reference exchange
        :param process:
        :param ref_flow:
        :return:
        """
        print(_interface)
        return self._perform_query(_interface, 'inventory', InventoryRequired('No access to exchange data'),
                                   process, ref_flow=ref_flow, **kwargs)

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
        return self._perform_query(_interface, 'exchange_relation', InventoryRequired('No access to exchange data'),
                                   process, ref_flow, exch_flow, direction, termination=termination, **kwargs)

    def lcia(self, process, ref_flow, quantity_ref, refresh=False, **kwargs):
        """
        Perform process foreground LCIA for the given quantity reference.
        :param process:
        :param ref_flow:
        :param quantity_ref:
        :param refresh:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'lcia', InventoryRequired('No access to exchange data'),
                                   process, ref_flow, quantity_ref, refresh=refresh, **kwargs)

    def traverse(self, fragment, scenario=None, **kwargs):
        """
        Traverse the fragment (observed) according to the scenario specification and return a list of FragmentFlows
        :param fragment:
        :param scenario:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'traverse', InventoryRequired('No access to fragment data'),
                                   fragment, scenario, **kwargs)

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        """
        Perform fragment LCIA by first traversing the fragment to determine node weights, and then combining with
        unit scores.
        :param fragment:
        :param quantity_ref:
        :param scenario:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'fragment_lcia', InventoryRequired('No access to fragment data'),
                                   fragment, quantity_ref, scenario, refresh=refresh, **kwargs)

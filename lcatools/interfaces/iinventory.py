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
        return self._perform_query([_interface, 'background'], 'exchange_values',
                                   InventoryRequired('No access to exchange data'),
                                   process, flow, direction, termination=termination, **kwargs)

    def inventory(self, process, ref_flow=None, **kwargs):
        """
        Return a list of exchanges with values. If no reference is supplied, return all unallocated exchanges, including
        reference exchanges. If a reference is supplied, return allocated (but not normalized) exchanges, excluding
        reference exchanges.
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

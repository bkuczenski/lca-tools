from .abstract_query import AbstractQuery


class ConfigRequired(Exception):
    pass


_interface = 'configure'


class ConfigureInterface(AbstractQuery):
    """
    A class for tuning and tweaking the reference flow configuration of processes in an archive.
    """
    def check_config(self, config, c_args, **kwargs):
        """
        Check that a given configuration tuple is valid
        :param config: a config option
        :param c_args: a tuple of args passed to the config optioin
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'check_config', ConfigRequired('Configuration Interface required'),
                                   config, c_args, **kwargs)

    def add_terms(self, term_type, *terms, **kwargs):
        """

        :param term_type: 'flow', 'context', or 'quantity'
        :param terms: any number of terms that are synonyms for one another, to be added to the term manager
        :param kwargs:

        Example: add_terms('context', 'emissions', 'NETL Coal Elementary Flows')
        :return:
        """
        return self._perform_query(_interface, 'add_terms', NotImplementedError,
                                   term_type, *terms, **kwargs)

    def set_reference(self, process_ref, flow_ref, direction=None, **kwargs):
        """
        Set a particular exchange as a reference flow for a given process.  The exchange must already exist.  No error
        if the exchange is already a reference.
        :param process_ref:
        :param flow_ref:
        :param direction: if None, the flow_ref must only appear with one direction
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'set_reference', ConfigRequired('Configuration Interface required'),
                                   process_ref, flow_ref, direction, **kwargs)

    def unset_reference(self, process_ref, flow_ref, direction=None, **kwargs):
        """
        Unset a particular exchange from being a reference.  The exchange must already exist.  No error if the exchange
        is already not a reference.
        :param process_ref:
        :param flow_ref:
        :param direction: if None, the flow_ref must only appear with one direction
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'unset_reference', ConfigRequired('Configuration Interface required'),
                                   process_ref, flow_ref, direction, **kwargs)

    def characterize_flow(self, flow_ref, quantity_ref, value, location='GLO', **kwargs):
        """
        Add a characterization to the given flow in the given quantity, with respect to the flow's native reference
         quantity.  Optional location field.
        :param flow_ref:
        :param quantity_ref:
        :param value:
        :param location: ['GLO']
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'characterize_flow',
                                   ConfigRequired('Configuration Interface required'),
                                   flow_ref, quantity_ref, value, location, **kwargs)

    def allocate_by_quantity(self, process_ref, quantity_ref, **kwargs):
        """
        Allocate the multioutput process by partitioning over the reference flows by the specified quantity.
        :param process_ref:
        :param quantity_ref:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'allocate_by_quantity',
                                   ConfigRequired('Configuration Interface required'),
                                   process_ref, quantity_ref, **kwargs)

from .abstract_query import AbstractQuery


class QuantityRequired(Exception):
    pass


_interface = 'quantity'


class QuantityInterface(AbstractQuery):
    """
    QuantityInterface
    """
    def profile(self, flow, **kwargs):
        """
        Generate characterizations for the named flow, with the reference quantity noted
        :param flow:
        :return:
        """
        return self._perform_query(_interface, 'profile', QuantityRequired('Must have quantity interface'),
                                   flow, **kwargs)

    def get_canonical(self, quantity, **kwargs):
        """
        Retrieve a canonical quantity based on a synonym or other distinguishable term.  In future this should be
        expanded to flows and contexts.
        :param quantity: external_id of quantity
        :return: quantity CatalogRef
        """
        return self.make_ref(self._perform_query(_interface, 'get_canonical',
                                                 QuantityRequired('Quantity interface required'),
                                                 quantity, **kwargs))

    def factors(self, quantity, flowable=None, compartment=None, **kwargs):
        """
        Return characterization factors for the given quantity, subject to optional flowable and compartment
        filter constraints. This is ill-defined because the reference unit is not explicitly reported in current
        serialization for characterizations (it is implicit in the flow)-- but it can be added to a web service layer.
        :param quantity:
        :param flowable:
        :param compartment:
        :return:
        """
        return self._perform_query(_interface, 'factors', QuantityRequired('Quantity interface required'),
                                   quantity, flowable=flowable, compartment=compartment, **kwargs)

    def cf(self, flow, quantity, locale='GLO', **kwargs):
        """
        Determine a flow's characterization factor for a given quantity by consulting the flow directly.  This will
        fall back to a Qdb lookup if the flow's originating resource cannot answer the question.
        :param flow:
        :param quantity:
        :param locale:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'cf', QuantityRequired('Quantity interface required'),
                                   flow, quantity, locale=locale, **kwargs)

    def quantity_relation(self, ref_quantity, flowable, compartment, query_quantity, locale='GLO', **kwargs):
        """
        Return a single number that converts the a unit of the reference quantity into the query quantity for the
        given flowable, compartment, and locale (default 'GLO').  If no locale is found, this would be a great place
        to run a spatial best-match algorithm.
        :param ref_quantity:
        :param flowable:
        :param compartment:
        :param query_quantity:
        :param locale:
        :return:
        """
        return self._perform_query(_interface, 'quantity_relation', QuantityRequired('Quantity interface required'),
                                   ref_quantity, flowable, compartment, query_quantity, locale=locale, **kwargs)

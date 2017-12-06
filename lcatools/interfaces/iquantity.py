from .abstract_query import AbstractQuery


class QuantityRequired(Exception):
    pass


_interface = 'quantity'


class QuantityInterface(AbstractQuery):
    """
    QuantityInterface
    """
    def lcia_methods(self, **kwargs):
        """
        Retrieve a list of known LCIA methods.  This is going to be implementation-dependent
        :return:
        """
        for i in self._perform_query(_interface, 'lcia_methods', QuantityRequired('Must have quantity interface'),
                                     **kwargs):
            yield self.make_ref(i)

    def profile(self, flow, **kwargs):
        """
        Generate characterizations for the named flow, with the reference quantity noted
        :param flow:
        :return:
        """
        return self._perform_query(_interface, 'profile', QuantityRequired('Must have quantity interface'),
                                   flow, **kwargs)

    def get_quantity(self, quantity, **kwargs):
        """
        Retrieve a canonical quantity from a qdb
        :param quantity: external_id of quantity
        :return: quantity CatalogRef
        """
        return self._perform_query(_interface, 'get_quantity', QuantityRequired('Quantity interface required'),
                                   quantity, **kwargs)

    def synonyms(self, item, **kwargs):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment
        :param item:
        :return: list of strings
        """
        return self._perform_query(_interface, 'synonyms', QuantityRequired('Quantity interface required'), item,
                                   ** kwargs)

    def flowables(self, quantity=None, compartment=None, **kwargs):
        """
        Return a list of flowable strings. Use quantity and compartment parameters to narrow the result
        set to those characterized by a specific quantity, those exchanged with a specific compartment, or both
        :param quantity:
        :param compartment:
        :return: list of pairs: CAS number, name
        """
        return self._perform_query(_interface, 'flowables', QuantityRequired('Quantity interface required'),
                                   quantity=quantity, compartment=compartment, **kwargs)

    def compartments(self, quantity=None, flowable=None, **kwargs):
        """
        Return a list of compartment strings. Use quantity and flowable parameters to narrow the result
        set to those characterized for a specific quantity, those with a specific flowable, or both
        :param quantity:
        :param flowable:
        :return: list of strings
        """
        return self._perform_query(_interface, 'compartments', QuantityRequired('Quantity interface required'),
                                   quantity=quantity, flowable=flowable, **kwargs)

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

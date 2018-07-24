from ..implementations import QuantityImplementation


class QdbQuantityImplementation(QuantityImplementation):
    """
    Quantity Interface
    """
    def get_canonical(self, synonym, **kwargs):
        """
        return a quantity by its synonym
        :param synonym:
        :return:
        """
        return self._archive[synonym]  # __getitem__ returns canonical quantity entity

    def synonyms(self, item, **kwargs):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment
        :param item:
        :return: list of strings
        """
        if self._archive.f_index(item) is not None:
            for k in self._archive.f_syns(item):
                yield k
        elif self.get_canonical(item) is not None:
            for k in self._archive.q.syns(item):
                yield k
        else:
            comp = self._archive.c_mgr.find_matching(item, interact=False)
            if comp is not None:
                for k in comp.synonyms:
                    yield k

    def flowables(self, quantity=None, compartment=None, **kwargs):
        """
        Return a list of flowable strings. Use quantity and compartment parameters to narrow the result
        set to those characterized by a specific quantity, those exchanged with a specific compartment, or both
        :param quantity:
        :param compartment: not implemented
        :return: list of pairs: CAS number, name
        """
        if quantity is not None:
            for k in self._archive.flows_for_quantity(quantity):
                yield self._archive.f_cas(k), self._archive.f_name(k)
        else:
            for cas, name in self._archive.flowables():
                yield cas, name

    def compartments(self, quantity=None, flowable=None, **kwargs):
        """
        Return a list of compartment strings. Use quantity and flowable parameters to narrow the result
        set to those characterized for a specific quantity, those with a specific flowable, or both
        :param quantity:
        :param flowable:
        :return: list of strings
        """
        pass

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
        if flowable is not None:
            flowable = self._archive.f_index(flowable)
        if compartment is not None:
            compartment = self._archive.c_mgr.find_matching(compartment)
        for cf in self._archive.cfs_for_quantity(quantity, compartment=compartment):
            if flowable is not None:
                if self._archive.f_index(cf.flow['Name']) != flowable:
                    continue
            yield cf

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
        return self._archive.convert(flowable=flowable, compartment=compartment, reference=ref_quantity,
                                     query=query_quantity, locale=locale, **kwargs)

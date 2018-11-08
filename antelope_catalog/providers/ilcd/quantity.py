from .ilcd import uuid_regex

from lcatools.implementations import QuantityImplementation


class IlcdQuantityImplementation(QuantityImplementation):
    def get_canonical(self, quantity, **kwargs):
        """
        Retrieve a canonical quantity from a qdb
        :param quantity: external_id of quantity
        :return: quantity entity
        """
        u = uuid_regex.search(quantity).groups()[0]
        return self._archive.load_lcia_method(u, load_all_flows=False)

    def synonyms(self, item, **kwargs):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment

        :param item:
        :return: list of strings
        """
        pass

    def flowables(self, quantity=None, compartment=None, **kwargs):
        """
        Return a list of flowable strings. Use quantity and compartment parameters to narrow the result
        set to those characterized by a specific quantity, those exchanged with a specific compartment, or both
        :param quantity:
        :param compartment: filter by compartment not implemented
        :return: list of pairs: CAS number, name
        """
        fbs = set()
        if quantity is not None:
            for factor in self._archive.generate_factors(quantity):
                fb = factor.flow['CasNumber'], factor.flow['Name']
                if fb not in fbs:
                    fbs.add(fb)
                    yield fb
        else:
            for f in self._archive.entities_by_type('flow'):
                fb = f['CasNumber'], f['Name']
                if fb not in fbs:
                    fbs.add(fb)
                    yield fb

    def compartments(self, quantity=None, flowable=None, **kwargs):
        """
        Return a list of compartment strings. Use quantity and flowable parameters to narrow the result
        set to those characterized for a specific quantity, those with a specific flowable, or both
        :param quantity:
        :param flowable:
        :return: list of strings
        """
        pass

    def factors(self, quantity, flowable=None, context=None, **kwargs):
        """
        Return characterization factors for the given quantity, subject to optional flowable and compartment
        filter constraints. This is ill-defined because the reference unit is not explicitly reported in current
        serialization for characterizations (it is implicit in the flow)-- but it can be added to a web service layer.
        :param quantity:
        :param flowable:
        :param context: not implemented
        :return:
        """
        for factor in self._archive.generate_factors(quantity):
            if flowable is not None:
                if factor.flow['Name'] != flowable and factor.flow['CasNumber'] != flowable:
                    continue
            yield factor

    def quantity_relation(self, flowable, ref_quantity, query_quantity, context, locale='GLO', **kwargs):
        """
        Return a single number that converts the a unit of the reference quantity into the query quantity for the
        given flowable, compartment, and locale (default 'GLO').  If no locale is found, this would be a great place
        to run a spatial best-match algorithm.
        :param flowable:
        :param ref_quantity:
        :param query_quantity:
        :param context:
        :param locale:
        :return:
        """
        pass

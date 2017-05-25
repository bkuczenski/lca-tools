from lcatools.catalog.basic import BasicInterface


class QuantityInterface(BasicInterface):
    """
    Unlike entity, foreground, and background interfaces, a quantity interface does not require a static archive
    (since there are no terminations to index) or a background manager (since there are no processes)

    The quantity interface requires a compartment manager to function.

    The interface works normally on normally-constituted archives, but also allows the archives to override the default
    implementations (which require load_all)
    """
    def __init__(self, archive, qdb, **kwargs):
        super(QuantityInterface, self).__init__(archive, **kwargs)
        self._qdb = qdb
        self._cm = qdb.c_mgr
        self._compartments = dict()

    def quantities(self, **kwargs):
        for q in self._archive.quantities(**kwargs):
            yield self.make_ref(self._qdb.get_canonical_quantity(q))

    def lcia_methods(self, **kwargs):
        for l in self._archive.lcia_methods(**kwargs):
            yield self.make_ref(self._qdb.get_canonical_quantity(l))

    def _check_compartment(self, string):
        if string is None:
            return None
        if string in self._compartments:
            return self._compartments[string]
        c = self._cm.find_matching(string)
        self._compartments[string] = c
        return c

    def get_quantity(self, quantity):
        """
        Retrieve a canonical quantity from a qdb
        :param quantity: external_id of quantity
        :return: quantity entity
        """
        if hasattr(self._archive, 'get_quantity'):
            return self.make_ref(self._archive.get_quantity(quantity))
        return self.make_ref(self._archive[quantity])

    def synonyms(self, item):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment
        :param item:
        :return: list of strings
        """
        if hasattr(self._archive, 'synonyms'):
            return self._archive.synonyms(item)
        raise NotImplemented

    def flowables(self, quantity=None, compartment=None):
        """
        Return a list of flowable strings. Use quantity and compartment parameters to narrow the result
        set to those characterized by a specific quantity, those exchanged with a specific compartment, or both
        :param quantity:
        :param compartment:
        :return: list of pairs: CAS number, name
        """
        if hasattr(self._archive, 'flowables'):
            for n in self._archive.flowables(quantity=quantity, compartment=compartment):
                yield n
        else:
            compartment = self._check_compartment(compartment)
            if quantity is not None:
                quantity = self._archive[quantity]
            fb = set()
            for f in self._archive.flows():
                if compartment is not None:
                    if self._check_compartment(f['Compartment']) is not compartment:
                        continue
                if quantity is not None:
                    if not f.has_characterization(quantity):
                        continue
                fb.add((f['CasNumber'], f['Name']))
            for n in sorted(list(fb), key=lambda x: x[1]):
                yield n

    def compartments(self, quantity=None, flowable=None):
        """
        Return a list of compartment strings. Use quantity and flowable parameters to narrow the result
        set to those characterized for a specific quantity, those with a specific flowable, or both
        :param quantity:
        :param flowable:
        :return: list of strings
        """
        if hasattr(self._archive, 'compartments'):
            for n in self._archive.compartments(quantity=quantity, flowable=flowable):
                yield n
        else:
            comps = set()
            for f in self._archive.flows():
                comps.add(self._check_compartment(f['Compartment']))
            for n in comps:
                yield str(n)

    def factors(self, quantity, flowable=None, compartment=None):
        """
        Return characterization factors for the given quantity, subject to optional flowable and compartment
        filter constraints. This is ill-defined because the reference unit is not explicitly reported in current
        serialization for characterizations (it is implicit in the flow)-- but it can be added to a web service layer.
        :param quantity:
        :param flowable:
        :param compartment:
        :return:
        """
        if hasattr(self._archive, 'factors'):
            for n in self._archive.factors(quantity, flowable=flowable, compartment=compartment):
                yield n
        else:
            flowable = flowable.lower()
            compartment = self._cm.find_matching(compartment)
            for f in self._archive.flows():
                if not f.has_characterization(quantity):
                    continue
                if flowable is not None:
                    if f['Name'].lower() != flowable:
                        continue
                if compartment is not None:
                    if self._cm.find_matching(f['Compartment']) != compartment:
                        continue
                yield f.factor(quantity)

    def quantity_relation(self, ref_quantity, flowable, compartment, query_quantity, locale='GLO'):
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
        if hasattr(self._archive, 'quantity_relation'):
            return self._archive.quantity_relation(ref_quantity, flowable, compartment, query_quantity, locale=locale)
        raise NotImplemented  # must be overridden

from lcatools.providers.ilcd import IlcdArchive, find_ns, find_common, find_tag, typeDirs, get_flow_ref, uuid_regex, \
    dtype_from_nsmap
from lcatools.entities import LcEntity, LcQuantity


def get_reference_quantity(q, ns=None):
    ref_to_ref = find_tag(q, 'referenceQuantity', ns=ns)[0]
    ug_uuid = ref_to_ref.attrib['refObjectId']
    ug_uri = ref_to_ref.attrib['uri']
    return ug_uuid, ug_uri


def get_cf_value(exch, ns=None):
    try:
        v = float(find_tag(exch, 'resultingAmount', ns=ns)[0])
    except ValueError:
        v = None
    return v


class IlcdLcia(IlcdArchive):
    """
    Slightly extends the IlcdArchive with a set of functions for loading LCIA factors and adding them as
    quantities + charaterizations
    """

    def _create_lcia_quantity(self, o, ns):

        u = str(find_common(o, 'UUID')[0])
        try_q = self[u]
        if try_q is not None:
            lcia = try_q
        else:
            n = str(find_common(o, 'name')[0])

            c = str(find_common(o, 'generalComment')[0])

            m = '; '.join([str(x) for x in find_tag(o, 'methodology', ns=ns)])
            ic = '; '.join([str(x) for x in find_tag(o, 'impactCategory', ns=ns)])
            ii = '; '.join([str(x) for x in find_tag(o, 'impactIndicator', ns=ns)])

            ry = str(find_tag(o, 'referenceYear', ns=ns)[0])
            dur = str(find_tag(o, 'duration', ns=ns)[0])

            r_uuid, r_uri = get_reference_quantity(o, ns=ns)
            rq = self._check_or_retrieve_child(r_uuid, r_uri)

            lcia = LcQuantity(u, Name=n, Comment=c, Method=m, Category=ic, Indicator=ii, ReferenceYear=ry, Duration=dur)
            lcia.set_external_ref('%s/%s' % (typeDirs['LCIAMethod'], u))
            lcia.reference_entity = rq.reference_entity

            self.add(lcia)
        return lcia

    def _fetch(self, term, dtype=None, version=None, **kwargs):
        o = super(IlcdLcia, self)._fetch(term, dtype=dtype, version=version, **kwargs)
        if isinstance(o, LcEntity):
            return o
        if dtype is None:
            dtype = dtype_from_nsmap(o.nsmap)
        if dtype == 'LCIAMethod':
            ns = find_ns(o.nsmap, 'LCIAMethod')
            return self._create_lcia_quantity(o, ns)
        return o

    def _load_factor(self, ns, factor, lcia, load_all_flows=False):
        f_uuid, f_uri, f_dir = get_flow_ref(factor, ns=ns)
        if self[f_uuid] is None:
            if not load_all_flows:
                # don't bother loading factors for flows that don't exist
                return
        cf = float(find_tag(factor, 'meanValue', ns=ns)[0])
        loc = str(find_tag(factor, 'location', ns=ns)[0])
        if loc == '':
            loc = None
        flow = self._check_or_retrieve_child(f_uuid, f_uri)
        if not flow.has_characterization(lcia):
            # TODO: adjust CF for different reference units!!! do this when a live one is found
            flow.add_characterization(lcia, value=cf, location=loc)
        return flow.factor(lcia)

    def load_lcia_method(self, u, version=None, load_all_flows=False):
        """

        :param u:
        :param version:
        :param load_all_flows: [False] If False, load CFs only for already-loaded flows. If True, load all flows
        :return:
        """
        o = self._get_objectified_entity(self._path_from_parts('LCIAMethod', u, version=version))
        ns = find_ns(o.nsmap, 'LCIAMethod')

        lcia = self._create_lcia_quantity(o, ns)

        if load_all_flows is not None:
            for factor in o['characterisationFactors'].getchildren():  # British spelling! brits aren't even IN the EU anymore
                self._load_factor(ns, factor, lcia, load_all_flows=load_all_flows)
        return lcia

    def load_lcia(self, **kwargs):
        for f in self.list_objects('LCIAMethod'):
            u = uuid_regex.search(f).groups()[0]
            if self._get_entity(u) is not None:
                continue

            self.load_lcia_method(u, **kwargs)
        self.check_counter()

    '''
    Quantity Interface
    '''
    def get_quantity(self, quantity):
        """
        Retrieve a canonical quantity from a qdb
        :param quantity: external_id of quantity
        :return: quantity entity
        """
        u = uuid_regex.search(quantity).groups()[0]
        return self.load_lcia_method(u, load_all_flows=False)

    def _generate_factors(self, quantity):
        o = self._get_objectified_entity(self._path_from_ref(quantity))
        ns = find_ns(o.nsmap, 'LCIAMethod')

        lcia = self._create_lcia_quantity(o, ns)

        for factor in o['characterisationFactors'].getchildren():
            yield self._load_factor(ns, factor, lcia, load_all_flows=True)

    def synonyms(self, item):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment

        :param item:
        :return: list of strings
        """
        pass

    def flowables(self, quantity=None, compartment=None):
        """
        Return a list of flowable strings. Use quantity and compartment parameters to narrow the result
        set to those characterized by a specific quantity, those exchanged with a specific compartment, or both
        :param quantity:
        :param compartment: filter by compartment not implemented
        :return: list of pairs: CAS number, name
        """
        fbs = set()
        if quantity is not None:
            for factor in self._generate_factors(quantity):
                fb = factor.flow['CasNumber'], factor.flow['Name']
                if fb not in fbs:
                    fbs.add(fb)
                    yield fb
        else:
            for f in self.flows():
                fb = f['CasNumber'], f['Name']
                if fb not in fbs:
                    fbs.add(fb)
                    yield fb

    def compartments(self, quantity=None, flowable=None):
        """
        Return a list of compartment strings. Use quantity and flowable parameters to narrow the result
        set to those characterized for a specific quantity, those with a specific flowable, or both
        :param quantity:
        :param flowable:
        :return: list of strings
        """
        pass

    def factors(self, quantity, flowable=None, compartment=None):
        """
        Return characterization factors for the given quantity, subject to optional flowable and compartment
        filter constraints. This is ill-defined because the reference unit is not explicitly reported in current
        serialization for characterizations (it is implicit in the flow)-- but it can be added to a web service layer.
        :param quantity:
        :param flowable:
        :param compartment: not implemented
        :return:
        """
        for factor in self._generate_factors(quantity):
            if flowable is not None:
                if factor.flow['Name'] != flowable and factor.flow['CasNumber'] != flowable:
                    continue
            yield factor

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
        pass

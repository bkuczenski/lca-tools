from .ilcd import IlcdArchive, typeDirs, get_flow_ref, uuid_regex, dtype_from_nsmap
from lcatools.providers.xml_widgets import *
from lcatools.entities import LcEntity, LcQuantity, LcUnit
from .quantity import IlcdQuantityImplementation
from .index import IlcdIndexImplementation


def get_cf_value(exch, ns=None):
    try:
        v = float(find_tag(exch, 'resultingAmount', ns=ns))
    except ValueError:
        v = None
    return v


class IlcdLcia(IlcdArchive):
    """
    Slightly extends the IlcdArchive with a set of functions for loading LCIA factors and adding them as
    quantities + charaterizations
    """
    def make_interface(self, iface, privacy=None):
        if iface == 'index':
            return IlcdIndexImplementation(self, privacy=privacy)
        elif iface == 'quantity':
            return IlcdQuantityImplementation(self, privacy=privacy)
        else:
            return super(IlcdLcia, self).make_interface(iface, privacy=privacy)

    def _make_reference_unit(self, o, ns=None):
        """
        This is a bit of a hack. ILCD has distinct LciaMethod objects and FlowProperty objects.  The LCIA Method lists
        a FlowProperty as its reference quantity (like "mass C2H4 equivalents"), and then the flow property lists a
        reference unit (like "mass").  This is a problem for us because we consider the LciaMethod to BE a quantity,
        and so we want the FlowProperty to present as a unit.
        :param o:
        :param ns:
        :return:
        """
        ref_to_ref = find_tag(o, 'referenceQuantity', ns=ns)
        r_uuid = ref_to_ref.attrib['refObjectId']
        r_uri = ref_to_ref.attrib['uri']
        return self._check_or_retrieve_child(r_uuid, r_uri)

    def _create_lcia_quantity(self, o, ns):

        u = str(find_common(o, 'UUID'))
        try_q = self[u]
        if try_q is not None:
            lcia = try_q
        else:
            n = str(find_common(o, 'name'))

            c = str(find_common(o, 'generalComment'))

            m = '; '.join([str(x) for x in find_tags(o, 'methodology', ns=ns)])
            ic = '; '.join([str(x) for x in find_tags(o, 'impactCategory', ns=ns)])
            ii = '; '.join([str(x) for x in find_tags(o, 'impactIndicator', ns=ns)])

            ry = str(find_tag(o, 'referenceYear', ns=ns))
            dur = str(find_tag(o, 'duration', ns=ns))

            rq = self._make_reference_unit(o, ns=ns)

            lcia = LcQuantity(u, Name=n, Comment=c, Method=m, Category=ic, Indicator=ii, ReferenceYear=ry,
                              Duration=dur, UnitConversion=rq['UnitConversion'])
            lcia.set_external_ref('%s/%s' % (typeDirs['LCIAMethod'], u))
            lcia.reference_entity = LcUnit('%s %s' % (rq.unit(), rq['Name']), unit_uuid=rq.uuid)

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
        cf = float(find_tag(factor, 'meanValue', ns=ns))
        loc = str(find_tag(factor, 'location', ns=ns))
        if loc == '':
            loc = None
        flow = self._check_or_retrieve_child(f_uuid, f_uri)
        if not flow.has_characterization(lcia, location=loc):
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
            if self._get_entity(u) is not None:  # we want to look strictly locally
                continue

            self.load_lcia_method(u, **kwargs)

    def generate_factors(self, quantity):
        o = self._get_objectified_entity(self._path_from_ref(quantity))
        ns = find_ns(o.nsmap, 'LCIAMethod')

        lcia = self._create_lcia_quantity(o, ns)

        for factor in o['characterisationFactors'].getchildren():
            yield self._load_factor(ns, factor, lcia, load_all_flows=True)

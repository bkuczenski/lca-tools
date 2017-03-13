from lcatools.providers.ilcd import IlcdArchive, find_ns, find_common, find_tag, typeDirs, get_flow_ref, uuid_regex
from lcatools.entities import LcQuantity


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

    def _create_lcia_quantity(self, o, load_all_flows=False):
        ns = find_ns(o.nsmap, 'LCIAMethod')

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

        for factor in o['characterisationFactors'].getchildren():  # British spelling! brits aren't even IN the EU anymore
            f_uuid, f_uri, f_dir = get_flow_ref(factor, ns=ns)
            if self[f_uuid] is None:
                if not load_all_flows:
                    # don't bother loading factors for flows that don't exist
                    continue
            cf = float(find_tag(factor, 'meanValue', ns=ns)[0])
            loc = str(find_tag(factor, 'location', ns=ns)[0])
            if loc == '':
                loc = None
            flow = self._check_or_retrieve_child(f_uuid, f_uri)
            # TODO: adjust CF for different reference units!!! do this when a live one is found
            flow.add_characterization(lcia, value=cf, location=loc)

    def load_lcia_method(self, u, version=None, load_all_flows=False):
        o = self._get_objectified_entity(self._path_from_parts('LCIAMethod', u, version=version))

        self._create_lcia_quantity(o, load_all_flows=load_all_flows)

    def load_lcia(self, **kwargs):
        for f in self.list_objects('LCIAMethod'):
            u = uuid_regex.search(f).groups()[0]
            if self._get_entity(u) is not None:
                continue

            self.load_lcia_method(u, **kwargs)
        self.check_counter()

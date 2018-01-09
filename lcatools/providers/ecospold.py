"""
With inspiration from bw2data/io/import_ecospold2.py and affiliated files

The purpose of this archive (and ILCD alike) is to provide a common interface, a la the semantic
web paper, to a collection of process data, to wit:
 - list of process metadata:
   UUID | Name | Spatial | Temporal | Ref Product | Comment |

"""

from __future__ import print_function, unicode_literals

import eight
import re

from lxml import objectify
# from lxml.etree import tostring

from lcatools.providers.base import NsUuidArchive
from lcatools.providers.archive import Archive
from lcatools.entities import LcQuantity, LcFlow, LcProcess
# from lcatools.exchanges import DirectionlessExchangeError

from lcatools.providers import tail
from lcatools.providers.xml_widgets import find_tag
from lcatools.interact import parse_math


"""
Used to install conversion factors between different flow reference units.  Satisfies the requirement:
conversion_dict[(k1, k2)] = f implies 1 k1 = f k2

Note: one conversion factor that was omitted is required by the 2015-era US LCI database: The process "Biodegradable
loose fill [RNA]" requires "natural gas, combusted in equipment" measured in kWh, but there is no conversion from m3 of
natural gas combusted to kWh.  My best guess is on a GCV basis of fuel input, so ~40 MJ/m3 = 11.111 kWh / m3

This value must be entered by hand when the USLCI database is loaded.
"""
conversion_dict = {
    ('Bq', 'kBq'): .001,
    ('t', 'kg'): 1000,
    ('kg', 't'): .001,
    ('kg*km', 't*km'): .001,
    ('m2', 'ha'): .0001,
    ('ha', 'm2'): 10000,
    ('sh tn', 't'): 0.907185,
    ('sh tn', 'kg'): 907.185,
    ('MJ', 'kWh'): 0.2777777,
    ('kWh', 'MJ'): 3.6,
    ('MJ', 'btu'): 947.817,
    ('l', 'm3'): .001,
    ('m3', 'l'): 1000
}


def not_none(x):
    return x if x is not None else ''


class EcospoldVersionError(Exception):
    pass


class EcospoldV1Archive(NsUuidArchive):
    """
    Create an Ecospold Archive object from a path.  By default, assumes the path points to a literal
    .7z file, of the type that one can download from the ecoinvent website.  Creates an accessor for
    files in that archive and allows a user to
    """

    nsmap = 'http://www.EcoInvent.org/EcoSpold01'  # only valid for v1 ecospold files
    spold_version = tail.search(nsmap).groups()[0]

    def __init__(self, source, prefix=None, **kwargs):
        """
        Just instantiates the parent class.
        :param source: physical data source
        :param prefix: difference between the internal path (ref) and the ILCD base
        :return:
        """
        super(EcospoldV1Archive, self).__init__(source, **kwargs)
        if prefix is not None:
            self._serialize_dict['prefix'] = prefix
        self._q_dict = dict()
        self._archive = Archive(self.source, internal_prefix=prefix)

    def list_datasets(self):
        assert self._archive.remote is False, "Cannot list objects for remote archives"
        for f in self._archive.listfiles():
            yield f

    def _fetch_filename(self, filename):
        return self._archive.readfile(filename)

    def _get_objectified_entity(self, filename):
        o = objectify.fromstring(self._archive.readfile(filename))
        if o.nsmap[None] != self.nsmap:
            raise EcospoldVersionError('This class is for EcoSpold v%s only!' % self.nsmap[-2:])
        return o

    def _create_quantity(self, unitstring):
        """
        In ecospold v1, quantities are only units, defined by string
        :param unitstring:
        :return:
        """
        if unitstring in self._q_dict:
            q = self._q_dict[unitstring]
        else:
            ref_unit, _ = self._create_unit(unitstring)
            uid = self._key_to_id(unitstring)

            q = LcQuantity(uid, Name='EcoSpold Quantity %s' % unitstring,
                           ReferenceUnit=ref_unit, Comment=self.spold_version)
            q.set_external_ref(unitstring)
            try_q = self._check_upstream(self._upstream_key(q))
            if try_q is None:
                self.add(q)
            else:
                q = try_q
            self._q_dict[unitstring] = q

        return q

    def _create_flow(self, exch):
        """
        An ecospold01 exchange is really just a long attribute list, plus an inputGroup or outputGroup (ignored here)
        :param exch:
        :return:
        """
        number = int(exch.get('number'))
        uid = self._key_to_id(number)
        try_f = self[uid]
        if try_f is not None:
            f = try_f
            assert f.entity_type == 'flow', "Expected flow, found %s" % f.entity_type

        else:
            # generate flow
            n = exch.get("name")
            q = self._create_quantity(exch.get("unit"))
            c = not_none(exch.get("generalComment"))
            cas = not_none(exch.get("CASNumber"))
            cat = [exch.get('category'), exch.get('subCategory')]

            f = LcFlow(uid, Name=n, CasNumber=cas, Comment=c, Compartment=cat)
            f.add_characterization(q, reference=True)
            f.set_external_ref(number)
            self.add(f)

        if exch.get("unit") != f.unit():
            local_q = self._create_quantity(exch.get("unit"))
            if not f.has_characterization(local_q):
                if (f.unit(), local_q.unit()) not in conversion_dict:
                    print('Flow %s needs characterization for unit %s' % (f, local_q))
                    val = parse_math(input('Enter conversion factor 1 %s = x %s' % (f.unit(), local_q)))
                else:
                    val = conversion_dict[(f.unit(), local_q.unit())]
                f.add_characterization(local_q, value=val)
        return f

    def _create_process(self, filename):
        """
        Extract dataset object from XML file
        :param filename:
        :return:
        """
        o = self._get_objectified_entity(filename)

        rf = set()  # reference flows
        flowlist = []

        for exch in o.dataset.flowData.getchildren():
            f = self._create_flow(exch)
            if hasattr(exch, 'outputGroup'):
                d = 'Output'
                if exch.outputGroup == 0 or exch.outputGroup == 2:
                    rf.add(f)
            elif hasattr(exch, 'OutputGroup'):
                d = 'Output'
                if exch.OutputGroup == 0 or exch.OutputGroup == 2:
                    rf.add(f)
            elif hasattr(exch, 'inputGroup'):
                d = 'Input'
            elif hasattr(exch, 'InputGroup'):
                d = 'Input'
            else:
                print('Abandoning directionless exchange for flow %s' % f)
                continue
                # raise DirectionlessExchangeError(tostring(exch))
            local_q = self._create_quantity(exch.get("unit"))
            v = float(exch.get('meanValue'))  # returns none if missing
            if local_q is not f.reference_entity:
                v = v / f.cf(local_q)
            flowlist.append((f, d, v))

        p_meta = o.dataset.metaInformation.processInformation
        n = p_meta.referenceFunction.get('name')

        u = self._key_to_id(n)

        try_p = self[u]
        if try_p is not None:
            p = try_p
            assert p.entity_type == 'process', "Expected process, found %s" % p.entity_type

        else:
            # create new process
            g = p_meta.geography.get('location')
            stt = {'begin': str(find_tag(p_meta, 'startDate')), 'end': str(find_tag(p_meta, 'endDate'))}

            c = p_meta.referenceFunction.get('generalComment')

            cls = [p_meta.referenceFunction.get('category'), p_meta.referenceFunction.get('subCategory')]
            p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                          Classifications=cls)
            p.set_external_ref(n)

            for flow, f_dir, val in flowlist:
                self._print('Exch %s [%s] (%g)' % (flow, f_dir, val))
                p.add_exchange(flow, f_dir, reference=None, value=val, add_dups=True)
            for ref in rf:
                p.add_reference(ref, 'Output')

            self.add(p)

        return p

    def _fetch(self, key, **kwargs):
        """
        If the argument is an external reference for a process, try loading the file
        :param key:
        :param kwargs:
        :return:
        """
        try:
            self._create_process(key + '.xml')
            return self[key]
        except KeyError:
            print('No way to fetch that key. try load_all()')

        return None

    def _load_all(self):
        """
        USLCI Hack: the flow "natural gas, combusted in equipment" is used in the database with three different
        reference units (kWh, l, and m3).  We want the reference unit to be m3.  So.. we load the process with
        that characterization first.
        :return:
        """
        for x in self.list_datasets():
            if bool(re.search('Natural gas, combusted in industrial equipment', x)):
                self.retrieve_or_fetch_entity('Natural gas, combusted in industrial equipment')
        for k in self.list_datasets():
            self._create_process(k)
        self.check_counter('quantity')
        self.check_counter('flow')
        self.check_counter('process')

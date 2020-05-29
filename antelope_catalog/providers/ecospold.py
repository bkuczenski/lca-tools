"""
With inspiration from bw2data/io/import_ecospold2.py and affiliated files

The purpose of this archive (and ILCD alike) is to provide a common interface, a la the semantic
web paper, to a collection of process data, to wit:
 - list of process metadata:
   UUID | Name | Spatial | Temporal | Ref Product | Comment |

"""

from __future__ import print_function, unicode_literals

import uuid
import re
import os

from lxml import objectify
# from lxml.etree import tostring

from lcatools.archives import LcArchive
from lcatools.entities import LcQuantity, LcFlow, LcProcess
from lcatools.interact import parse_math
# from lcatools.exchanges import DirectionlessExchangeError

from .file_store import FileStore
from .xml_widgets import find_tag

tail = re.compile('/([^/]+)$')

"""
Used to install conversion factors between different flow reference units.  Satisfies the requirement:
conversion_dict[(k1, k2)] = f implies 1 k1 = f k2

Note: one conversion factor that was omitted is required by the 2015-era US LCI database: The process "Biodegradable
loose fill [RNA]" requires "natural gas, combusted in equipment" measured in kWh, but there is no conversion from m3 of
natural gas combusted to kWh.  My best guess is on a GCV basis of fuel input, so ~40 MJ/m3 = 11.111 kWh / m3

This value must be entered by hand when the USLCI database is loaded.
"""
conversion_dict = {
    ('kBq', 'Bq'): 1000,
    ('t', 'kg'): 1000,
    ('kg*km', 't*km'): .001,
    ('ha', 'm2'): 10000,
    ('sh tn', 't'): 0.907185,
    ('sh tn', 'kg'): 907.185,
    ('kWh', 'MJ'): 3.6,
    ('MJ', 'btu'): 947.817,
    ('m3', 'l'): 1000
}

def apply_conversion(local_q, f):
    if (f.unit, local_q.unit) in conversion_dict:
        val = conversion_dict[(f.unit, local_q.unit)]
        f.reference_entity['UnitConversion'][local_q.unit] = val
        local_q['UnitConversion'][f.unit] = 1.0 / val
        return local_q.cf(f) == val
    elif (local_q.unit, f.unit) in conversion_dict:
        val = conversion_dict[(local_q.unit, f.unit)]
        local_q['UnitConversion'][f.unit] = val
        f.reference_entity['UnitConversion'][local_q.unit] = 1.0 / val
        return local_q.cf(f) == 1.0 / val
    return False


def not_none(x):
    return x if x is not None else ''


class EcospoldVersionError(Exception):
    pass


class EcospoldV1Archive(LcArchive):
    """
    Create an Ecospold Archive object from a path.  By default, assumes the path points to a literal
    .7z file, of the type that one can download from the ecoinvent website.  Creates an accessor for
    files in that archive and allows a user to
    """

    nsmap = 'http://www.EcoInvent.org/EcoSpold01'  # only valid for v1 ecospold files
    spold_version = tail.search(nsmap).groups()[0]

    def __init__(self, source, prefix=None, ns_uuid=None, **kwargs):
        """
        Just instantiates the parent class.
        :param source: physical data source
        :param prefix: difference between the internal path (ref) and the ILCD base
        :param ns_uuid: NS UUID not allowed for ecospold ve
        :return:
        """
        if ns_uuid is None:
            ns_uuid = uuid.uuid4()
        super(EcospoldV1Archive, self).__init__(source, ns_uuid=ns_uuid, **kwargs)
        if prefix is not None:
            self._serialize_dict['prefix'] = prefix
        self._q_dict = dict()
        self._archive = FileStore(self.source, internal_prefix=prefix)

    def list_datasets(self):
        assert self._archive.remote is False, "Cannot list objects for remote archives"
        for f in self._archive.listfiles():
            yield f

    def _fetch_filename(self, filename):
        """
        In USLCI (honestly, I've never seen another ecospoldv1 archive so I can't generalize), activities  whose
        names contain '<' or '>' characters have those characters stripped from the filenames, so we try that here as
        a workaround.
        :param filename:
        :return:
        """
        try:
            s = self._archive.readfile(filename)
        except KeyError:
            trim_lg = ''.join(k for k in filename if k not in '<>')
            s = self._archive.readfile(trim_lg)
        return s

    def _get_objectified_entity(self, filename):
        o = objectify.fromstring(self._fetch_filename(filename))
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

            q = LcQuantity(unitstring, Name='EcoSpold Quantity %s' % unitstring,
                           ReferenceUnit=ref_unit, Comment=self.spold_version)
            self.add(q)

            self._q_dict[unitstring] = q

        return q

    def _create_flow(self, exch):
        """
        An ecospold01 exchange is really just a long attribute list, plus an inputGroup or outputGroup (ignored here)
        :param exch:
        :return:
        """
        number = str(exch.get('number'))
        try_f = self[number]
        if try_f is not None:
            f = try_f
            assert f.entity_type == 'flow', "Expected flow, found %s" % f.entity_type

        else:
            # generate flow
            n = exch.get("name")
            q = self._create_quantity(exch.get("unit"))
            c = not_none(exch.get("generalComment"))
            cas = not_none(exch.get("CASNumber"))
            cat = [k for k in filter(None, (exch.get('category'), exch.get('subCategory')))]

            f = LcFlow(number, Name=n, CasNumber=cas, Comment=c, Compartment=cat, ReferenceQuantity=q)
            self.add(f)

        if exch.get("unit") != f.unit:
            local_q = self._create_quantity(exch.get("unit"))
            if local_q.cf(f) == 0.0:
                if not apply_conversion(local_q, f):
                    print('Flow %s needs characterization for unit %s' % (f, local_q))
                    val = parse_math(input('Enter conversion factor 1 %s = x %s' % (f.unit, local_q)))
                    self.tm.add_characterization(f.link, f.reference_entity, local_q, val, context=f.context,
                                                 origin=self.ref)
        return f

    def _extract_exchanges(self, o):
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
                v = v / local_q.cf(f)
            c = exch.get('generalComment')
            flowlist.append((f, d, v, c))
        return rf, flowlist

    def _create_process(self, ext_ref):
        """
        Extract dataset object from XML file
        :param ext_ref:
        :return:
        """
        try_p = self[ext_ref]
        if try_p is not None:
            p = try_p
            assert p.entity_type == 'process', "Expected process, found %s" % p.entity_type
            return p
        o = self._get_objectified_entity(ext_ref + '.xml')

        p_meta = o.dataset.metaInformation.processInformation
        n = p_meta.referenceFunction.get('name')

        # create new process
        g = p_meta.geography.get('location')
        stt = {'begin': str(find_tag(p_meta, 'startDate')), 'end': str(find_tag(p_meta, 'endDate'))}

        c = p_meta.referenceFunction.get('generalComment')

        cls = [p_meta.referenceFunction.get('category'), p_meta.referenceFunction.get('subCategory')]
        p = LcProcess(ext_ref, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                      Classifications=cls)

        rf, flowlist = self._extract_exchanges(o)

        for flow, f_dir, val, cmt in flowlist:
            if flow in rf and f_dir == 'Output':
                term = None
            else:
                term = self.tm[flow.context]
            self._print('Exch %s [%s] (%g)' % (flow, f_dir, val))
            x = p.add_exchange(flow, f_dir, reference=None, value=val, termination=term, add_dups=True)
            if cmt is not None:
                x.comment = cmt

        for ref in rf:
            p.set_reference(ref, 'Output')

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
            self._create_process(key)
            return self[key]
        except KeyError:
            raise KeyError('No way to fetch key "%s". try load_all()' % key)

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
            self._create_process(os.path.splitext(k)[0])
        self.check_counter('quantity')
        self.check_counter('flow')
        self.check_counter('process')

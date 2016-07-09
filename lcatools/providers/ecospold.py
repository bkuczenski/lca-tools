"""
With inspiration from bw2data/io/import_ecospold2.py and affiliated files

The purpose of this archive (and ILCD alike) is to provide a common interface, a la the semantic
web paper, to a collection of process data, to wit:
 - list of process metadata:
   UUID | Name | Spatial | Temporal | Ref Product | Comment |

"""

from __future__ import print_function, unicode_literals

import six

import os

from lxml import objectify

from lcatools.providers.base import NsUuidArchive
from lcatools.providers.archive import Archive
from lcatools.entities import LcQuantity, LcFlow, LcProcess
from lcatools.exchanges import DirectionlessExchangeError

from lcatools.providers import tail
from lcatools.providers.xml_widgets import find_tag


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

    def __init__(self, ref, prefix=None, **kwargs):
        """
        Just instantiates the parent class.
        :param ref: just a reference
        :param prefix: difference between the internal path (ref) and the ILCD base
        :return:
        """
        super(EcospoldV1Archive, self).__init__(ref, **kwargs)
        self.internal_prefix = prefix
        self._archive = Archive(self.ref)

    def _build_prefix(self):
        path = ''
        if self.internal_prefix is not None:
            path = os.path.join(self.internal_prefix, path)
        return path

    def list_datasets(self):
        assert self._archive.remote is False, "Cannot list objects for remote archives"
        return self._archive.listfiles(in_prefix=self._build_prefix())

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
        try_q = self.quantity_with_unit(unitstring)
        if try_q is None:
            ref_unit, _ = self._create_unit(unitstring)
            uid = self._key_to_id(unitstring)

            q = LcQuantity(uid, Name='EcoSpold Quantity %s' % unitstring,
                           ReferenceUnit=ref_unit, Comment=self.spold_version)
            q.set_external_ref(unitstring)
            self.add(q)
        else:
            q = try_q

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

        return f

    def _create_process(self, filename):
        """
        Extract dataset object from XML file
        :param filename:
        :return:
        """
        o = self._get_objectified_entity(filename)

        rf = None  # reference flow
        flowlist = []

        for exch in o.dataset.flowData.getchildren():
            f = self._create_flow(exch)
            if hasattr(exch, 'outputGroup'):
                d = 'Output'
                if exch.outputGroup == 0:
                    assert rf is None, "Multiple reference flows found!"
                    rf = f
            elif hasattr(exch, 'inputGroup'):
                d = 'Input'
            else:
                raise DirectionlessExchangeError
            v = exch.get('meanValue')  # returns none if missing
            flowlist.append((f, d, float(v)))

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
            stt = {'begin': str(find_tag(p_meta, 'startDate')[0]), 'end': str(find_tag(p_meta, 'endDate')[0])}

            c = p_meta.referenceFunction.get('generalComment')

            cls = [p_meta.referenceFunction.get('category'), p_meta.referenceFunction.get('subCategory')]
            p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                          Classifications=cls)
            p.set_external_ref(n)

            if rf is None:
                rx = None
            else:
                rx = p.add_reference(rf, 'Output')
            for flow, f_dir, val in flowlist:
                self._print('Exch %s [%s] (%g)' % (flow, f_dir, val))
                p.add_exchange(flow, f_dir, reference=None, value=val, add_dups=True)

            self.add(p)

        return p

    def _fetch(self, uid, **kwargs):
        """
        Nothing to do here-- if it's not found, it needs to be loaded
        :param uid:
        :param kwargs:
        :return:
        """
        print('No way to fetch by UUID. Loading all processes...')
        self.load_all()

    def _load_all(self):
        """
        No need to "fetch" with ecospold v1, since UUIDs are not known in advance.
        Instead, just load all the processes at once.
        :return:
        """
        for k in self.list_datasets():
            self._create_process(k)
        self.check_counter('quantity')
        self.check_counter('flow')
        self.check_counter('process')

    def serialize(self, **kwargs):
        j = super(EcospoldV1Archive, self).serialize(**kwargs)
        if self.internal_prefix is not None:
            j['prefix'] = self.internal_prefix
        return j

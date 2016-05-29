"""
Import ecospold2 files
"""

from __future__ import print_function, unicode_literals

import six

import os
import re

import uuid

from lxml import objectify
from time import time

from lcatools.providers import tail
from lcatools.providers.xml_widgets import *

from lcatools.interfaces import ArchiveInterface, to_uuid
from lcatools.providers.archive import Archive
from lcatools.entities import LcQuantity, LcFlow, LcProcess
from lcatools.exchanges import Exchange, DirectionlessExchangeError

if six.PY2:
    bytes = str
    str = unicode


class EcospoldV2Error(Exception):
    pass


class EcospoldV2Archive(ArchiveInterface):
    """
    class for loading metadata from ecospold v2 files. Now I know ecoinvent supplies a whole ton of supplementary
    information in files that are *outside* the ecospold archives- and that information is going to be IGNORED.
    or loaded separately. But not handled here.
    """

    nsmap = 'http://www.EcoInvent.org/EcoSpold02'  # only valid for v1 ecospold files
    spold_version = tail.search(nsmap).groups()[0]

    def __init__(self, ref, prefix=None, ns_uuid=None, quiet=True):
        """
        Just instantiates the parent class.
        :param ref: just a reference
        :param prefix: difference between the internal path (ref) and the ILCD base
        :return:
        """
        super(EcospoldV2Archive, self).__init__(ref, quiet=quiet)
        self.internal_prefix = prefix
        self._archive = Archive(self.ref)

        # internal namespace UUID for generating keys
        ns_uuid = to_uuid(ns_uuid)
        self._ns_uuid = uuid.uuid4() if ns_uuid is None else ns_uuid

    # no need for key_to_id - keys in ecospold are uuids
    def _prefix(self, filename):
        if self.internal_prefix is not None:
            try:
                filename = os.path.join(self.internal_prefix, filename)
            except TypeError:  # None filename
                filename = self.internal_prefix
        return filename

    def _de_prefix(self, string):
        if self.internal_prefix is None:
            return string
        else:
            return re.sub('^' + os.path.join(self.internal_prefix, ''), '', string)

    def _fetch_filename(self, filename):
        return self._archive.readfile(self._prefix(filename))

    def list_datasets(self, startswith=None):
        assert self._archive.remote is False, "Cannot list objects for remote archives"
        return [self._de_prefix(x) for x in self._archive.listfiles(in_prefix=self._prefix(startswith))]

    def _get_objectified_entity(self, filename):
        try:
            o = objectify.fromstring(self._fetch_filename(filename))
        except ValueError:
            print('failed on :%s:' % filename)
            return None
        if o.nsmap[None] != self.nsmap:
            raise EcospoldV2Error('This class is for EcoSpold v%s only!' % self.nsmap[-2:])
        return o

    def _create_quantity(self, exchange):
        """
        In ecospold v2, quantities are still only units, defined by string.  They do get their own uuids, but only
        as 'properties' of the flows- flows themselves are only measured by unit.
        this code is cc'd from ecospold1
        :param exchange:
        :return:
        """
        unitstring = exchange.unitName.text
        unit_uuid = exchange.attrib['unitId']
        try_q = self[unit_uuid]
        if try_q is None:
            ref_unit, _ = self._create_unit(unitstring)

            q = LcQuantity(unit_uuid, Name='EcoSpold Quantity %s' % unitstring, ReferenceUnit=ref_unit,
                           Comment=self.spold_version)
            self[unit_uuid] = q
        else:
            q = try_q

        return q

    @staticmethod
    def _cls_to_text(i):
        if isinstance(i, objectify.ObjectifiedElement):
            return ' '.join([i.classificationSystem.text, i.classificationValue.text])
        else:
            return ''

    @staticmethod
    def _cat_to_text(i):
        if isinstance(i, objectify.ObjectifiedElement):
            return ' '.join([i.compartment.text, i.subcompartment.text])
        else:
            return ''

    def _create_flow(self, exchange):
        if 'intermediate' in exchange.tag:
            uid = exchange.attrib['intermediateExchangeId']
            cat = [self._cls_to_text(exchange.classification)]
        elif 'elementary' in exchange.tag:
            uid = exchange.attrib['elementaryExchangeId']
            cat = [self._cat_to_text(exchange.compartment)]
        else:
            raise AttributeError('No exchange type found for id %s' % exchange.attrib['id'])

        if self[uid] is not None:
            return self[uid]

        if 'casNumber' in exchange.attrib:
            cas = exchange.attrib['casNumber']
        else:
            cas = None

        q = self._create_quantity(exchange)

        n = exchange.name.text
        c = 'EcoSpold02 Flow'

        f = LcFlow(uid, Name=n, ReferenceQuantity=q, CasNumber=cas, Comment=c, Compartment=cat)
        self[uid] = f

        return f

    def _create_process(self, filename):
        """
        Extract dataset object from XML file
        :param filename:
        :return:
        """
        o = self._get_objectified_entity(filename)

        ad = find_tag(o, 'activityDescription')[0]

        u = ad.activity.get('id')

        if self[u] is not None:
            return self[u]

        rf = None  # reference flow
        flowlist = []

        for exch in find_tag(o, 'flowData')[0].getchildren():
            if 'parameter' in exch.tag:
                continue

            f = self._create_flow(exch)
            if hasattr(exch, 'outputGroup'):
                d = 'Output'
                if exch.outputGroup == 0:
                    if rf is None:
                        rf = [f]
                    else:
                        rf.append(f)
                        # Multiple reference flows found!
            elif hasattr(exch, 'inputGroup'):
                d = 'Input'
            else:
                raise DirectionlessExchangeError
            flowlist.append((f, d))

        n = find_tag(ad, 'activityName')[0].text
        try:
            c = find_tag(ad, 'generalComment')[0]['text'].text
        except TypeError:
            c = 'no comment.'
        except AttributeError:
            print('%s: no comment' % filename)
            c = 'no comment.'
        g = find_tag(ad, 'geography')[0].shortname.text

        tp = find_tag(ad, 'timePeriod')[0]
        stt = 'interval(%s, %s)' % (tp.get('startDate'), tp.get('endDate'))
        cls = [self._cls_to_text(i) for i in find_tag(ad, 'classification')]

        p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                      Classifications=cls)

        if rf is not None:
            if len(rf) == 1:
                p['ReferenceExchange'] = Exchange(p, rf[0], 'Output')

        self[u] = p

        for flow, f_dir in flowlist:
            self._add_exchange(Exchange(p, flow, f_dir))

        return p

    def _fetch(self, uid, ref_flow=None):
        """
        ecospoldV2 files are named by activityId_referenceFlow - if none is supplied, take the first one found
        matching activityId
        :param uid:
        :param ref_flow:
        :return:
        """
        if ref_flow is not None:
            uid = '_'.join([uid, ref_flow])
        files = self.list_datasets(uid)
        if len(files) == 0:
            return None
        return self._create_process(files[0])

    def _load_all(self):
        now = time()
        count = 0
        for k in self.list_datasets():
            self._create_process(k)
            count += 1
            if count % 100 == 0:
                print(' Loaded %d processes (t=%.2f s)' % (count, time()-now))

        print(' Loaded %d processes (t=%.2f s)' % (count, time() - now))
        self.check_counter('quantity')
        self.check_counter('flow')
        self.check_counter('process')

    def serialize(self, **kwargs):
        j = super(EcospoldV2Archive, self).serialize(**kwargs)
        if self.internal_prefix is not None:
            j['prefix'] = self.internal_prefix
        j['nsUuid'] = str(self._ns_uuid)
        return j

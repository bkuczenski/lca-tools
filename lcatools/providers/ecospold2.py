"""
Import ecospold2 files
"""

from __future__ import print_function, unicode_literals

import six

import os
import re

from lxml import objectify
from lxml.etree import XMLSyntaxError
from time import time

from collections import namedtuple

from lcatools.providers import tail
from lcatools.providers.xml_widgets import *

from lcatools.interfaces import ArchiveInterface, uuid_regex
from lcatools.providers.archive import Archive
from lcatools.entities import LcQuantity, LcFlow, LcProcess
from lcatools.exchanges import Exchange, DirectionlessExchangeError

if six.PY2:
    bytes = str
    str = unicode


EcospoldExchange = namedtuple('EcospoldExchange', ('flow', 'direction', 'value'))


def spold_reference_flow(filename):
    """
    second UUID, first match should be reference flow uuid
    :param filename:
    :return:
    """
    return uuid_regex.findall(filename)[1][0]


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

    def __init__(self, ref, prefix=None, **kwargs):
        """
        Just instantiates the parent class.
        :param ref: just a reference
        :param prefix: difference between the internal path (ref) and the ILCD base
        :return:
        """
        super(EcospoldV2Archive, self).__init__(ref, **kwargs)
        self.internal_prefix = prefix
        if self.internal_prefix is not None:
            self._serialize_dict['prefix'] = self.internal_prefix

        self._archive = Archive(self.ref)

    # no need for _key_to_id - keys in ecospold are uuids
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

    def _get_objectified_entity_with_lt_gt(self, filename):
        try:
            f = self._fetch_filename(filename)
            f = re.sub(' < ', ' &lt; ', re.sub(' > ', ' &gt; ', f.decode()))
            o = objectify.fromstring(f)
        except ValueError:
            print('failed on :%s:' % filename)
            return None
        except TypeError:
            print('failed on :%s:' % filename)
            raise
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
            self.add(q)
        else:
            q = try_q

        return q

    @staticmethod
    def _cls_to_text(i):
        if isinstance(i, objectify.ObjectifiedElement):
            return ': '.join([i.classificationSystem.text, i.classificationValue.text])
        else:
            return ''

    @staticmethod
    def _cat_to_text(i):
        if isinstance(i, objectify.ObjectifiedElement):
            return [i.compartment.text, i.subcompartment.text]
        else:
            return []

    def _create_flow(self, exchange):
        if 'intermediate' in exchange.tag:
            uid = exchange.attrib['intermediateExchangeId']
            cat = [self._cls_to_text(exchange.classification)]
        elif 'elementary' in exchange.tag:
            uid = exchange.attrib['elementaryExchangeId']
            cat = self._cat_to_text(exchange.compartment)
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

        f = LcFlow(uid, Name=n, CasNumber=cas, Comment=c, Compartment=cat)
        f.add_characterization(quantity=q, reference=True)
        self.add(f)

        return f

    def _create_process_entity(self, o):
        ad = find_tag(o, 'activityDescription')[0]

        u = ad.activity.get('id')

        if self[u] is not None:
            return self[u]

        n = find_tag(ad, 'activityName')[0].text
        try:
            c = find_tag(ad, 'generalComment')[0]['text'].text
        except TypeError:
            c = 'no comment.'
        except AttributeError:
            print('activity ID %s: no comment' % u)
            c = 'no comment.'
        g = find_tag(ad, 'geography')[0].shortname.text

        tp = find_tag(ad, 'timePeriod')[0]
        stt = {'begin': tp.get('startDate'), 'end': tp.get('endDate')}
        cls = [self._cls_to_text(i) for i in find_tag(ad, 'classification')]

        p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                      Classifications=cls)

        self.add(p)
        return p

    def _grab_reference_flow(self, o, rf):
        """
        Create a reference exchange from the flowdata
        :param o:
        :param rf:
        :return:
        """
        for x in find_tag(o, 'flowData')[0].getchildren():
            if 'intermediate' in x.tag:
                if x.attrib['intermediateExchangeId'] == rf:
                    return self._create_flow(x)

        raise KeyError('Noted reference exchange %s not found!' % rf)

    def _collect_exchanges(self, o):
        """

        :param o:
        :return:
        """
        flowlist = []

        for exch in find_tag(o, 'flowData')[0].getchildren():
            if 'parameter' in exch.tag:
                continue

            f = self._create_flow(exch)
            if hasattr(exch, 'outputGroup'):
                d = 'Output'
            elif hasattr(exch, 'inputGroup'):
                d = 'Input'
            else:
                raise DirectionlessExchangeError
            v = float(exch.get('amount'))  # or None if not found
            flowlist.append(EcospoldExchange(f, d, v))
        return flowlist

    def _create_process(self, filename, exchanges=True):
        """
        Extract dataset object from XML file
        :param filename:
        :return:
        """
        try:
            o = self._get_objectified_entity(filename)
        except XMLSyntaxError:
            try:
                o = self._get_objectified_entity_with_lt_gt(filename)
            except XMLSyntaxError:
                print('Failed loading %s' % filename)
                raise

        p = self._create_process_entity(o)
        rf = self._grab_reference_flow(o, spold_reference_flow(filename))
        rx = p.add_reference(rf, 'Output')
        self._print('Identified reference exchange\n %s' % rx)
        if exchanges:
            for exch in self._collect_exchanges(o):
                if exch.value != 0:
                    self._print('Exch %s [%s] (%g)' % (exch.flow, exch.direction, exch.value))
                    p.add_exchange(exch.flow, exch.direction, reference=rx, value=exch.value)

        return p

    '''
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
    '''

    def retrieve_or_fetch_entity(self, filename, **kwargs):
        entity = self._get_entity(filename)  # this checks upstream if it exists
        if entity is not None:
            if spold_reference_flow(filename) in [x.flow.get_uuid() for x in entity.reference_entity]:
                return entity
        return self._create_process(filename, **kwargs)

    def _load_all(self, exchanges=True):
        now = time()
        count = 0
        for k in self.list_datasets():
            self.retrieve_or_fetch_entity(k, exchanges=exchanges)
            count += 1
            if count % 100 == 0:
                print(' Loaded %d processes (t=%.2f s)' % (count, time()-now))

        print(' Loaded %d processes (t=%.2f s)' % (count, time() - now))
        self.check_counter()

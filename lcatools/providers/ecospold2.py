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

from lcatools.providers.base import LcArchive
from lcatools.interfaces import uuid_regex
from lcatools.providers.archive import Archive
from lcatools.entities import LcQuantity, LcFlow, LcProcess
from lcatools.exchanges import ExchangeValue, DirectionlessExchangeError
from lcatools.characterizations import Characterization
from lcatools.lcia_results import LciaResult, LciaResults

if six.PY2:
    bytes = str
    str = unicode


EcospoldExchange = namedtuple('EcospoldExchange', ('flow', 'direction', 'value', 'termination', 'is_ref'))
EcospoldLciaResult = namedtuple('EcospoldLciaResult', ('Method', 'Category', 'Indicator', 'score'))


def spold_reference_flow(filename):
    """
    second UUID, first match should be reference flow uuid
    :param filename:
    :return:
    """
    return uuid_regex.findall(filename)[1][0]


class EcospoldV2Error(Exception):
    pass


class EcospoldV2Archive(LcArchive):
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

    def fg_proxy(self, proxy):
        for ds in self.list_datasets(proxy):
            self.retrieve_or_fetch_entity(ds)
        return self[proxy]

    def bg_proxy(self, proxy):
        return self.fg_proxy(proxy)

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
        """
        makes a flow entity and adds to the db
        :param exchange:
        :return:
        """
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
            cas = ''

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
            if 'impactIndicator' in exch.tag:
                continue

            f = self._create_flow(exch)
            is_ref = False
            if hasattr(exch, 'outputGroup'):
                d = 'Output'
                og = exch.outputGroup
                if og == 0:
                    is_ref = True
                elif og == 2:  # 1, 3 not used
                    for cls in find_tag(exch, 'classification'):
                        if cls.classificationSystem == 'By-product classification':
                            if str(cls.classificationValue).startswith('allocat'):
                                is_ref = True
            elif hasattr(exch, 'inputGroup'):
                d = 'Input'
            else:
                raise DirectionlessExchangeError
            v = float(exch.get('amount'))  # or None if not found
            t = exch.get('activityLinkId')  # or None if not found
            flowlist.append(EcospoldExchange(f, d, v, t, is_ref))
        return flowlist

    def _collect_impact_scores(self, o, process, flow):
        """
        the old "1115"
        :param o:
        :return:
        """

        scores = []
        exch = ExchangeValue(process, flow, 'Output', value=1.0)

        for cf in find_tag(o, 'flowData')[0].getchildren():
            if 'impactIndicator' in cf.tag:
                m = cf.impactMethodName.text
                c = cf.impactCategoryName.text
                i = cf.name.text
                v = float(cf.get('amount'))
                scores.append(EcospoldLciaResult(m, c, i, v))

        return scores

    def objectify(self, filename):
        try:
            o = self._get_objectified_entity(filename)
        except XMLSyntaxError:
            print('  !!XMLSyntaxError-- trying to escape < and > signs')
            try:
                o = self._get_objectified_entity_with_lt_gt(filename)
            except XMLSyntaxError:
                print('  !!Failed loading %s' % filename)
                raise
        return o

    def _create_process(self, filename, exchanges=True):
        """
        Extract dataset object from XML file
        :param filename:
        :return:
        """
        o = self.objectify(filename)

        p = self._create_process_entity(o)
        rf = self._grab_reference_flow(o, spold_reference_flow(filename))
        rx = p.add_reference(rf, 'Output')
        self._print('Identified reference exchange\n %s' % rx)
        if exchanges:
            for exch in self._collect_exchanges(o):
                if exch.is_ref:
                    p.add_reference(exch.flow, exch.direction)
                    if exch.termination is not None:
                        '''
                        # this should be an error, but you know ecoinvent...
                        raise EcospoldV2Error('Terminated Reference flow encountered in %s\nFlow %s Term %s' %
                                              (p.get_uuid(), exch.flow.get_uuid(), exch.termination))
                        '''
                        print('Ignoring termination in reference exchange, Process: %s\nFlow %s Term %s' %
                                              (p.get_uuid(), exch.flow.get_uuid(), exch.termination))
                        p.add_exchange(exch.flow, exch.direction, reference=rx, value=exch.value,
                                       termination=None)
                        continue
                if exch.value != 0:
                    self._print('Exch %s [%s] (%g)' % (exch.flow, exch.direction, exch.value))
                    p.add_exchange(exch.flow, exch.direction, reference=rx, value=exch.value,
                                   termination=exch.termination)

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

    def retrieve_lcia_scores(self, filename, quantities=None):
        """
        This function retrieves LCIA scores from an Ecospold02 file and stores them as characterizations in
        an LcFlow entity corresponding to the *first* (and presumably, only) reference intermediate flow

        Only stores cfs for quantities that exist locally.
        :param filename:
        :param quantities: list of quantity entities to look for (defaults to self.quantities())
        :return: a dict of quantity uuid to score
        """
        if quantities is None:
            quantities = self.quantities()

        import time
        start_time = time.time()
        print('Loading LCIA results from %s' % filename)
        o = self.objectify(filename)

        self._print('%30.30s -- %5f' % ('Objectified', time.time() - start_time))
        p = self._create_process_entity(o)
        rf = self._grab_reference_flow(o, spold_reference_flow(filename))

        exch = ExchangeValue(p, rf, 'Output', value=1.0)

        tags = dict()
        for q in quantities:
            if 'Method' in q.keys():
                if q['Name'] in tags:
                    raise KeyError('Name collision %s' % q['Name'])
                tags[q['Name']] = q

        results = LciaResults(p)

        for char in find_tag(o, 'flowData')[0].getchildren():
            if 'impactIndicator' in char.tag:
                m = char.impactMethodName.text
                c = char.impactCategoryName.text
                i = char.name.text
                v = float(char.get('amount'))
                my_tag = ', '.join([m, c, i])
                if my_tag in tags:
                    q = tags[my_tag]
                    result = LciaResult(q)
                    cf = Characterization(rf, q, value=v, location=p['SpatialScope'])
                    result.add_score(p.get_uuid(), exch, cf, p['SpatialScope'])
                    results[q.get_uuid()] = result

        self._print('%30.30s -- %5f' % ('Impact scores collected', time.time() - start_time))

        return results

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

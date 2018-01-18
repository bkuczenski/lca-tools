"""
Import ecospold2 files
"""

from __future__ import print_function, unicode_literals

import os
from collections import namedtuple
from time import time

import six
from lxml import objectify
from lxml.etree import XMLSyntaxError

from lcatools.characterizations import Characterization
from lcatools.entities import LcQuantity, LcFlow, LcProcess
from lcatools.exchanges import ExchangeValue, DirectionlessExchangeError
from lcatools.lcia_results import LciaResult, LciaResults
from lcatools.providers import tail
from lcatools.providers.archive import Archive
from lcatools.providers.base import LcArchive
from lcatools.providers.xml_widgets import *
from lcatools.providers.interfaces import uuid_regex

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
    try:
        return uuid_regex.findall(filename)[1][0]
    except IndexError:
        return None


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

    def __init__(self, source, prefix=None, linked=True, **kwargs):
        """
        Just instantiates the parent class.
        :param source: physical data source
        :param prefix: relative path for datasets from the archive root
        :param linked: [True] whether the archive includes unlinked or linked datasets. Reference exchanges
        get detected differently in one case versus the other (see _create_process)
        :return:
        """
        super(EcospoldV2Archive, self).__init__(source, **kwargs)
        if prefix is not None:
            self._serialize_dict['prefix'] = prefix

        self._archive = Archive(self.source, internal_prefix=prefix)
        self._linked = linked

    def fg_proxy(self, proxy):
        for ds in self.list_datasets(proxy):
            self.retrieve_or_fetch_entity(ds)
        return self[proxy]

    def bg_proxy(self, proxy):
        return self.fg_proxy(proxy)

    # no need for _key_to_id - keys in ecospold are uuids
    def _fetch_filename(self, filename):
        st = self._archive.readfile(filename)
        if st is None:
            raise FileNotFoundError
        return st

    def list_datasets(self, startswith=None):
        assert self._archive.remote is False, "Cannot list objects for remote archives"
        for x in self._archive.listfiles(in_prefix=startswith):
            yield x

    def _get_objectified_entity(self, filename):
        try:
            o = objectify.fromstring(self._fetch_filename(filename))
        except FileNotFoundError:
            raise
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

        f = self[uid]
        if f is not None:
            return f

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
        ad = find_tag(o, 'activityDescription')

        u = ad.activity.get('id')

        if self[u] is not None:
            return self[u]

        n = find_tag(ad, 'activityName').text
        try:
            c = find_tag(ad, 'generalComment')['text'].text
        except TypeError:
            c = 'no comment.'
        except AttributeError:
            print('activity ID %s: no comment' % u)
            c = 'no comment.'
        g = find_tag(ad, 'geography').shortname.text

        tp = find_tag(ad, 'timePeriod')
        stt = {'begin': tp.get('startDate'), 'end': tp.get('endDate')}
        cls = [self._cls_to_text(i) for i in find_tags(ad, 'classification')]

        p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                      Classifications=cls)

        parent = find_tag(o, 'activity').get('parentActivityId')
        if parent is not None:
            p['ParentActivityId'] = parent

        self.add(p)
        return p

    def _grab_reference_flow(self, o, rf):
        """
        Create a reference exchange from the flowdata
        :param o:
        :param rf:
        :return:
        """
        for x in find_tag(o, 'flowData').getchildren():
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

        for exch in find_tag(o, 'flowData').getchildren():
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
                    for cls in find_tags(exch, 'classification'):
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

    @staticmethod
    def _collect_impact_scores(o):  # , process, flow):
        """
        the old "1115"
        :param o:
        :return:
        """

        scores = []
        # exch = ExchangeValue(process, flow, 'Output', value=1.0)

        for cf in find_tag(o, 'flowData').getchildren():
            if 'impactIndicator' in cf.tag:
                m = cf.impactMethodName.text
                c = cf.impactCategoryName.text
                i = cf.name.text
                v = float(cf.get('amount'))
                scores.append(EcospoldLciaResult(m, c, i, v))

        return scores

    def objectify(self, filename):
        self._print('\nObjectifying %s' % filename)
        try:
            o = self._get_objectified_entity(filename)
        except FileNotFoundError:
            raise
        except XMLSyntaxError:
            print('  !!XMLSyntaxError-- trying to escape < and > signs')
            try:
                o = self._get_objectified_entity_with_lt_gt(filename)
            except XMLSyntaxError:
                print('  !!Failed loading %s' % filename)
                raise
        return o

    def find_tag(self, filename, tag):
        return find_tag(self.objectify(filename), tag)

    def _create_process(self, filename, exchanges=True):
        """
        Extract dataset object from XML file.  Handles unlinked and linked archives differently:
         * for unlinked (unallocated) archives, there is exactly one spold file for each process.  Reference flows are
           determined contextually, based on outputGroup and By-product classification (see _collect_exchanges).

         * for linked (allocated) archives, there is a distinct spold file for each allocation. References are
           determined based on the filename, which specifies the reference exchange for which the allocation was
           performed.

        :param filename:
        :return:
        """
        try:
            o = self.objectify(filename)
        except KeyError:
            raise FileNotFoundError

        p = self._create_process_entity(o)
        if self._linked:
            rf = self._grab_reference_flow(o, spold_reference_flow(filename))
            p.add_exchange(rf, 'Output')  # this should get overwritten with an ExchangeValue later
            rx = p.add_reference(rf, 'Output')
            self._print('# Identified reference exchange\n %s' % rx)
        else:
            rx = None
        if exchanges:
            for exch in self._collect_exchanges(o):
                """
                If the dataset is linked, all we do is load non-zero exchanges, ideally all with terminations
                If the dataset is unlinked, we first test if the exchange is marked with a conflicting termination
                  (a reference flow with a termination is regarded as an error) and if so, we raise.
                   if not, we proceed normally.  EXCEPT- we don't want to wind up with a reference exchange
                   that is not present in the exchange list because it's zero-valued.  There shouldn't be any zero-
                   valued reference exchanges in unlinked datasets, so we will err on the side of adding zero-valued
                   exchanges.  The unlinked data should include as much information as possible.
                """
                if self._linked:
                    if exch.value != 0:
                        self._print('## Exch %s [%s] (%g)' % (exch.flow, exch.direction, exch.value))
                        # still need to sanitize termination on reference flows, lest we get a double entry
                        if exch.is_ref:
                            if exch.termination is not None:
                                print('Squashing bad termination in linked reference exchange, %s\nFlow %s Term %s' % (
                                    p.get_uuid(), exch.flow.get_uuid(), exch.termination))
                                p.add_exchange(exch.flow, exch.direction, reference=rx, value=exch.value,
                                               termination=None)
                                continue
                        p.add_exchange(exch.flow, exch.direction, reference=rx, value=exch.value,
                                       termination=exch.termination)
                else:
                    # use exch.is_ref to identify references; store unallocated values (rx already None)
                    if exch.is_ref:
                        if exch.termination is not None:
                            raise EcospoldV2Error('Terminated Reference flow encountered in %s\nFlow %s Term %s' % (
                                                  p.get_uuid(), exch.flow.get_uuid(), exch.termination))
                            # suggested resolution: neutralize the is_ref
                    self._print('## Exch %s [%s] (%g)' % (exch.flow, exch.direction, exch.value))
                    p.add_exchange(exch.flow, exch.direction, reference=rx, value=exch.value,
                                   termination=exch.termination)
                    if exch.is_ref:
                        self._print('## ## Exch is reference %s %s' % (exch.flow, exch.direction))
                        p.add_reference(exch.flow, exch.direction)
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

    def _fetch(self, ext_ref, **kwargs):
        """
        We want to handle two different kinds of external references: spold filenames (uuid_uuid.spold) and simple
        entity uuids.
        :param ext_ref:
        :param kwargs:
        :return:
        """

        try:
            return self._create_process(ext_ref, **kwargs)
        except FileNotFoundError:
            p = None
            for f in self.list_datasets(ext_ref):
                p = self._create_process(f, **kwargs)
            return p

    def retrieve_lcia_scores(self, filename, quantities=None):
        """
        This function retrieves LCIA scores from an Ecospold02 file and stores them as characterizations in
        an LcFlow entity corresponding to the *first* (and presumably, only) reference intermediate flow

        Only stores cfs for quantities that exist locally.
        :param filename:
        :param quantities: list of quantity entities to look for (defaults to all local lcia_methods)
        :return: a dict of quantity uuid to score
        """
        if quantities is None:
            quantities = [l for l in self.entities_by_type('quantity') if l.is_lcia_method()]

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

        for char in find_tag(o, 'flowData').getchildren():
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

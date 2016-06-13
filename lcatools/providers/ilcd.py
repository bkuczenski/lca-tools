from __future__ import print_function, unicode_literals

import re
import os

from lxml import objectify
from lxml.etree import XMLSyntaxError

try:  # python3
    from urllib.parse import urljoin
    from urllib.error import HTTPError
except ImportError:  # python2
    from urlparse import urljoin
    from urllib2 import HTTPError

    bytes = str
    str = unicode


from lcatools.providers.archive import Archive
from lcatools.providers.xml_widgets import *
from lcatools.entities import LcFlow, LcProcess, LcQuantity, LcUnit
from lcatools.exchanges import Exchange
from lcatools.interfaces import ArchiveInterface, uuid_regex

import posixpath

typeDirs = {'Process': 'processes',
            'Flow': 'flows',
            'LCIAMethod': 'lciamethods',
            'FlowProperty': 'flowproperties',
            'UnitGroup': 'unitgroups',
            'Source': 'sources',
            'Contact': 'contacts'
            }

elcd3_local_fallback = os.path.join(os.path.expanduser('~'), 'Dropbox', 'data',
                                    'ELCD', 'ELCD3.2.zip')


elcd3_remote_fallback = "http://eplca.jrc.ec.europa.eu/ELCD3/resource/"


def find_ns(nsmap, dtype):
    return next((k for k, v in nsmap.items() if re.search(dtype + '$', v)))


def _check_dtype(dtype):
    if dtype not in typeDirs:
        print('Datatype %s not known.' % dtype)
        return False
    return True


def _extract_dtype(filename, pathtype=os.path):
    cands = [i for i in re.split(pathtype.sep, filename) if i in typeDirs.values()]
    dtype = [k for k, v in typeDirs.items() if v in cands]
    if len(dtype) == 0:
        dtype = [None]
    uid = uuid_regex.search(filename).groups()[0]
    return dtype[0], uid


def get_flow_ref(exch, ns=None):
    f_uuid = find_tag(exch, 'referenceToFlowDataSet', ns=ns)[0].attrib['refObjectId']
    f_uri = find_tag(exch, 'referenceToFlowDataSet', ns=ns)[0].attrib['uri']
    f_dir = find_tag(exch, 'exchangeDirection', ns=ns)[0].text
    return f_uuid, f_uri, f_dir


def get_reference_flow(process, ns=None):
    try_ref = find_tag(process, 'referenceToReferenceFlow', ns=ns)[0]
    if try_ref == '':
        return None, None, None  # multioutput, no specified reference
    else:
        ref_to_ref = int(try_ref)
    rf = [i for i in process['exchanges'].getchildren()
          if int(i.attrib['dataSetInternalID']) == ref_to_ref][0]
    return get_flow_ref(rf, ns=ns)


def get_reference_flow_property(flow, ns=None):
    # load or check the reference quantity
    ref_to_ref = int(find_tag(flow, 'referenceToReferenceFlowProperty', ns=ns)[0])
    rfp = [i for i in flow['flowProperties'].getchildren()
           if int(i.attrib['dataSetInternalID']) == ref_to_ref][0]
    rfp_uuid = find_tag(rfp, 'referenceToFlowPropertyDataSet', ns=ns)[0].attrib['refObjectId']
    rfp_uri = find_tag(rfp, 'referenceToFlowPropertyDataSet', ns=ns)[0].attrib['uri']
    return rfp_uuid, rfp_uri


def get_reference_unit_group(q, ns=None):
    ref_to_ref = find_tag(q, 'referenceToReferenceUnitGroup', ns=ns)[0]
    ug_uuid = ref_to_ref.attrib['refObjectId']
    ug_uri = ref_to_ref.attrib['uri']
    return ug_uuid, ug_uri


def get_exch_value(exch, ns=None):
    try:
        v = float(find_tag(exch, 'resultingAmount', ns=ns)[0])
    except ValueError:
        v = None
    return v

'''
class IlcdEntity(object):
    """
    Container for objectified ILCD entities from XML
    """
    def __init__(self, path):
        """

        :param path:
        :return:
        """
        self.o = objectify.fromstring(string)

    def el(self):
'''


class IlcdArchive(ArchiveInterface):
    """
    This class handles de-referencing for ILCD archives
    """

    def __init__(self, ref, prefix=None, quiet=True):
        """
        Just instantiates the parent class.
        :param ref: root of the archive
        :param prefix: difference between the internal path (ref) and the ILCD base
          (note: for local archives, this defaults to 'ILCD'; for remote arcnives it
           defaults to empty)
        :param quiet: forwarded to ArchiveInterface
        :return:
        """
        super(IlcdArchive, self).__init__(ref, quiet=quiet)
        self.internal_prefix = prefix
        if prefix is not None:
            self._serialize_dict['prefix'] = prefix

        self._archive = Archive(self.ref)

        if not self._archive.OK:
            print('Trying local ELCD reference')
            self._archive = Archive(elcd3_local_fallback)
        if not self._archive.OK:
            print('Falling back to ELCD Remote Reference')
            self._archive = Archive(elcd3_remote_fallback, query_string='format=xml')

        if self._archive.compressed or self._archive.remote:
            self._pathtype = posixpath
        else:
            self._pathtype = os.path

    def _build_prefix(self):
        if self._archive.remote:
            path = ''
        else:
            path = 'ILCD'
        if self.internal_prefix is not None:
            path = self._pathtype.join(self.internal_prefix, path)
        return path

    def _de_prefix(self, file):
        return re.sub('^' + self._pathtype.join(self._build_prefix(), ''), '', file)

    def _build_entity_path(self, dtype, uid):
        assert _check_dtype(dtype)
        postpath = self._pathtype.join(self._build_prefix(), typeDirs[dtype], uid)
        return postpath + '.xml'

    def search_by_id(self, uid, dtype=None):
        return [i for i in self.list_objects(dtype=dtype) if re.search(uid, i, flags=re.IGNORECASE)]

    def list_objects(self, dtype=None):
        assert self._archive.remote is False, "Cannot list objects for remote archives"
        in_prefix = self._build_prefix()
        if dtype is not None:
            assert _check_dtype(dtype)
            in_prefix = self._pathtype.join(in_prefix, typeDirs[dtype])
        return [self._de_prefix(f) for f in self._archive.listfiles(in_prefix=in_prefix)]

    def _fetch_filename(self, filename):
        return self._archive.readfile(filename)

    def _check_or_retrieve_child(self, filename, uid, uri):
        child = self._get_entity(uid)
        if child is None:
            new_path = urljoin(filename, uri)
            dtype, uid = _extract_dtype(new_path, self._pathtype)
            child = self.retrieve_or_fetch_entity(uid, dtype=dtype)
        return child

    def _get_objectified_entity(self, filename):
        return objectify.fromstring(self._fetch_filename(filename))

    def objectify(self, uid, **kwargs):
        search_results = self.search_by_id(uid, **kwargs)
        return [self._get_objectified_entity(k) for k in search_results]

    def _create_unit(self, unit_ref):
        """
        UnitGroups aren't stored as full-fledged entities- they are stored as dicts inside quantities.
        :param unit_ref:
        :return:
        """
        dtype, uid = _extract_dtype(unit_ref, self._pathtype)
        filename = self._build_entity_path(dtype, uid)
        o = self._get_objectified_entity(filename)

        ns = find_ns(o.nsmap, 'UnitGroup')

        u = str(find_common(o, 'UUID')[0])
        reference_unit = int(find_tag(o, 'referenceToReferenceUnit', ns=ns)[0])
        unitstring = str(o['units'].getchildren()[reference_unit]['name'])
        ref_unit = LcUnit(unitstring, unit_uuid=u)
        ref_unit.set_external_ref('%s/%s' % (typeDirs['UnitGroup'], u))

        unitconv = dict()
        for i in o['units'].getchildren():
            unitconv[str(i['name'])] = 1.0 / float(i['meanValue'])
        return ref_unit, unitconv

    def _create_quantity(self, filename):
        """

        :param filename:
        :return:
        """
        o = self._get_objectified_entity(filename)
        ns = find_ns(o.nsmap, 'FlowProperty')

        u = str(find_common(o, 'UUID')[0])
        n = str(find_common(o, 'name')[0])

        c = str(find_common(o, 'generalComment')[0])

        ug, ug_uri = get_reference_unit_group(o, ns=ns)

        ug_path = urljoin(filename, '../unitgroups/' + ug)  # need the path without extension- I know- it's all sloppy

        refunit, unitconv = self._create_unit(ug_path)

        q = LcQuantity(u, Name=n, ReferenceUnit=refunit, UnitConversion=unitconv, Comment=c)
        q.set_external_ref('%s/%s' % (typeDirs['FlowProperty'], u))

        self.add(q)

        return q

    @staticmethod
    def _create_dummy_flow_from_exch(uid, exch):
        n = str(find_common(exch, 'shortDescription')[0])
        print('Creating DUMMY flow with name %s' % n)
        return LcFlow(uid, Name=n, Comment='Dummy flow (HTTP or XML error)')

    def _create_flow(self, filename):
        """

        :param filename: path to the data set relative to the archive
        :return: an LcFlow
        """
        o = self._get_objectified_entity(filename)

        ns = find_ns(o.nsmap, 'Flow')

        u = str(find_common(o, 'UUID')[0])
        n = str(find_tag(o, 'baseName', ns=ns)[0])

        rfp, rfp_uri = get_reference_flow_property(o, ns=ns)
        q = self._check_or_retrieve_child(filename, rfp, rfp_uri)

        c = str(find_common(o, 'generalComment')[0])

        cas = str(find_tag(o, 'CASNumber', ns=ns)[0])

        cat = find_common(o, 'category')
        if cat == ['']:
            cat = find_common(o, 'class')
        cat = [str(i) for i in cat]

        f = LcFlow(u, Name=n, CasNumber=cas, Comment=c, Compartment=cat)
        f.set_external_ref('%s/%s' % (typeDirs['Flow'], u))

        f.add_characterization(q, reference=True)

        self.add(f)
        return f

    def _create_process(self, filename):
        """

        :param filename:
        :return:
        """
        o = self._get_objectified_entity(filename)

        ns = find_ns(o.nsmap, 'Process')

        try:
            rf, rf_uri, rf_dir = get_reference_flow(o, ns=ns)
        except XMLSyntaxError:
            rf = None
            rf_dir = None

        exch_list = []

        for exch in o['exchanges'].getchildren():
            # load all child flows
            f_id, f_uri, f_dir = get_flow_ref(exch, ns=ns)
            try:
                f = self._check_or_retrieve_child(filename, f_id, f_uri)
            except (HTTPError, XMLSyntaxError, KeyError):
                f = self._create_dummy_flow_from_exch(f_id, exch)
                self.add(f)
            v = get_exch_value(exch, ns=ns)
            exch_list.append((f, f_dir, v))

        u = str(find_common(o, 'UUID')[0])
        n = str(find_tag(o, 'baseName', ns=ns)[0])

        g = find_tag(o, 'locationOfOperationSupplyOrProduction', ns=ns)[0].attrib['location']

        stt = {'begin': str(find_common(o, 'referenceYear')[0]), 'end': str(find_common(o, 'dataSetValidUntil')[0])}

        c = str(find_common(o, 'generalComment')[0])

        cls = [str(i) for i in find_common(o, 'class')]

        p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                      Classifications=cls)

        p.set_external_ref('%s/%s' % (typeDirs['Process'], u))

        for flow, f_dir, val in exch_list:
            is_rf = (rf == flow.get_uuid() and rf_dir == f_dir)
            p.add_exchange(flow, f_dir, reference=is_rf, value=val)

        self.add(p)

        return p

    def _fetch(self, uid, **kwargs):
        """
        fetch an object from the archive by ID.
        dtype MUST be specified as kwarg for remote archives; otherwise will search
        :param uid:
        :return:
        """
        try:
            dtype = kwargs['dtype']
        except KeyError:
            dtype = None

        if dtype is None:
            dtype, uu = _extract_dtype(uid, self._pathtype)
            if dtype is not None:
                uid = uu

        if dtype is None:
            if self._archive.remote:
                print('Cannot search on remote archives. Please supply a dtype')
                return None
            search_results = self.search_by_id(uid)
            if len(search_results) > 0:
                print('Found Results:')
                [print(i) for i in search_results]
                if len(search_results) > 1:
                    print('Please specify dtype')
                    return None
                filename = search_results[0]
                dtype, uid = _extract_dtype(filename, self._pathtype)
            else:
                print('No results.')
                return None
        else:
            filename = self._build_entity_path(dtype, uid)

        entity = self._get_entity(uid)
        if entity is not None:
            return entity

        if dtype == 'Flow':
            return self._create_flow(filename)
        elif dtype == 'Process':
            return self._create_process(filename)
        elif dtype == 'FlowProperty':
            return self._create_quantity(filename)
        else:
            return objectify.fromstring(self._archive.readfile(filename))

    def _load_all(self):
        for i in self.list_objects('Process'):
            self.retrieve_or_fetch_entity(i)
        self.check_counter('quantity')
        self.check_counter('flow')
        self.check_counter('process')



'''
class IlcdWebInterface(IlcdArchive):
    """
    Collects entities from an ILCD web server and stores their metadata locally

    self.ref should be assumed to be an http link prefix _ such that:
    _/(datatypes)/(uuid).xml is valid for readable uuids.

    """
    def fetch_entity(self, type, id):
        """

        :param type:
        :param id:
        :return:
        """
'''

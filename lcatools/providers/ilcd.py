import re
import os

from lxml import objectify
from lxml.etree import XMLSyntaxError

from urllib.parse import urljoin

from lcatools.providers.archive import Archive
from lcatools.entities import LcFlow, LcProcess, LcQuantity, LcUnit
from lcatools.interfaces import BasicInterface, uuid_regex


typeDirs = {'Process': 'processes',
            'Flow': 'flows',
            'LCIAMethod': 'lciamethods',
            'FlowProperty': 'flowproperties',
            'UnitGroup': 'unitgroups',
            'Source': 'sources',
            'Contact': 'contacts'
            }


def _check_dtype(dtype):
    if dtype not in typeDirs:
        print('Datatype %s not known.' % dtype)
        return False
    return True


def _extract_dtype(filename):
    cands = [i for i in re.split(os.path.sep, filename) if i in typeDirs.values()]
    dtype = [k for k, v in typeDirs.items() if v in cands]
    if len(dtype) == 0:
        dtype = [None]
    uid = uuid_regex.search(filename).groups()[0]
    return dtype[0], uid


def _find_tag(o, tag, ns=None):
    """
    Deals with the fuckin' ILCD namespace shit
    :param o: objectified element
    :param tag:
    :return:
    """
    found = o.findall('.//{0}{1}'.format('{' + o.nsmap[ns] + '}', tag))
    return [''] if len(found) == 0 else found


def _find_common(o, tag):
    return _find_tag(o, tag, ns='common')


def get_flow_ref(exch):
    f_uuid = _find_tag(exch, 'referenceToFlowDataSet')[0].attrib['refObjectId']
    f_uri = _find_tag(exch, 'referenceToFlowDataSet')[0].attrib['uri']
    return f_uuid, f_uri


def get_reference_flow(process):
    try_ref = _find_tag(process, 'referenceToReferenceFlow')[0]
    if try_ref == '':
        return None, None  # multioutput, no specified reference
    else:
        ref_to_ref = int(try_ref)
    rf = [i for i in process['exchanges'].getchildren()
          if int(i.attrib['dataSetInternalID']) == ref_to_ref][0]
    return get_flow_ref(rf)


def get_reference_flow_property(flow):
    # load or check the reference quantity
    ref_to_ref = int(_find_tag(flow, 'referenceToReferenceFlowProperty')[0])
    rfp = [i for i in flow['flowProperties'].getchildren()
           if int(i.attrib['dataSetInternalID']) == ref_to_ref][0]
    rfp_uuid = _find_tag(rfp, 'referenceToFlowPropertyDataSet')[0].attrib['refObjectId']
    rfp_uri = _find_tag(rfp, 'referenceToFlowPropertyDataSet')[0].attrib['uri']
    return rfp_uuid, rfp_uri


def get_reference_unit_group(q):
    ref_to_ref = _find_tag(q, 'referenceToReferenceUnitGroup')[0]
    ug_uuid = ref_to_ref.attrib['refObjectId']
    ug_uri = ref_to_ref.attrib['uri']
    return ug_uuid, ug_uri


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


class IlcdArchive(BasicInterface):
    """
    This class handles de-referencing for ILCD archives
    """

    def __init__(self, *args, prefix=None):
        """
        Just instantiates the parent class.
        :param args: just a reference
        :param prefix: difference between the internal path (ref) and the ILCD base
        :return:
        """
        super(IlcdArchive, self).__init__(*args)
        self.internal_prefix = prefix
        self._archive = Archive(self.ref)

    def _build_prefix(self, dtype=None):
        if self._archive.remote:
            path = ''
        else:
            path = 'ILCD'
        if self.internal_prefix is not None:
            path = os.path.join(self.internal_prefix, path)
        if dtype is not None:
            path = os.path.join(path, typeDirs[dtype])
        return path

    def _build_entity_path(self, dtype, uid):
        assert _check_dtype(dtype)
        postpath = os.path.join(self._build_prefix(dtype), uid)
        return postpath + '.xml'

    def _search_by_id(self, uid, dtype=None):
        return [i for i in self.list_objects(dtype=dtype) if re.search(uid, i, flags=re.IGNORECASE)]

    def list_objects(self, dtype=None):
        assert self._archive.remote is False, "Cannot list objects for remote archives"
        if dtype is not None:
            assert _check_dtype(dtype)
        return self._archive.listfiles(in_prefix=self._build_prefix(dtype))

    def _fetch_filename(self, filename):
        return self._archive.readfile(filename)

    def _check_or_retrieve_child(self, filename, uid, uri):
        child = self._get_entity(uid)
        if child is None:
            new_path = urljoin(filename, uri)
            dtype, uid = _extract_dtype(new_path)
            child = self.retrieve_or_fetch_entity(uid, dtype=dtype)
        return child

    def _get_objectified_entity(self, filename):
        return objectify.fromstring(self._archive.readfile(filename))

    def objectify(self, uid):
        e = self.retrieve_or_fetch_entity(uid)
        dtype = {
            'process': 'Process',
            'flow': 'Flow',
            'quantity': 'FlowProperty'
        }[e.entity_type]
        return self._get_objectified_entity(self._build_entity_path(dtype, e.get_uuid()))

    def _create_unitgroup(self, filename):
        """
        UnitGroups aren't stored as full-fledged entities- they are stored as dicts inside quantities.
        :param filename:
        :return:
        """
        o = self._get_objectified_entity(filename)

        u = str(_find_common(o, 'UUID')[0])
        reference_unit = int(_find_tag(o, 'referenceToReferenceUnit')[0])
        unitstring = str(o['units'].getchildren()[reference_unit]['name'])
        ref_unit = LcUnit(unitstring, u)

        unitconv = dict()
        for i in o['units'].getchildren():
            unitconv[str(i['name'])] = float(i['meanValue'])
        return ref_unit, unitconv

    def _create_quantity(self, filename):
        """

        :param filename:
        :return:
        """
        o = self._get_objectified_entity(filename)

        u = str(_find_common(o, 'UUID')[0])
        n = str(_find_common(o, 'name')[0])

        c = str(_find_common(o, 'generalComment')[0])

        ug, ug_uri = get_reference_unit_group(o)

        ug_path = urljoin(filename, ug_uri)

        refunit, unitconv = self._create_unitgroup(ug_path)

        try_q = self.quantity_with_unit(refunit.unitstring())

        if len(try_q) == 0:
            q = LcQuantity(u, Name=n, ReferenceUnit=refunit, UnitConversion=unitconv, Comment=c)
            self[u] = q
        else:
            q = try_q[0]

        return q

    def _create_flow(self, filename):
        """

        :param filename: path to the data set relative to the archive
        :return: an LcFlow
        """
        o = self._get_objectified_entity(filename)

        u = str(_find_common(o, 'UUID')[0])
        n = str(_find_tag(o, 'baseName')[0])

        rfp, rfp_uri = get_reference_flow_property(o)
        q = self._check_or_retrieve_child(filename, rfp, rfp_uri)

        c = str(_find_common(o, 'generalComment')[0])

        cas = str(_find_tag(o, 'CASNumber')[0])

        cat = _find_common(o, 'category')
        if cat == ['']:
            cat = _find_common(o, 'class')
        cat = [str(i) for i in cat]

        f = LcFlow(u, Name=n, ReferenceQuantity=q, CasNumber=cas, Comment=c, Compartment=cat)
        self[u] = f
        return f

    def _create_process(self, filename):
        """

        :param filename:
        :return:
        """
        o = self._get_objectified_entity(filename)

        try:
            rf, rf_uri = get_reference_flow(o)
        except XMLSyntaxError:
            rf = None

        flowlist = []

        for exch in o['exchanges'].getchildren():
            # load all child flows
            f_id, f_uri = get_flow_ref(exch)
            flowlist.append(self._check_or_retrieve_child(filename, f_id, f_uri))

        u = str(_find_common(o, 'UUID')[0])
        n = str(_find_tag(o, 'baseName')[0])

        g = _find_tag(o, 'locationOfOperationSupplyOrProduction')[0].attrib['location']

        stt = "interval(%s, %s)" % (_find_common(o, 'referenceYear')[0],
                                    _find_common(o, 'dataSetValidUntil')[0])

        c = str(_find_common(o, 'generalComment')[0])

        cls = [str(i) for i in _find_common(o, 'class')]

        p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                      Classifications=cls)

        if rf is not None:
            f = self[rf]
            p['ReferenceFlow'] = f

        self[u] = p
        return p, flowlist

    def _fetch(self, uid, **kwargs):
        """
        fetch an object from the archive by ID.
        dtype MUST be specified as kwarg for remote archives; otherwise will search
        :param uid:
        :return:
        """
        print('Trying to fetch new entity with %s' % uid)
        try:
            dtype = kwargs['dtype']
        except KeyError:
            dtype = None

        if dtype is None:
            dtype, uu = _extract_dtype(uid)
            if dtype is not None:
                uid = uu

        if dtype is None:
            if self._archive.remote:
                print('Cannot search on remote archives. Please supply a dtype')
                return None
            search_results = self._search_by_id(uid)
            if len(search_results) > 0:
                print('Found Results:')
                [print(i) for i in search_results]
                if len(search_results) > 1:
                    print('Please specify dtype')
                    return None
                filename = search_results[0]
                dtype, uid = _extract_dtype(filename)
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

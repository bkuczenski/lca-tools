from __future__ import print_function, unicode_literals

import os
from itertools import chain

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


from ..file_store import FileStore
from ..xml_widgets import *

from lcatools.interfaces import uuid_regex

from lcatools.archives import LcArchive
from lcatools.entities import LcFlow, LcProcess, LcQuantity, LcUnit
from lcatools.characterizations import DuplicateCharacterizationError

typeDirs = {'Process': 'processes',
            'Flow': 'flows',
            'LCIAMethod': 'LCIAmethods',  # love that case consistency
            'FlowProperty': 'flowproperties',
            'UnitGroup': 'unitgroups',
            'Source': 'sources',
            'Contact': 'contacts'
            }

elcd3_local_fallback = os.path.join(os.path.expanduser('~'), 'Dropbox', 'data',
                                    'ELCD', 'ELCD3.2.zip')


elcd3_remote_fallback = "http://eplca.jrc.ec.europa.eu/ELCD3/resource/"


def _check_dtype(dtype):
    if dtype not in typeDirs:
        print('Datatype %s not known.' % dtype)
        return False
    return True


def _extract_uuid(filename):
    return uuid_regex.search(filename).groups()[0]


def _extract_dtype(filename, pathtype=os.path):
    cands = [i for i in re.split(pathtype.sep, filename) if i in typeDirs.values()]
    dtype = [k for k, v in typeDirs.items() if v in cands]
    if len(dtype) == 0:
        raise ValueError('No dtype found: %s' % filename)
    return dtype[0]


def dtype_from_nsmap(nsmap):
    for v in nsmap.values():
        cand = re.sub('(^.*/)', '', v)
        if cand in typeDirs:
            return cand
    return None


def get_flow_ref(exch, ns=None):
    f_uuid = find_tag(exch, 'referenceToFlowDataSet', ns=ns).attrib['refObjectId']
    f_uri = find_tag(exch, 'referenceToFlowDataSet', ns=ns).attrib['uri']
    f_dir = find_tag(exch, 'exchangeDirection', ns=ns).text
    return f_uuid, f_uri, f_dir


def grab_flow_name(o, ns=None):
    return ', '.join(chain(filter(len, [str(find_tag(o, k, ns=ns))
                                        for k in ('baseName',
                                                  'treatmentStandardsRoutes',
                                                  'mixAndLocationTypes',
                                                  'flowProperties')])))


def get_reference_flow(process, ns=None):
    try_ref = find_tag(process, 'referenceToReferenceFlow', ns=ns)
    if try_ref == '':
        return None, None, None  # multioutput, no specified reference
    else:
        ref_to_ref = int(try_ref)
    rf = [i for i in process['exchanges'].getchildren()
          if int(i.attrib['dataSetInternalID']) == ref_to_ref][0]
    return get_flow_ref(rf, ns=ns)


def get_reference_flow_property_id(flow, ns=None):
    # load or check the reference quantity
    return int(find_tag(flow, 'referenceToReferenceFlowProperty', ns=ns))
'''    rfp = [i for i in flow['flowProperties'].getchildren()
           if int(i.attrib['dataSetInternalID']) == ref_to_ref][0]
    rfp_uuid = find_tag(rfp, 'referenceToFlowPropertyDataSet', ns=ns)[0].attrib['refObjectId']
    rfp_uri = find_tag(rfp, 'referenceToFlowPropertyDataSet', ns=ns)[0].attrib['uri']
    return rfp_uuid, rfp_uri
'''


def get_reference_unit_group(q, ns=None):
    ref_to_ref = find_tag(q, 'referenceToReferenceUnitGroup', ns=ns)
    ug_uuid = ref_to_ref.attrib['refObjectId']
    ug_uri = ref_to_ref.attrib['uri']
    return ug_uuid, ug_uri


def get_exch_value(exch, ns=None):
    try:
        v = float(find_tag(exch, 'resultingAmount', ns=ns))
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


class IlcdArchive(LcArchive):
    """
    This class handles de-referencing for ILCD archives
    """

    def __init__(self, source, prefix=None, **kwargs):
        """
        Just instantiates the parent class.
        :param source: root of the archive
        :param prefix: difference between the internal path (ref) and the ILCD base
          (note: for local archives, this defaults to 'ILCD'; for remote arcnives it
           defaults to empty)
        :param quiet: forwarded to ArchiveInterface
        :return:
        """
        super(IlcdArchive, self).__init__(source, **kwargs)
        self.internal_prefix = prefix
        if prefix is not None:
            self._serialize_dict['prefix'] = prefix

        self._archive = FileStore(self.source, internal_prefix=prefix)

        if not self._archive.OK:
            print('Trying local ELCD reference')
            self._archive = FileStore(elcd3_local_fallback)
            if self._archive.OK:
                self._source = elcd3_local_fallback
        if not self._archive.OK:
            print('Falling back to ELCD Remote Reference')
            self._archive = FileStore(elcd3_remote_fallback, query_string='format=xml')
            if self._archive.OK:
                self._source = elcd3_remote_fallback
        if not self._archive.remote:
            self._archive.internal_prefix = 'ILCD'  # appends

    @property
    def _pathtype(self):
        return self._archive.pathtype

    @staticmethod
    def _path_from_ref(ref):
        """
        This fails if the filename has a version specification
        :param ref:
        :return:
        """
        return ref + '.xml'

    @staticmethod
    def _path_from_uri(uri):
        """
        just need to strip any leading '../' from the uri
        :param uri:
        :return:
        """
        uri = re.sub('^(\.\./)*', '', uri)
        return uri

    def _path_from_parts(self, dtype, uid, version=None):
        """
        aka 'path from parts'
        :param dtype: required
        :param uid: required
        :param version: optional [None]
        :return: a single (prefixed) path
        """
        assert _check_dtype(dtype)
        postpath = self._pathtype.join(typeDirs[dtype], uid)
        if version is not None:
            postpath += '_' + version
        return postpath + '.xml'

    def search_by_id(self, uid, dtype=None):
        return [i for i in self.list_objects(dtype=dtype) if re.search(uid, i, flags=re.IGNORECASE)]

    def list_objects(self, dtype=None):
        assert self._archive.remote is False, "Cannot list objects for remote archives"
        in_prefix = None
        if dtype is not None:
            in_prefix = typeDirs[dtype]
        for f in self._archive.listfiles(in_prefix=in_prefix):
            yield f

    def _fetch_filename(self, filename):
        return self._archive.readfile(filename)

    def _check_or_retrieve_child(self, uid, uri):
        child = self.__getitem__(uid)  # looks upstream first
        if child is None:
            child = self._fetch(uid, uri=uri)
        return child

    def _get_objectified_entity(self, filename):
        return objectify.fromstring(self._fetch_filename(filename))

    def _search_for_term(self, term, dtype=None):
        search_results = self.search_by_id(term, dtype=dtype)
        if len(search_results) > 0:
            self._print('Found Results:')
            [print(i) for i in search_results]
            if len(search_results) > 1:
                print('Please refine search')
                return None
            result = search_results[0]
            dtype = _extract_dtype(result, self._pathtype)
            return self.objectify(result, dtype=dtype)
        print('No results for %s' % term)
        return None

    def objectify(self, term, dtype=None, version=None, uri=None):
        if uri is not None:
            return self._get_objectified_entity(self._path_from_uri(uri))
        if dtype is None:
            try:
                dtype = _extract_dtype(term, self._pathtype)
            except ValueError:
                return self._search_for_term(term)

        try:
            uid = _extract_uuid(term)
        except AttributeError:
            # can't find UUID: search is required
            return self._search_for_term(term, dtype=dtype)

        # if we get here, uid is valid and dtype is valid
        entity = self.__getitem__(uid)  # checks upstream first (!! should this be local only?)
        if entity is not None:
            return entity

        try:
            # if we are a search result, this will succeed
            o = self._get_objectified_entity(term)
        except (KeyError, FileNotFoundError):
            # we are not a search result-- let's build the entity path
            try:
                o = self._get_objectified_entity(self._path_from_parts(dtype, uid, version=version))
            except (KeyError, FileNotFoundError):
                # still not found- maybe it isn't here
                return None
        return o

    def _create_unit(self, unit_ref):
        """
        UnitGroups aren't stored as full-fledged entities- they are stored as dicts inside quantities.
        :param unit_ref:
        :return:
        """
        dtype = _extract_dtype(unit_ref, self._pathtype)
        try:
            uid = _extract_uuid(unit_ref)
        except AttributeError:
            return super(IlcdArchive, self)._create_unit(unit_ref)
        filename = self._path_from_parts(dtype, uid)
        o = self._get_objectified_entity(filename)

        ns = find_ns(o.nsmap, 'UnitGroup')

        u = str(find_common(o, 'UUID'))
        reference_unit = int(find_tag(o, 'referenceToReferenceUnit', ns=ns))
        unitstring = str(o['units'].getchildren()[reference_unit]['name'])
        ref_unit = LcUnit(unitstring, unit_uuid=u)
        ref_unit.set_external_ref('%s/%s' % (typeDirs['UnitGroup'], u))

        unitconv = dict()
        for i in o['units'].getchildren():
            unitconv[str(i['name'])] = 1.0 / float(i['meanValue'])
        return ref_unit, unitconv

    def _create_quantity(self, o):
        """

        :param o: objectified FlowProperty
        :return:
        """
        u = str(find_common(o, 'UUID'))
        try_q = self[u]
        if try_q is not None:
            return try_q

        ns = find_ns(o.nsmap, 'FlowProperty')

        n = str(find_common(o, 'name'))

        c = str(find_common(o, 'generalComment'))

        ug, ug_uri = get_reference_unit_group(o, ns=ns)

        ug_path = self._pathtype.join('unitgroups', ug)  # need the path without extension- I know- it's all sloppy

        refunit, unitconv = self._create_unit(ug_path)

        q = LcQuantity(u, Name=n, ReferenceUnit=refunit, UnitConversion=unitconv, Comment=c)

        q.set_external_ref('%s/%s' % (typeDirs['FlowProperty'], u))

        self.add(q)

        return q

    @staticmethod
    def _create_dummy_flow_from_exch(uid, exch):
        n = str(find_common(exch, 'shortDescription'))
        print('Creating DUMMY flow (%s) with name %s' % (uid, n))
        return LcFlow(uid, Name=n, Comment='Dummy flow (HTTP or XML error)', Compartment=['dummy flows'])

    def _create_flow(self, o):
        """

        :param o: objectified flow
        :return: an LcFlow
        """
        u = str(find_common(o, 'UUID'))
        try_f = self[u]
        if try_f is not None:
            return try_f

        ns = find_ns(o.nsmap, 'Flow')
        n = grab_flow_name(o, ns=ns)

        c = str(find_common(o, 'generalComment'))

        cas = str(find_tag(o, 'CASNumber', ns=ns))

        cat = find_tags(o, 'category', ns='common')
        if cat == ['']:
            cat = find_tags(o, 'class', ns='common')
        cat = [str(i) for i in cat]

        if str(find_tag(o, 'typeOfDataSet', ns=ns)) == 'Elementary flow':
            f = LcFlow(u, Name=n, CasNumber=cas, Comment=c, Compartment=cat)
        else:
            f = LcFlow(u, Name=n, CasNumber=cas, Comment=c, Compartment=['Intermediate flows'], Class=cat)

        f.set_external_ref('%s/%s' % (typeDirs['Flow'], u))

        ref_to_ref = get_reference_flow_property_id(o, ns=ns)
        for fp in o['flowProperties'].getchildren():
            if int(fp.attrib['dataSetInternalID']) == ref_to_ref:
                is_ref = True
            else:
                is_ref = False
            val = float(find_tag(fp, 'meanValue', ns=ns))

            ref = find_tag(fp, 'referenceToFlowPropertyDataSet', ns=ns)
            rfp_uuid = ref.attrib['refObjectId']
            rfp_uri = ref.attrib['uri']

            try:
                q = self._check_or_retrieve_child(rfp_uuid, rfp_uri)
            except (HTTPError, XMLSyntaxError, KeyError):
                continue

            try:
                f.add_characterization(q, reference=is_ref, value=val)
            except DuplicateCharacterizationError:
                print('Duplicate Characterization in entity %s\n %s = %g' % (u, q, val))
                # let it go

        self.add(f)
        return f

    def _create_process_entity(self, o, ns):
        u = str(find_common(o, 'UUID'))
        try_p = self[u]
        if try_p is not None:
            return try_p

        n = ', '.join(chain(filter(len, [str(find_tag(o, k, ns=ns))
                                         for k in ('baseName',
                                                   'treatmentStandardsRoutes',
                                                   'mixAndLocationTypes',
                                                   'functionalUnitFlowProperties')])))

        try:
            g = find_tag(o, 'locationOfOperationSupplyOrProduction', ns=ns).attrib['location']
        except AttributeError:
            g = 'GLO'

        stt = {'begin': str(find_common(o, 'referenceYear')), 'end': str(find_common(o, 'dataSetValidUntil'))}

        c = str(find_common(o, 'generalComment'))

        cls = [str(i) for i in find_tags(o, 'class', ns='common')]

        p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                      Classifications=cls)

        self.add(p)

        p.set_external_ref('%s/%s' % (typeDirs['Process'], u))

        return p

    def _create_process(self, o):
        """

        :param o: objectified process
        :return:
        """
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
                f = self._check_or_retrieve_child(f_id, f_uri)
            except (HTTPError, XMLSyntaxError, KeyError):
                u = str(find_common(o, 'UUID'))
                print('In UUID %s:' % u)
                f = self._create_dummy_flow_from_exch(f_id, exch)
                self.add(f)
            v = get_exch_value(exch, ns=ns)
            cmt = find_tag(exch, 'generalComment', ns=ns)
            exch_list.append((f, f_dir, v, cmt))

        p = self._create_process_entity(o, ns)

        for flow, f_dir, val, cmt in exch_list:
            x = p.add_exchange(flow, f_dir, reference=None, value=val,
                               add_dups=True)  # add_dups: poor quality control on ELCD
            if len(cmt) > 0:
                x.comment = cmt
            if rf == flow.get_uuid() and rf_dir == f_dir:
                p.add_reference(flow, f_dir)

        return p

    def _fetch(self, term, dtype=None, version=None, **kwargs):
        """
        fetch an object from the archive by reference.

        term is either: a uid and a dtype (and optional version) OR a filename
        dtype MUST be specified as kwarg for remote archives; otherwise will search
        :param term:
        :return:
        """
        if dtype is None:
            try:
                dtype = _extract_dtype(term, self._pathtype)
            except ValueError:
                pass

        o = self.objectify(term, dtype=dtype, version=version, **kwargs)
        if o is None:
            return None

        if dtype is None:
            dtype = dtype_from_nsmap(o.nsmap)

        if dtype == 'Flow':
            try:
                return self._create_flow(o)
            except KeyError:
                print('KeyError on term %s dtype %s version %s' % (term, dtype, version))
        elif dtype == 'Process':
            return self._create_process(o)
        elif dtype == 'FlowProperty':
            return self._create_quantity(o)
        else:
            return o

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

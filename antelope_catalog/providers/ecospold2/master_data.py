from lxml import objectify
from ..file_store import FileStore


class EcoSpoldMasterData(object):

    masters = dict()

    def _read_master(self, filename):
        if filename in self.masters:
            return self.masters[filename]
        else:
            try:
                o = objectify.fromstring(self._fs.readfile(filename + '.xml'))
            except FileNotFoundError:
                o = None
            self.masters[filename] = o
            return o

    __master_units = None
    __master_props = None
    __elem = None
    __inter = None

    @property
    def master_units(self):
        if self.__master_units is None:
            o = self._read_master('Units')
            if o is None:
                self.__master_units = {}
            else:
                self.__master_units = {k.attrib['id']: k for k in o.iterfind('unit', namespaces=o.nsmap)}
        return self.__master_units

    @property
    def master_properties(self):
        if self.__master_props is None:
            o = self._read_master('Properties')
            if o is None:
                self.__master_props = {}
            else:
                self.__master_props = {k.attrib['id']: k for k in o.iterfind('property', namespaces=o.nsmap)}
        return self.__master_props

    @property
    def elementary_exchanges(self):
        if self.__elem is None:
            o = self._read_master('ElementaryExchanges')
            if o is None:
                self.__elem = {}
            else:
                self.__elem = {k.attrib['id']: k for k in o.iterfind('elementaryExchange', namespaces=o.nsmap)}
        return self.__elem

    @property
    def intermediate_exchanges(self):
        if self.__inter is None:
            o = self._read_master('IntermediateExchanges')
            if o is None:
                self.__inter = {}
            else:
                self.__inter = {k.attrib['id']: k for k in o.iterfind('intermediateExchange', namespaces=o.nsmap)}
        return self.__inter

    def __init__(self, source, internal_prefix='MasterData'):
        self._fs = FileStore(source, internal_prefix=internal_prefix)

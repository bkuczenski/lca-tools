import os
import re
from lxml import objectify

from .data_source import DataSource, DataCollection
from .ecoinvent_lcia import EcoinventLciaConfig, EI_LCIA_SPREADSHEETS
from antelope_catalog.providers.file_store import FileStore

FILE_PREFIX = ('current_Version_', 'ecoinvent ')
FILE_EXT = ('7z', 'zip')
ECOINVENT_SYS_MODELS = ('apos', 'conseq', 'cutoff', 'undefined')
MODELMAP = {
    'apos': ('apos',),
    'conseq': ('consequential', 'consequential_longterm'),
    'cutoff': ('cutoff',),
    'undefined': ('undefined', )
}

E_CFG = {'hints': [  # cover elementary contexts that need directional hints, plus units used in elementary flows only
    ['context', 'air', 'to air'],
    ['context', 'water', 'to water'],
    ['quantity', 'EcoSpold Quantity kg', 'Mass'],
    ['quantity', 'EcoSpold Quantity m2', 'Area'],
    ['quantity', 'EcoSpold Quantity MJ', 'Net Calorific Value'],
    ['quantity', 'EcoSpold Quantity m2*year', 'Area*time'],
    ['quantity', 'EcoSpold Quantity m3', 'Volume']
]}  # omitted: kBq, EUR2005, m3*year


class Ecoinvent3Base(DataSource):

    _ds_type = 'EcospoldV2Archive'

    def __init__(self, data_root, version, model, **kwargs):
        assert model in ECOINVENT_SYS_MODELS
        super(Ecoinvent3Base, self).__init__(data_root=data_root, **kwargs)
        self._version = version
        self._model = model

    @property
    def _lci_ref(self):
        if self.lci_source is not None:
            yield 'local.ecoinvent.lci.%s.%s' % (self._version, self._model)

    @property
    def _inv_ref(self):
        if self.inv_source is not None:
            yield 'local.ecoinvent.%s.%s' % (self._version, self._model)

    @property
    def references(self):
        for x in self._lci_ref:
            yield x
        for x in self._inv_ref:
            yield x

    def interfaces(self, ref):
        yield 'inventory'

    def make_resources(self, ref):
        if ref in self._lci_ref:
            yield self._make_resource(ref, self.lci_source, interfaces='inventory', prefix='datasets', config=E_CFG)
        elif ref in self._inv_ref:
            if self._model == 'undefined':
                yield self._make_resource(ref, self.inv_source, interfaces='inventory', prefix='datasets - public',
                                          linked=False, config=E_CFG)
            else:
                yield self._make_resource(ref, self.inv_source, interfaces='inventory', prefix='datasets', config=E_CFG)

    def _fname(self, ftype=None):
        precheck = os.path.join(self.root, self._model)
        if ftype is not None:
            precheck += '_%s' % ftype
        if os.path.isdir(precheck):
            return precheck
        for pf in FILE_PREFIX:
            for mod in MODELMAP[self._model]:
                if ftype is None:
                    pname = '%s%s_%s_ecoSpold02' % (pf, self._version, mod)
                else:
                    pname = '%s%s_%s_%s_ecoSpold02' % (pf, self._version, mod, ftype)
                dsource = os.path.join(self.root, self._version, pname)
                if os.path.isdir(dsource):
                    return dsource
                for ext in FILE_EXT:
                    fname = '%s.%s' % (pname, ext)
                    source = os.path.join(self.root, self._version, fname)
                    if os.path.exists(source):
                        return source

    @property
    def inv_source(self):
        return self._fname()

    @property
    def lci_source(self):
        return self._fname('lci')

    def elementary_exchanges(self):
        for k in (self.inv_source, self.lci_source):
            try:
                fs = FileStore(k, internal_prefix='MasterData')
            except TypeError:
                continue
            try:
                o = objectify.fromstring(fs.readfile('ElementaryExchanges.xml'))
            except FileNotFoundError:
                continue
            yield o


class EcoinventConfig(DataCollection):
    """
    Ecoinvent Configurator
    This DataCollection generates LcResource objects for ecoinvent archives.  The only required input is the root
    folder containing the ecoinvent data.  Within this folder should be subfolders named after the major and minor
    version of the database (e.g. "3.3"). Within the subfolders should be the archives.

    Archives must be named according to ecoinvent's various conventions:
     #1#2_#3_ecoSpold02#4
    where
     #1 is one of ('current_Version_' or 'ecoinvent ')
     #2 is the major and minor version ('2.2', '3.01', '3.1', '3.2', etc)
     #3 is the system model ('apos', 'cutoff', 'consequential', or 'consequential_longterm')
     #4 is either omitted (in the case of an expanded directory) or an archive extension ('.zip' or '.7z')

    Within the archives, either compressed or expanded, the datasets are assumed to be in a subfolder called 'datasets'

    The class does not currently support loading undefined data collections, but this could be added in the future.

    """
    @property
    def ecoinvent_versions(self):
        for d in os.listdir(self._root):
            if os.path.isdir(os.path.join(self._root, d)):
                if re.match('[23]\.[0-9]+', d):
                    yield d
                if d.lower() == 'lcia':
                    yield d

    def factory(self, data_root, **kwargs):
        for v in self.ecoinvent_versions:
            if v.lower() == 'lcia':
                lcia_path = os.path.join(data_root, v)
                for ver, filename in EI_LCIA_SPREADSHEETS.items():
                    if os.path.exists(os.path.join(lcia_path, filename)):
                        yield EcoinventLciaConfig(lcia_path, version=ver)
            else:
                for m in ECOINVENT_SYS_MODELS:
                    yield Ecoinvent3Base(data_root, v, m, **kwargs)
                    yield EcoinventLciaConfig(os.path.join(data_root, v), version=v)

    def elementary_exchanges(self):
        print('DEPRECATED. please use configured EcospoldV2 Archives instead. (??)')
        for k, v in sorted(self._sources.items(), key=lambda x: x[0], reverse=True):
            if hasattr(v, 'elementary_exchanges'):
                for e in v.elementary_exchanges():
                    yield e

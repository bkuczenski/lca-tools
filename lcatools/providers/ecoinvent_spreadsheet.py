from __future__ import print_function, unicode_literals

from lcatools.providers.base import NsUuidArchive
from lcatools.entities import LcProcess, LcFlow, LcQuantity
from lcatools.providers.ecospold2 import EcospoldV2Archive
from lcatools.interact import pick_reference

import pandas as pd
import uuid
import os


class EcoinventSpreadsheet(NsUuidArchive):
    """
    A class for implementing the basic interface based on the contents of an ecoinvent
    "activity overview" spreadsheet. Note the lack of specification for such a spreadsheet.
    """

    def _little_read(self, sheetname):
        print('Reading %s ...' % sheetname)
        return pd.read_excel(self.ref, sheetname=sheetname).fillna('')

    def __init__(self, ref, version='Unspecified', internal=False, data_dir=None, model=None, **kwargs):
        """
        :param ref:
        :param version:
        :param internal:
        :param kwargs: quiet, upstream
        """
        super(EcoinventSpreadsheet, self).__init__(ref, **kwargs)
        self.version = version
        self.internal = internal

        self._serialize_dict['version'] = version
        self._serialize_dict['internal'] = version

        # these things are query-only, for foreground use
        self._data_dir = data_dir
        self._model = model
        if self._data_dir is not None:
            if model == 'undefined':
                self.fg = EcospoldV2Archive(self._fg_filename, prefix='datasets - public')
                self.bg = None
            else:
                self.fg = EcospoldV2Archive(self._fg_filename, prefix='datasets')
                self.bg = EcospoldV2Archive(self._bg_filename, prefix='datasets')
        else:
            self.fg = None
            self.bg = None

    @property
    def _fg_filename(self):
        if self._data_dir is None:
            raise AttributeError('No data directory')
        else:
            fn = os.path.join(self._data_dir, '_'.join(['current_Version', self.version, self._model,
                                                        'ecoSpold02']) + '.zip')
            print('Loading FG from %s' % fn)
            return fn

    @property
    def _bg_filename(self):
        if self._data_dir is None:
            raise AttributeError('No data directory')
        else:
            fn = os.path.join(self._data_dir, '_'.join(['current_Version', self.version, self._model,
                                                        'lcia', 'ecoSpold02']) + '.zip')
            print('Loading BG from %s' % fn)
            return fn

    def fg_lookup(self, process, flow=None):
        """
        Supply an exchange or a process-- if process, return a list; if exchange, return a dataset or None
        :param process:
        :param flow:
        :return:
        """
        if self.fg is None:
            print('No foreground data')
            return super(EcoinventSpreadsheet, self).fg_lookup(process)
        else:
            if flow is None:
                for ds in self.fg.list_datasets(process.get_uuid()):
                    self.fg.retrieve_or_fetch_entity(ds)
                p = self.fg[process.get_uuid()]
                print('This process has multiple allocations. Select reference flow:')
                ref = pick_reference(p)
                if ref is None:
                    return p.exchanges()
                return p.allocated_exchanges(ref)
            else:
                p = self.fg.retrieve_or_fetch_entity('_'.join([process.get_uuid(), flow.get_uuid()]) + '.spold')
                return p.allocated_exchanges(flow)

    def bg_lookup(self, process, ref_flow=None, quantities=None, scenario=None, flowdb=None):
        if self.bg is None:
            raise AttributeError('No background')
        else:
            if ref_flow is None:
                for ds in self.bg.list_datasets(process.get_uuid()):
                    self.bg.retrieve_or_fetch_entity(ds)
                p = self.bg[process.get_uuid()]
                print('This process has multiple allocations. Select reference flow:')
                ref_flow = pick_reference(p)
            return self.bg.retrieve_lcia_scores('_'.join([process.get_uuid(), ref_flow.get_uuid()]) + '.spold',
                                                quantities=quantities)

    def _create_quantity(self, unitstring):
        """
        In ecoinvent activity overview spreadsheets, quantities are only units, defined by string. 'properties'
        are additionally defined with name and unit together, but because the prinicpal units don't have names
        (and because for the time being we are neglecting the F-Q relation), we ignore them.
        :param unitstring:
        :return:
        """
        try_q = self.quantity_with_unit(unitstring)
        if try_q is None:
            ref_unit, _ = self._create_unit(unitstring)
            u = self._key_to_id(unitstring)

            q = LcQuantity(u, Name='Ecoinvent Spreadsheet Quantity %s' % unitstring,
                           ReferenceUnit=ref_unit, Comment=self.version)
            q.set_external_ref(unitstring)
            self.add(q)
        else:
            q = try_q

        return q

    def _create_flow(self, u, unit, ext_ref, Name=None, Compartment=None, **kwargs):
        upstream_key = ', '.join([Name] + Compartment)
        f = self._try_flow(u, upstream_key)
        if f is None:
            f = LcFlow(u, Name=Name, Compartment=Compartment, **kwargs)
            f.set_external_ref(ext_ref)
            q = self._create_quantity(unit)
            if q is None:
                raise ValueError
            f.add_characterization(quantity=q, reference=True)
            self.add(f)
        else:
            f.update(kwargs)

    def _create_quantities(self, _elementary, _intermediate):
        unitname = 'unit' if self.internal else 'unitName'
        units = set(_elementary[unitname].unique().tolist()).union(
            set(_intermediate[unitname].unique().tolist()))
        for u in units:
            self._create_quantity(u)

    @staticmethod
    def _elementary_key(row):
        return '%s [%s, %s]' % (row['name'], row['compartment'], row['subcompartment'])

    def _external_intermediate(self, _intermediate):
        """
        name, unitName, CAS, synonyms, comment
        :return:
        """
        print('Handling intermediate exchanges [public spreadsheet]')
        for index, row in _intermediate.iterrows():
            n = row['name']
            u = self._key_to_id(n)
            self._create_flow(u, row['unitName'], n, Name=n, CasNumber=row['CAS'],
                              Compartment=['Intermediate flow'], Comment=row['comment'],
                              Synonyms=row['synonyms'])

    def _external_elementary(self, _elementary):
        """
        name; compartment; subcompartment; unitName; casNumber; formula; synonyms
        """
        print('Handling elementary exchanges [public spreadsheet]')
        for index, row in _elementary.iterrows():
            key = self._elementary_key(row)
            u = self._key_to_id(key)
            cat = [row['compartment'], row['subcompartment']]
            self._create_flow(u, row['unitName'], key, Name=row['name'], CasNumber=row['casNumber'],
                              Compartment=cat, Comment='', Formula=row['formula'],
                              Synonyms=row['synonyms'])

    def _internal_intermediate(self, _intermediate):
        """
        intermediate flow UUIDs should be hashes of just the name, for easy lookup.
        for internal, we only want name and unit
        :return:
        """
        print('Handling intermediate exchanges [internal spreadsheet]')
        inter = _intermediate[_intermediate[:2]].drop_duplicates()
        for index, row in inter.iterrows():
            n = row['name']
            u = self._key_to_id(n)
            self._create_flow(u, row['unit'], n, Name=n, Compartment=['Intermediate flow'],
                              CasNumber='', Comment='')

    def _internal_elementary(self, _elementary):
        """
        name; compartment; subcompartment; unit; formula; CAS; property; property amount; property unit
        keep first 6
        :return:
        """
        print('Handling elementary exchanges [internal spreadsheet]')
        int_elem = _elementary[_elementary[:6]].drop_duplicates()  # just take first 6 columns

        for index, row in int_elem.iterrows():
            key = self._elementary_key(row)
            u = self._key_to_id(key)
            n = row['name']
            cat = [row['compartment'], row['subcompartment']]
            self._create_flow(u, row['unit'], key, Name=n, Compartment=cat,
                              CasNumber=row['CAS'], Comment='', Formula=row['formula'])

    def load_activities(self):
        print('Handling activities...')
        _activity = self._little_read('activity overview')
        for index, row in _activity.iterrows():
            if self.internal:
                u = row['Activity UUID']
            else:
                u = row['activity uuid']

            u = uuid.UUID(u)

            if self[u] is None:
                """
                create the process
                """
                n = row['activityName']
                if self.internal:
                    g = row['Geography']
                    st = 'interval(%s, %s)' % (row['Start'], row['End'])
                    c = row['Tags']

                else:
                    g = row['geography']
                    st = {'begin': row['start date'], 'end': row['end date']}
                    c = row['tags']

                p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=st)
                try:
                    if self.internal:
                        p['TechnologyLevel'] = row['Technology Level']
                    else:
                        p['TechnologyLevel'] = row['technologyLevel']
                except KeyError:
                    pass

                try:
                    p['IsicClass'] = row['ISIC class']
                    p['IsicNumber'] = row['ISIC number']
                except KeyError:
                    pass
                self.add(p)
            else:
                p = self[u]

            """
            Now, handle the flows
            """
            if self.internal:
                exch_name = row['Product']
                ref_check = 'Product Type'
            else:
                exch_name = row['product name']
                ref_check = 'group'

            exch_flow = self[self._key_to_id(exch_name)]
            if row[ref_check] == 'ReferenceProduct':
                p.add_reference(exch_flow, 'Output')

            p.add_exchange(exch_flow, 'Output')

    def _load_all(self):
        _elementary = self._little_read('elementary exchanges')
        _intermediate = self._little_read('intermediate exchanges')
        self._create_quantities(_elementary, _intermediate)
        self.check_counter('quantity')
        if self.internal:
            self._internal_elementary(_elementary)
            self.check_counter('flow')
            self._internal_intermediate(_intermediate)
            self.check_counter('flow')
        else:  # external
            self._external_elementary(_elementary)
            self.check_counter('flow')
            self._external_intermediate(_intermediate)
            self.check_counter('flow')
        self.load_activities()
        self.check_counter('process')

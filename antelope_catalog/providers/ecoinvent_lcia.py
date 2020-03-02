"""
The point of the ecoinvent LCIA methods is to ste^H^H^Hadapt the hard work of the Ecoinvent Centre, so as to develop
LCIA methods and results for ecoinvent LCI that match their own results.

2020-01-11
WARNING: this assumes mass as reference quantity by default, which leads to obvious quantity conversion errors for
non-mass-reference indicators like land occupation.

Solution requires access to Ecoinvent metadata in order to determine the reference quantities for flows.
"""

from __future__ import print_function, unicode_literals

from lcatools.archives import BasicArchive
# from lcatools.literate_float import LiterateFloat

from lcatools.entities import LcQuantity

import os
import xlrd
import time

EI_LCIA_VERSION = '3.1'
EI_LCIA_NSUUID = '46802ca5-8b25-398c-af10-2376adaa4623'  # use the same value for all implementations so they lookup

Ecoinvent_Indicators = os.path.join(os.path.dirname(__file__), 'data',
                                    'list_of_methods_and_indicators_ecoinvent_v3.2.xlsx')

EI_LCIA_ERRORS = {
    ('ReCiPe Midpoint (E) (obsolete)', 'water depletion', 'WDP', 
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0}, 
    ('ReCiPe Midpoint (E) w/o LT (obsolete)', 'water depletion w/o LT', 'WDP w/o LT', 
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0},
    ('ReCiPe Midpoint (H) (obsolete)', 'water depletion', 'WDP', 
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0},
    ('ReCiPe Midpoint (H) w/o LT (obsolete)', 'water depletion w/o LT', 'WDP w/o LT', 
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0},
    ('ReCiPe Midpoint (I) (obsolete)', 'water depletion', 'WDP', 
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0},
    ('ReCiPe Midpoint (E)', 'water depletion', 'WDP',
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0},
    ('ReCiPe Midpoint (E) w/o LT', 'water depletion w/o LT', 'WDP w/o LT',
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0},
    ('ReCiPe Midpoint (H)', 'water depletion', 'WDP',
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0},
    ('ReCiPe Midpoint (H) w/o LT', 'water depletion w/o LT', 'WDP w/o LT',
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0},
    ('ReCiPe Midpoint (I)', 'water depletion', 'WDP',
     'Water, unspecified natural origin', 'natural resource', 'in water'): {None: 1.0},
}


_EI_LCIA_ERR_FLOWS = set(k[3] for k in EI_LCIA_ERRORS.keys())  # some minimal optimization

class EcoinventLcia(BasicArchive):
    """
    Class to import the Ecoinvent LCIA implementation and construct a flow-cf-quantity catalog.
    The external keys are concatenations of the three
    """
    _ns_uuid_required = True

    _drop_columns = ['Change?']

    @staticmethod
    def _sheet_to_rows(sheet):
        g = sheet.get_rows()
        headings = [h.value for h in next(g)]

        rows = []
        for row in g:
            d = dict()
            for i, h in enumerate(headings):
                if h in EcoinventLcia._drop_columns:
                    continue
                d[h] = row[i].value
            rows.append(d)
        return rows

    def _load_xl_rows(self):
        """
        25+sec just to open_workbook for EI3.1 LCIA (pandas is similar)
        note: this is down to 15.5 sec
        note: back up to 20 sec for EI3.5
        """
        b = self._xls.sheet_by_name(self._sheet_name)

        self._xl_rows = self._sheet_to_rows(b)

    def __init__(self, source, ei_archive=None, ref=None, mass_quantity=None,
                 version=EI_LCIA_VERSION, ns_uuid=EI_LCIA_NSUUID, static=True, **kwargs):
        """
        EI_LCIA_VERSION default is presently 3.1 for the spreadsheet named 'LCIA implementation v3.1 2014_08_13.xlsx'

        :param source:
        :param ei_archive: required to determine reference quantities for flows
        :param ref: hard-coded 'local.ecoinvent.[EI_LCIA_VERSION].lcia'; specify at instantiation to override
        :param mass_quantity:
        :param version: default is '3.1'
        :param ns_uuid: required; default / convention is '46802ca5-8b25-398c-af10-2376adaa4623'
        :param static: this archive type is always static
        :param kwargs: quiet, upstream
        """
        version = str(version)
        if ref is None:
            ref = '.'.join(['local', 'ecoinvent', version, 'lcia'])
        super(EcoinventLcia, self).__init__(source, ref=ref, ns_uuid=ns_uuid, static=True, **kwargs)
        self._xl_rows = []
        self._version = version
        self._wb = None
        if ei_archive is None:
            print('Warning: no ecoinvent archive! Non-mass methods will be broken!')
        self._ei_archive = ei_archive

        # mass = mass_quantity or LcQuantity.new('Mass', self._create_unit('kg')[0])
        # self.add(mass)
        # self._mass = mass

    @property
    def _xls(self):
        if self._wb is None:
            start = time.time()
            self._wb = xlrd.open_workbook(self.source)
            print('Opened workbook; Elapsed time %.3f' % (time.time() - start))
        return self._wb

    @staticmethod
    def _quantity_key(row):
        return ', '.join([row[k] for k in ('method', 'category', 'indicator')])

    @staticmethod
    def _error_key(row):
        return tuple(row[k] for k in ('method', 'category', 'indicator', 'name', 'compartment', 'subcompartment'))

    @property
    def _value_tag(self):
        return 'CF %s' % self._version

    @property
    def _sheet_name(self):
        if self._version == '3.1':
            return 'impact methods'
        else:
            return 'CFs'

    def _create_quantity(self, row):
        """
        here row is a dict from self._xl_rows
        :param row:
        :return:
        """
        key = self._quantity_key(row)
        try_q = self[key]
        if try_q is None:
            unit, _ = self._create_unit(row['unit'])

            q = LcQuantity(key, Name=key, referenceUnit=unit, Comment='Ecoinvent LCIA implementation',
                           Method=row['method'], Category=row['category'], Indicator=row['indicator'])
            self.add(q)
        else:
            q = try_q
        return q

    def _create_all_quantities(self):
        if float(self._version) <= 3.2:
            self._create_all_quantities_3_2()
        else:
            b = self._xls.sheet_by_name('units')
            qs = self._sheet_to_rows(b)
            for row in qs:
                row['unit'] = row['impact score unit']
                self._create_quantity(row)

    def _create_all_quantities_3_2(self):
        """
        This should not be used-- quantities should be taken from the source file
        :return:
        """
        x = xlrd.open_workbook(Ecoinvent_Indicators)
        w = x.sheet_by_index(0)

        qs = self._sheet_to_rows(w)
        for row in qs:
            self._create_quantity(row)

    def _get_value(self, row):
        if 'Known issue' in row:
            if row['Known issue'] != '':
                return row['Known issue']
        return row[self._value_tag]

    def _check_row(self, row):
        if row['name'] in _EI_LCIA_ERR_FLOWS:
            k = self._error_key(row)
            if k in EI_LCIA_ERRORS:
                err = EI_LCIA_ERRORS[k]
                if self._version in err:
                    return err[self._version]
                elif None in err:
                    return err[None]
        return self._get_value(row)

    def _load_all(self):
        self._create_all_quantities()
        if len(self._xl_rows) == 0:
            self._load_xl_rows()
        for row in self._xl_rows:
            q = self._create_quantity(row)
            try:
                v = self._check_row(row)  # LiterateFloat(self._get_value(row), **row)
            except ValueError:
                print('Skipping row %s' % row)
                continue
            cx = self.tm.add_context((row['compartment'], row['subcompartment']), origin=self.ref)
            fb = row['name']
            try:
                f = next(self._ei_archive.tm.flows_for_flowable(fb))
            except StopIteration:
                print('Unable to find a flow for flowable "%s" -- skipping' % fb)
                # raise KeyError
            if self[f.reference_entity.external_ref] is None:
                self.add_entity_and_children(f.reference_entity)
            self.tm.add_characterization(row['name'], f.reference_entity, q, v, context=cx, origin=self.ref)

        self.check_counter()

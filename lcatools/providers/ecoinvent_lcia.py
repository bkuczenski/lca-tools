"""
At this point I am really straight repeating a lot of Chris's work. but who can deal with his unwieldy data structures?
"""

from __future__ import print_function, unicode_literals

from lcatools.providers.base import NsUuidArchive
from lcatools.characterizations import Characterization
from lcatools.literate_float import LiterateFloat

from lcatools.entities import LcFlow, LcQuantity

import os
import xlrd

Ecoinvent_Indicators = os.path.join(os.path.dirname(__file__), 'data',
                                    'list_of_methods_and_indicators_ecoinvent_v3.2.xlsx')


class EcoinventLcia(NsUuidArchive):
    """
    Class to import the Ecoinvent LCIA implementation and construct a flow-cf-quantity catalog.
    The external keys are concatenations of the three
    """

    _drop_fields = ['Change?']

    @staticmethod
    def _sheet_to_rows(sheet):
        g = sheet.get_rows()
        headings = [h.value for h in next(g)]

        rows = []
        for row in g:
            d = dict()
            for i, h in enumerate(headings):
                if h in EcoinventLcia._drop_fields:
                    continue
                d[h] = row[i].value
            rows.append(d)
        return rows

    def _load_xl_rows(self):
        """
        25+sec just to open_workbook for EI3.1 LCIA (pandas is similar)
        """
        b = xlrd.open_workbook(self.ref).sheet_by_name(self._sheet_name)

        self._xl_rows = self._sheet_to_rows(b)

    def __init__(self, ref, sheet_name='impact methods', mass_quantity=None,
                 value_tag='CF 3.1', **kwargs):
        """

        :param ref:
        :param sheet_name: 'impact methods'
        :param ns_uuid:
        :param mass_quantity:
        :param value_tag: 'CF 3.1'
        :param kwargs: quiet, upstream
        """
        super(EcoinventLcia, self).__init__(ref, **kwargs)
        self._xl_rows = []
        self._sheet_name = sheet_name
        self._value_tag = value_tag

        mass = mass_quantity or LcQuantity.new('Mass', self._create_unit('kg')[0])
        self.add(mass)
        self._mass = mass

    @staticmethod
    def _quantity_key(row):
        return ', '.join([row[k] for k in ('method', 'category', 'indicator')])

    @staticmethod
    def _flow_key(row):
        return ', '.join([row[k] for k in ('name', 'compartment', 'subcompartment')])

    def _create_quantity(self, row):
        """
        here row is a dict from self._xl_rows
        :param row:
        :return:
        """
        key = self._quantity_key(row)
        u = self.key_to_id(key)
        try_q = self[u]
        if try_q is None:
            unit, _ = self._create_unit(row['unit'])

            q = LcQuantity(u, Name=key, referenceUnit=unit, Comment='Ecoinvent LCIA implementation',
                           method=row['method'], category=row['category'], indicator=row['indicator'])
            q.set_external_ref(key)
            self.add(q)
        else:
            q = try_q
        return q

    def _create_all_quantities(self):
        x = xlrd.open_workbook(Ecoinvent_Indicators)
        w = x.sheet_by_index(0)

        qs = self._sheet_to_rows(w)
        for row in qs:
            self._create_quantity(row)

    def _create_flow(self, row):
        key = self._flow_key(row)
        u = self.key_to_id(key)
        try_f = self[u]
        if try_f is None:
            f = LcFlow(u, Name=row['name'], CasNumber='', Compartment=[row['compartment'], row['subcompartment']],
                       Comment=row['note'], referenceQuantity=self._mass)
            f.set_external_ref(key)
            self.add(f)
        else:
            f = try_f
        return f

    def _get_value(self, row):
        if row['Known issue'] != '':
            return row['Known issue']
        else:
            return row[self._value_tag]

    def load_all(self):
        self._create_all_quantities()
        if len(self._xl_rows) == 0:
            self._load_xl_rows()
        for row in self._xl_rows:
            f = self._create_flow(row)
            q = self._create_quantity(row)
            v = LiterateFloat(self._get_value(row), **row)
            self._add_characterization(Characterization(flow=f, quantity=q, value=v))
        self.check_counter()

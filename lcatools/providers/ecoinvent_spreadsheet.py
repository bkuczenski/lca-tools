from lcatools.interfaces import BasicInterface
from lcatools.entities import LcProcess, LcFlow, LcQuantity, LcUnit
from lcatools.exchanges import Exchange

import pandas as pd
import uuid


class EcoinventSpreadsheet(BasicInterface):
    """
    A class for implementing the basic interface based on the contents of an ecoinvent
    "activity overview" spreadsheet. Note the lack of specification for such a spreadsheet.
    """

    def _little_read(self, sheetname):
        print('Reading %s ...' % sheetname)
        return pd.read_excel(self.ref, sheetname=sheetname).fillna('')

    def __init__(self, ref, version='Unspecified', internal=False):
        super(EcoinventSpreadsheet, self).__init__(ref)
        self.version = version
        self.internal = internal
        self._ns_uuid = uuid.uuid4()
        self._activity = self._little_read('activity overview')
        self._elementary = self._little_read('elementary exchanges')
        self._intermediate = self._little_read('intermediate exchanges')

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
            ref_unit = LcUnit(unitstring)

            q = LcQuantity.new('Ecoinvent Spreadsheet Quantity %s' % unitstring, ref_unit, Comment=self.version)
            q.set_external_ref(unitstring)
            self[q.get_uuid()] = q
        else:
            q = try_q

        return q

    def create_quantities(self):
        unitname = 'unit' if self.internal else 'unitName'
        units = set(self._elementary[unitname].unique().tolist()).union(
            set(self._intermediate[unitname].unique().tolist()))
        for u in units:
            self._create_quantity(u)

    def _external_intermediate(self):
        """
        name, unitName, CAS, synonyms, comment
        :return:
        """
        print('Handling intermediate exchanges [public spreadsheet]')
        for index, row in self._intermediate.iterrows():
            u = uuid.uuid3(self._ns_uuid, row['name'])
            if u in self._entities:
                continue
            q = self.quantity_with_unit(row['unitName'])
            f = LcFlow(u, Name=row['name'], CasNumber=row['CAS'],
                       Compartment=['Intermediate flow'], ReferenceQuantity=q, Comment=row['comment'],
                       Synonyms=row['synonyms'])
            self[u] = f

    def _external_elementary(self):
        """
        name; compartment; subcompartment; unitName; casNumber; formula; synonyms
        """
        print('Handling elementary exchanges [public spreadsheet]')
        for index, row in self._elementary.iterrows():
            u = uuid.uuid3(self._ns_uuid, row.to_string())
            if u in self._entities:
                continue
            q = self.quantity_with_unit(row['unitName'])
            cat = [row['compartment'], row['subcompartment']]
            f = LcFlow(u, Name=row['name'], CasNumber=row['casNumber'],
                       Compartment=cat, ReferenceQuantity=q, Comment='',
                       Formula=row['formula'],
                       Synonyms=row['synonyms'])
            self[u] = f

    def _internal_intermediate(self):
        """
        intermediate flow UUIDs should be hashes of just the name, for easy lookup.
        for internal, we only want name and unit
        :return:
        """
        print('Handling intermediate exchanges [internal spreadsheet]')
        inter = self._intermediate[self._intermediate[:2]].drop_duplicates()
        for index, row in inter.iterrows():
            u = uuid.uuid3(self._ns_uuid, row['name'])
            if u in self._entities:
                continue
            q = self.quantity_with_unit(row['unit'])
            f = LcFlow(u, Name=row['name'], ReferenceQuantity=q, Compartment=['Intermediate flow'],
            CasNumber='', Comment='')
            self[u] = f

    def _internal_elementary(self):
        """
        name; compartment; subcompartment; unit; formula; CAS; property; property amount; property unit
        keep first 6
        :return:
        """
        print('Handling elementary exchanges [internal spreadsheet]')
        int_elem = self._elementary[self._elementary[:6]].drop_duplicates()  # just take first 6 columns

        for index, row in int_elem.iterrows():
            u = uuid.uuid3(self._ns_uuid, row.to_string())
            if u in self._entities:
                continue
            cat = [row['compartment'], row['subcompartment']]
            q = self.quantity_with_unit(row['unit'])
            f = LcFlow(u, Name=row['name'], ReferenceQuantity=q, Compartment=cat,
                       CasNumber=row['CAS'], Comment='', Formula=row['formula'])
            self[u] = f

    def load_activities(self):
        print('Handling activities...')
        for index, row in self._activity.iterrows():
            if self.internal:
                u = row['Activity UUID']
            else:
                u = row['activity uuid']

            u = uuid.UUID(u)

            if u not in self._entities:
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
                    st = 'interval(%s, %s)' % (row['start date'], row['end date'])
                    c = row['tags']

                p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=st)
                if self.internal:
                    p['TechnologyLevel'] = row['Technology Level']
                else:
                    p['TechnologyLevel'] = row['technologyLevel']

                p['IsicClass'] = row['ISIC class']
                p['IsicNumber'] = row['ISIC number']
                self[u] = p

            """
            Now, handle the flows
            """
            if self.internal:
                exch_name = row['Product']
                ref_check = 'Product Type'
            else:
                exch_name = row['product name']
                ref_check = 'group'

            exch_flow = self[uuid.uuid3(self._ns_uuid, exch_name)]
            exch = Exchange(self[u], exch_flow, 'Output')

            if row[ref_check] == 'ReferenceProduct':
                self[u]['ReferenceExchange'] = exch

            self._add_exchange(exch)

    def load_all(self):
        self.create_quantities()
        if self.internal:
            self._internal_elementary()
            self._internal_intermediate()
        else:  # external
            self._external_elementary()
            self._external_intermediate()
        self.load_activities()


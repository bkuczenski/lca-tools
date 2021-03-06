import re

from lcatools.implementations import QuantityImplementation
from .q_info import q_info_21 as q_info


CAS_regexp = re.compile('^[0-9]{,6}-[0-9]{2}-[0-9]$')


def transform_numeric_cas(numeric_cas):
    if numeric_cas is None:
        return None
    if isinstance(numeric_cas, str):
        if numeric_cas == 'x':
            return None
        if bool(CAS_regexp.match(numeric_cas)):
            return numeric_cas
    ss = str(int(numeric_cas))
    return '-'.join([ss[:-3], ss[-3:-1], ss[-1:]])


class Traci21QuantityImplementation(QuantityImplementation):

    def get_canonical(self, name, **kwargs):
        if hasattr(name, 'entity_type'):
            if name.entity_type == 'quantity':
                return self[name]
        for qi in self._archive.lcia_method_iter():
            if qi['Category'] == name:
                return qi
        return super(Traci21QuantityImplementation, self).get_canonical(name, **kwargs)

    def flowables(self, quantity=None, compartment=None, **kwargs):
        if quantity is not None:
            quantity = self.get_canonical(quantity)
        for i, row in self._archive.iterrows():
            try:
                next(self._archive.cf_for_method_and_compartment(row, method=quantity, compartment=compartment))
            except StopIteration:
                continue
            yield transform_numeric_cas(row['CAS #']), row['Substance Name'].lower()

    def compartments(self, quantity=None, flowable=None, **kwargs):
        if quantity is not None:
            quantity = self.get_canonical(quantity)
        if flowable is not None:
            flowable = next(self._archive.row_for_key(flowable))
        comps = set()
        for col, val in q_info.items():
            if quantity is None or val.Category == quantity['Category']:
                if flowable is None:
                    comps.add(val.Compartment)
                else:
                    try:
                        cf = float(flowable[col])
                    except ValueError:
                        continue
                    if cf != 0:
                        comps.add(val.Compartment)
        for c in comps:
            yield c

    def factors(self, quantity, flowable=None, compartment=None, **kwargs):
        q = self.get_canonical(quantity)
        if q is not None:
            if flowable is not None:
                for row in self._archive.row_for_key(flowable):
                    for cf in self._archive.cf_for_method_and_compartment(row, method=q, compartment=compartment):
                        yield cf
            else:
                for i, row in self._archive.iterrows():
                    for cf in self._archive.cf_for_method_and_compartment(row, method=q, compartment=compartment):
                        yield cf

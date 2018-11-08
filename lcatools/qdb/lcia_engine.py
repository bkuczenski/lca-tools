# from antelope_reports import FlowablesGrid
from .qdb import Qdb


class LciaEngine(object):
    """
    A class for interfacing with a Qdb to perform LCIA calculations
    """
    def __init__(self, **kwargs):
        """

        :param kwargs: passed to Qdb
        """
        self._qdb = Qdb(**kwargs)
        self._lcia_methods = set()

    @property
    def qdb(self):
        return self._qdb

    def quantities(self, **kwargs):
        return self._qdb.search(entity_type='quantity', **kwargs)

    '''
    def flows_table(self, *args, **kwargs):
        """
        Creates a new flowables grid using the local Qdb and gives it to the user.
        :param args:
        :param kwargs:
        :return:
        """
        return FlowablesGrid(self._qdb, *args, **kwargs)
    '''
    """
    Qdb interaction
    """
    def is_elementary(self, flow):
        return self._qdb.c_mgr.is_elementary(flow)

    def load_lcia_factors(self, ref):
        if ref.link not in self._lcia_methods:
            for fb in ref.flowables():
                self._qdb.add_new_flowable(*filter(None, fb))
            for cf in ref.factors():
                self._qdb.add_cf(cf)
            self._lcia_methods.add(ref.link)

    def annotate(self, flow, quantity=None, factor=None, value=None, locale=None):
        """
        Adds a flow annotation to the Qdb.
        Two steps:
         - adds a characterization to the flow using the given data. Provide either a
         factor=Characterization, or qty + value + location.
         If locale is provided with factor, it only applies the factor applying to the given locale. (otherwise, all
         locations in the CF are applied to the flow)
         - adds the flow to the local qdb and saves to disk.
        """
        if factor is None:
            if locale is None:
                locale = 'GLO'
            if value is None:
                value = self._qdb.convert(flow, query=quantity, locale=locale)
            flow.OLD_add_characterization(quantity, value=value, origin=self._qdb.ref, location=locale)
        else:
            ref_conversion = self._qdb.convert_reference(flow, factor.flow.reference_entity, locale=locale)
            if locale is None:
                for l in factor.locations():
                    flow.OLD_add_characterization(factor.quantity, location=l, value=factor[l] * ref_conversion,
                                              origin=factor.origin(l))
            else:
                flow.OLD_add_characterization(factor.quantity, location=locale, value=factor[locale] * ref_conversion,
                                          origin=factor.origin(locale))

        self._qdb.add_entity_and_children(flow)
        for cf in flow.characterizations():
            self._qdb.add_cf(cf)
        self._qdb.save()

    def quantify(self, flowable, quantity, compartment=None):
        return [c for c in self._qdb.quantify(flowable, quantity, compartment=compartment)]




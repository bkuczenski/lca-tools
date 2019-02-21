from collections import defaultdict
import re
import os

from .quantity_manager import QuantityManager
from lcatools.archives.term_manager import TermManager, _flowable_terms  # , Context
from lcatools.interfaces import FlowWithoutContext
from .quelled_cf import QuelledCF
# from synonym_dict.example_flowables import Flowable


'''
Switchable biogenic CO2:

* Biogenic CO2 is CO2, so the flowable used to store CFs is 124-38-9 always
* Because flowable is looked up within the quantity implementation, we can assign a synonym to the flow itself, and 
  watch for it
* Then the switch determines whether or not to quell the CF returned to the user, without changing the database
'''
biogenic = re.compile('(biotic|biogenic|non-fossil)', flags=re.IGNORECASE)


DEFAULT_CONTEXTS = os.path.abspath(os.path.join(os.path.dirname(__file__), 'contexts.json'))


class LciaEngine(TermManager):
    """
    This just adds a Quantity layer to the TermManager and adds instruments to handle flowables by origin.
    I don't think it has to do anything else
    """

    def __init__(self, quantities=None, quell_biogenic_co2=False, contexts=None, **kwargs):
        if contexts is None:
            contexts = DEFAULT_CONTEXTS
        super(LciaEngine, self).__init__(contexts=contexts, **kwargs)

        self._qm = QuantityManager(source_file=quantities)

        # another reverse mapping
        self._fb_by_origin = defaultdict(set)

        # difficult problem, this
        self._quell_biogenic = quell_biogenic_co2

        # we store the child object and use it to signify biogenic CO2 to optionally quell
        self._fm.new_object('carbon dioxide', '124-38-9')
        self._bio_co2 = self._fm.new_object('carbon dioxide (biogenic)', '124-38-9', create_child=True)

    @property
    def quantities(self):
        for k in self._qm.objects:
            yield k

    def _canonical_q(self, quantity):
        x = self._qm[str(quantity)]
        if x is None:
            self._qm.add_quantity(quantity)
            return quantity
        return x

    def _check_flowable(self, flow, fb):
        """
        Subclass handles two problems: tracking flowables by origin and biogenic CO2.

        biogenic: if ANY of the flow's terms match the biogenic
        :param flow:
        :param fb
        :return:
        """
        if '124-38-9' in fb:
            if any([self.is_biogenic(term) for term in _flowable_terms(flow)]):
                fb = self._bio_co2
                flow.flowable = fb  # force reset flowable
        else:
            try:
                f = flow.flowable
                if f not in self._fm:
                    self._fm.add_synonym(f, fb)
                self._fb_by_origin[flow.origin].add(f)
            except FlowWithoutContext:
                flow.flowable = fb
        self._fb_by_origin[flow.origin].add(str(fb))

    def flowables(self, search=None, origin=None):
        if origin is None:
            for k in super(LciaEngine, self).flowables(search=search):
                yield k
        else:
             for k in self._fb_by_origin[origin]:
                 if search is None:
                     yield k
                 else:
                     if bool(re.search(search, k, flags=re.IGNORECASE)):
                         yield k

    @staticmethod
    def is_biogenic(term):
        return bool(biogenic.search(term))

    def _quell_co2(self, flowable, context):
        """
        We know this flow is biogenic CO2, so we ask if it is a resource from air, or if it is any emission
        :param flowable: orig, not looked-up in _fm
        :param context: from CF
        :return: bool
        """
        if flowable is self._bio_co2:
            if context.is_subcompartment(self._cm['from air']):
                return True
            if context.is_subcompartment(self._cm['Emissions']):
                return True
        return False

    def factors_for_flowable(self, flowable, quantity=None, context=None, dist=0):
        for k in super(LciaEngine, self).factors_for_flowable(flowable, quantity=quantity, context=context, dist=dist):
            if self._quell_co2(flowable, k.context):
                yield QuelledCF.from_cf(k, flowable=self._bio_co2)
            else:
                yield k

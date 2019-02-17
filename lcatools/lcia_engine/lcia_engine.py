from collections import defaultdict
import re

from .quantity_manager import QuantityManager
from lcatools.archives.term_manager import TermManager, _flowable_terms  # , Context
# from synonym_dict.example_flowables import Flowable


biogenic = re.compile('(biotic|biogenic|non-fossil)', flags=re.IGNORECASE)
'''
Thinking on biogenic CO2:

1- it cannot be a distinct flowable at all times, and also be a selectable option
 1a- 124-38-9 is 124-38-9. CO2 is CO2.
2- it has to be determined by add_flow; thus flow.flowable needs to distinguish
 2a- so we use a flowable that is undisclosed child to CO2, and check for it
3- quantity implementation needs to respect this and use flow.flowable consistently to ID exchanges 
4- If the flowable matches 
    A- if quell is true: 0
    B- if quell is false: if no CF is found
    CF sh 

'''


class LciaEngine(TermManager):
    """
    This just adds a Quantity layer to the TermManager and adds instruments to handle flowables by origin.
    I don't think it has to do anything else
    """

    def __init__(self, quantities=None, quell_biogenic_co2=False, **kwargs):
        super(LciaEngine, self).__init__(**kwargs)

        self._qm = QuantityManager(source_file=quantities)

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

    def add_flow(self, flow, merge_strategy=None):
        """
        Subclass handles two problems: tracking flowables by origin and biogenic CO2.

        biogenic: if ANY of the flow's terms match the biogenic
        :param flow:
        :param merge_strategy:
        :return:
        """
        fb = super(LciaEngine, self).add_flow(flow, merge_strategy=merge_strategy)
        self._fb_by_origin[flow.origin].add(str(fb))
        if '124-38-9' in fb:
            if any([self.is_biogenic(term) for term in _flowable_terms(flow)]):
                return self._bio_co2
        return fb

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

    '''
    # need to write _quell (which checks compartments and quell setting)
    # need to write create QuelledCF
    def factors_for_flowable(self, flowable, quantity=None, context=None, dist=0):
        for k in super(LciaEngine, self).factors_for_flowable(flowable, quantity=quantity, context=context, dist=dist):
            if flowable is self._bio_co2:
                if self._quell(k):
                    yield QuelledCF(k)
                else:
                    yield k
            else:
                yield k
    '''


from collections import defaultdict
import re
import os

from lcatools.archives.term_manager import TermManager, NoFQEntry  # , Context
from .quelled_cf import QuelledCF
from .clookup import CLookup, SCLookup

from synonym_dict.example_flowables import FlowablesDict


'''
Switchable biogenic CO2:

* Biogenic CO2 is CO2, so the flowable used to store CFs is 124-38-9 always
* Because flowable is looked up within the quantity implementation, we can assign a synonym to the flow itself, and 
  watch for it
* Then the switch determines whether or not to quell the CF returned to the user, without changing the database
'''
biogenic = re.compile('(biotic|biogenic|non-fossil|in air)', flags=re.IGNORECASE)


DEFAULT_CONTEXTS = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'contexts.json'))
DEFAULT_FLOWABLES = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', 'flowables.json'))


class LciaEngine(TermManager):
    """
    This just adds a Quantity layer to the TermManager and adds instruments to handle flowables by origin.
    I don't think it has to do anything else
    """
    def _configure_flowables(self, flowables):
        """
        Setup local flowables database with flows that require special handling. Also loads the flowables file.

        When overriding this function, place the super() call between pre-load and post-load activities.
        :return:
        """
        self._fm.new_entry('carbon dioxide', '124-38-9')
        # we store the child object and use it to signify biogenic CO2 to optionally quell
        # this strategy depends on the ability to set a query flow's name-- i.e. FlowInterface
        self._bio_co2 = self._fm.new_entry('carbon dioxide (biotic)', '124-38-9', create_child=True)

        # now load + merge from file
        self._fm.load(flowables)

        # now add all known "biotic" synonyms for CO2 to the biotic child
        for k in self._fm.synonyms('124-38-9'):
            if bool(biogenic.search(k)):
                self._bio_co2.add_term(k)

    def __init__(self, contexts=None, flowables=None, quantities=None,
                 quell_biogenic_co2=False,
                 strict_clookup=True,
                 **kwargs):
        """

        :param quantities:
        :param quell_biogenic_co2:
        :param contexts:
        :param flowables:
        :param strict_clookup: [True] whether to prohibit multiple CFs for each quantity / flowable / context tuple
        :param kwargs: from TermManager: quiet, merge_strategy
        """
        if contexts is None:
            contexts = DEFAULT_CONTEXTS
        if flowables is None:
            flowables = DEFAULT_FLOWABLES
        super(LciaEngine, self).__init__(contexts=contexts, flowables=None, quantities=quantities, **kwargs)

        # override flowables manager with FlowablesDict-- mainly to upsample CAS numbers for matching
        self._fm = FlowablesDict()
        self._configure_flowables(flowables)

        # the CF lookup: allow hierarchical traversal over compartments [or, um, use a graph db..]
        self._cl_typ = {True: SCLookup,
                        False: CLookup}[strict_clookup]  #
        # another reverse mapping
        self._origins = set()
        self._fb_by_origin = defaultdict(set)  # maps origin to flowables having that origin

        # difficult problem, this
        self._quell_biogenic = quell_biogenic_co2

    def _add_flow_terms(self, flow, merge_strategy=None):
        """
        Subclass handles two problems: tracking flowables by origin and biogenic CO2.

        Should probably test this shit

        Under our scheme, it is a requirement that the flowables list used to initialize the LciaEngine is curated.

        biogenic: if ANY of the flow's terms match the biogenic
        :param flow:
        :return:
        """
        fb = super(LciaEngine, self)._add_flow_terms(flow, merge_strategy=merge_strategy)
        self._fb_by_origin[flow.origin].add(str(fb))
        if '124-38-9' in fb:
            try:
                bio = next(t for t in flow.synonyms if bool(biogenic.search(t)))
            except StopIteration:
                # no biogenic terms
                return fb
            self._bio_co2.add_term(bio)  # ensure that bio term is a biogenic synonym
            flow.name = bio  # ensure that flow's name shows up with that term
            self._fb_by_origin[flow.origin].add(bio)
        return fb

    def import_cfs(self, quantity):
        """
        Given a quantity, import its CFs into the local database.  Unfortunately this is still going to be slow because
        every part of the CF still needs to be canonicalized. The only thing that's saved is creating a new
        Characterization instance.
        :param quantity:
        :return:
        """
        try:
            qq = self._canonical_q(quantity)
        except KeyError:
            qq = self.add_quantity(quantity)

        for cf in quantity.factors():
            try:
                fb = self._fm[cf.flowable]
            except KeyError:
                fb = self._create_flowable(cf.flowable)

            cx = self.add_context(cf.context)
            self._qassign(qq, fb, cf, context=cx)

    def _find_exact_cf(self, qq, fb, cx, origin):
        try:
            ql = self._qlookup(qq, fb)
        except NoFQEntry:
            return None
        # cfs = ql._context_origin(cx, origin=origin)
        cfs = ql.find(cx, dist=0, origin=origin)  # sliiiiightly slower but much better readability
        if len(cfs) > 0:
            if len(cfs) > 1:  # can only happen if qq's origin is None ??
                try:
                    return next(cf for cf in cfs if cf.origin is None)
                except StopIteration:
                    # this should never happen
                    return next(cf for cf in cfs if cf.origin == qq.origin)
            return list(cfs)[0]
        return None

    @staticmethod
    def _store_cf(cl, context, new_cf):
        """
        Assigns the cf to the mapping; does subclass-specific collision checking
        :param cl:
        :param new_cf:
        :return:
        """
        try:
            cl.add(new_cf, key=context)
        except TypeError:
            print(type(cl))
            raise

    def _qassign(self, qq, fb, new_cf, context=None):
        super(LciaEngine, self)._qassign(qq, fb, new_cf, context)
        self._origins.add(new_cf.origin)

    def merge_flowables(self, dominant, *syns):
        for syn in syns:
            self._fm.merge(dominant, syn)

    def save_flowables(self, filename=None):
        self._fm.save(filename)

    def save_contetxts(self, filename=None):
        self._cm.save(filename)

    @property
    def quantities(self):
        for k in self._qm.objects:
            yield k

    def flowables(self, search=None, origin=None):
        """
        Adds ability to filter by origin
        :param search:
        :param origin:
        :return:
        """
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
        We assume that all biogenic CO2 flows will be detected via add_flow, and will have their names set to something
        known to our _bio_co2 Flowable. So: If we are quelling, and if the flowable (string) is synonym to _bio_co2,
        we ask if it is a resource from air, or if it is any emission.
        :param flowable: orig, not looked-up in _fm
        :param context: from CF
        :return: bool
        """
        if self._quell_biogenic is False:
            return False
        if flowable in self._bio_co2:
            if context.is_subcompartment(self._cm['from air']):
                return True
            if context.is_subcompartment(self._cm['Emissions']):
                return True
        return False

    def _factors_for_flowable(self, fb, qq, cx, **kwargs):
        """
        detach lookup for cleanness. canonical everything
        :param fb:
        :param qq:
        :param cx:
        :param kwargs: used in subclasses
        :return:
        """
        try:
            cl = self._qlookup(qq, fb)
        except NoFQEntry:
            return
        if cx is None:
            for v in cl.cfs():
                yield v
        else:
            for v in cl.find(cx, **kwargs):
                yield v

    def factors_for_flowable(self, flowable, quantity=None, context=None, **kwargs):
        for k in super(LciaEngine, self).factors_for_flowable(flowable, quantity=quantity, context=context, **kwargs):
            if self._quell_co2(flowable, k.context):
                yield QuelledCF.from_cf(k, flowable=self._bio_co2)
            else:
                yield k

    def add_from_json(self, j, q_map, origin=None):
        if 'SynonymSets' in j and 'Flowables' not in j:
            j['Flowables'] = j.pop('SynonymSets')
        super(LciaEngine, self).add_from_json(j, q_map, origin=origin)

    def _serialize_qdict(self, origin, quantity, values=False):
        _ql = self._qaccess(quantity)
        return {str(fb): cl.serialize_for_origin(origin, values=values) for fb, cl in _ql.items()}

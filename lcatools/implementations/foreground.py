from .basic import BasicImplementation
from lcatools.interfaces import ForegroundInterface, BackgroundRequired
from lcatools.exchanges import comp_dir
from lcatools.fragment_flows import frag_flow_lcia


from lcatools.entities.editor import FragmentEditor


class FragRecursionError(Exception):
    """
    used to cutoff recursive traversals
    """
    pass


ed = FragmentEditor(interactive=False)


class ForegroundImplementation(BasicImplementation, ForegroundInterface):
    """
    A foreground manager allows a user to build foregrounds.  This should work with catalog references, rather
    than actual entities.

    This interface should cover all operations for web tool

    To build a fragment, we need:
     * child flows lambda
     * uuid

     * parent or None

     * Flow and Direction (relative to parent)
     * cached exchange value (or 1.0)

     * external_ref is uuid always? unless set-- ForegroundArchive gets a mapping from external_ref to uuid

     * other properties: Name, StageName

    To terminate a fragment we need a catalog ref: origin + ref for:
     * to foreground -> terminate to self
     * to subfragment -> just give uuid
     * background terminations are terminations to background fragments, as at present
     * process from archive, to become sub-fragment
     * flow, to become fg emission

    Fragment characteristics:
     * background flag
     * balance flag

    Scenario variants:
     - exchange value
     - termination
    """
    _count = 0

    def __init__(self,  *args, **kwargs):
        """

        :param fg_archive: must be a ForegroundArchive, which will be the object of management
        :param qdb: quantity database, used for compartments and id'ing flow properties--
        """
        super(ForegroundImplementation, self).__init__(*args, **kwargs)

        self._recursion_check = None  # prevent recursive loops on frag-from-node

    def fragments(self, *args, **kwargs):
        """

        :param args: optional regex filter(s)
        :param kwargs: background [None], show_all [False]
        :return:
        """
        return self._archive.fragments(*args, **kwargs)

    def frag(self, string, strict=True):
        return self._archive.frag(string, strict=strict)

    '''
    Create and modify fragments
    '''

    def new_fragment(self, *args, **kwargs):
        """

        :param args: flow, direction
        :param kwargs: uuid=None, parent=None, comment=None, value=None, balance=False; **kwargs passed to LcFragment
        :return:
        """
        frag = ed.create_fragment(*args, **kwargs)
        self._archive.add_entity_and_children(frag)
        return frag

    def find_or_create_term(self, exchange, background=None):
        """
        Finds a fragment that terminates the given exchange
        :param exchange:
        :param background: [None] - any frag; [True] - background frag; [False] - foreground frag
        :return:
        """
        try:
            bg = next(f for f in self._archive.fragments(background=background) if f.term.terminates(exchange))
            print('%% Found existing termination bg=%s for %s' % (background, exchange.termination))
        except StopIteration:
            if background is None:
                background = False
            print('@@ Creating new termination bg=%s for %s' % (background, exchange.termination))
            bg = ed.create_fragment(exchange.flow, comp_dir(exchange.direction), background=background)
            bg.terminate(self._archive.catalog_ref(exchange.process.origin, exchange.termination,
                                                   entity_type='process'), term_flow=exchange.flow)
            self._archive.add_entity_and_children(bg)
        return bg

    def create_fragment_from_node_(self, process_ref, ref_flow=None, observe=True, **kwargs):
        """
        Given a process reference without context and a reference, create a fragment using the complementary exchanges
         to the reference.  Terminate background flows to background (only if a background interface is available).
         If fragments are found to terminate foreground flows, terminate to them; otherwise leave cut-offs.

        This method does not deal well with multioutput processes that do not have a specified partitioning allocation-
         though it's possible that the place to deal with that is the inventory query.

        :param process_ref:
        :param ref_flow:
        :param observe:
        :param kwargs:
        :return:
        """
        rx = process_ref.reference(ref_flow)
        top_frag = ed.create_fragment(rx.flow, rx.direction,
                                      comment='Reference fragment; %s ' % process_ref.link)
        term = top_frag.terminate(process_ref)
        self._child_fragments(top_frag, term, **kwargs)
        self._archive.add_entity_and_children(top_frag)
        if observe:
            top_frag.observe(accept_all=True)
        return top_frag

    def terminate_fragment(self, fragment, term_node, term_flow=None, scenario=None, **kwargs):
        """
        Given a fragment, terminate it to the given node, either process_ref or subfragment.
        :param fragment:
        :param term_node:
        :param term_flow: required if the node does not natively terminate the fragment
        :param scenario:
        :param kwargs:
        :return:
        """
        if len([t for t in fragment.child_flows]) > 0:
            print('warning: fragment has child flows and duplication is not detected')
            # how should we deal with this?
        term = fragment.terminate(term_node, scenario=scenario, term_flow=term_flow)
        self._child_fragments(fragment, term, **kwargs)
        if term_node.entity_type == 'process':
            fragment.observe(scenario=scenario, accept_all=True)

    def _child_fragments(self, parent, term, include_elementary=False):
        process = term.term_node
        child_exchs = [x for x in process.inventory(ref_flow=term.term_flow)]
        comment = 'Created from inventory query to %s' % process.link

        if include_elementary:
            for x in process.elementary(child_exchs):
                elem = ed.create_fragment(x.flow, x.direction, parent=parent, value=x.value,
                                          comment='FG Emission; %s' % comment,
                                          StageName='Foreground Emissions')
                elem.to_foreground()  # this needs renamed

        for x in process.intermediate(child_exchs):
            child_frag = ed.create_fragment(x.flow, x.direction, parent=parent, value=x.value, comment=comment)

            if x.termination is not None:
                try:
                    iib = process.is_in_background(termination=x.termination, ref_flow=x.flow)
                except BackgroundRequired:
                    iib = False
                if iib:
                    bg = self.find_or_create_term(x, background=True)
                    child_frag.terminate(bg)
                else:
                    # definitely some code duplication here with find_or_create_term--- but can't exactly use because
                    # we don't want to create any subfragments here
                    try:
                        subfrag = next(f for f in self._archive.fragments(background=False) if
                                       f.term.terminates(x))
                        child_frag.terminate(subfrag)

                    except StopIteration:
                        child_frag['termination'] = x.termination

    def create_forest(self, process_ref, ref_flow=None, observe=True):
        """
        create a collection of maximal fragments that span the foreground for a given node.  Any node that is
        multiply invoked will be a subfragment; any node that has a unique parent will be a child fragment.

        All the resulting fragments will be added to the foreground archive.

        Given that the fragment traversal routine can be made to effectively handle single-large-loop topologies (i.e.
        where the fragment depends on its own reference but is otherwise acyclic), this routine may have to do some
        slightly complicated adjacency modeling.

        emissions are excluded, mainly because of the existence of pre-aggregated foreground nodes with very
        large emission lists. But this will need to be sorted because the current fragment traversal requires an
        inventory interface-- though I suppose I could simply fallback to a background.emissions() call on
        InventoryRequired. [done]

        :param process_ref:
        :param ref_flow:
        :param observe: [True]
        :return:
        """
        '''
        Here's how this works:
         * given a node, we ask the background interface for the node's foreground
           =  the bg interface gives us a list of nonzero values in Af, formulated as exchanges
         * starting with the reference node, we create a fragment for each Af exchange
           = we assign it
         * we keep a mapping of termination strings to terminations, beginning with a process ref
           = if we encounter a termination that has already been mapped, upgrade it to a subfragment
           = if a fragment's termination already exists as a subfragment, terminate to the subfragment
           = otherwise, log the termination and add it as a child flow
        '''
        fx = process_ref.foreground(ref_flow=ref_flow)

        # index the entire local collection of fragments to find existing terminations and existing subfragments
        term_map = dict(((k.term.term_node.external_ref, k) for k in self._archive.fragments(show_all=True)
                        if k.term.is_process))
        subfrags = dict(((k.term.term_node.external_ref, k) for k in self._archive.fragments()))

        top_frag = self._new_node(fx[0].flow, fx[0].direction, process_ref)
        top_frag['Comment'] = 'Reference node; foreground query to %s' % process_ref.link
        term_map[process_ref.external_ref] = top_frag
        subfrags[process_ref.external_ref] = top_frag

        for x in fx[1:]:
            parent = term_map[x.process.external_ref]
            assert x.termination is not None
            if x.termination in subfrags:
                # termination is already a subfragment
                child = ed.create_fragment(x.flow, x.direction, parent=parent, value=x.value, comment='Subfragment')
                # need to test for recursion! this could even happen inside terminate()
                child.terminate(subfrags[x.termination])
            else:
                # termination not [yet] encountered twice
                if x.termination in term_map:
                    # termination encountered for the second time -- need to make a subfragment
                    print('### Splitting subfragment at %s' % x.termination)
                    subfrag = ed.split_subfragment(term_map[x.termination])
                    subfrags[x.termination] = subfrag
                    child = ed.create_fragment(x.flow, x.direction, parent=parent, value=x.value, comment='Subfragment')
                    # need to test for recursion! this could even happen inside terminate()
                    child.terminate(subfrags[x.termination])
                else:
                    # termination encountered for the first time -- need to make a new node
                    term = self._archive.catalog_ref(x.process.origin, x.termination, entity_type='process')
                    child = self._new_node(x.flow, x.direction, term, value=x.value, parent=parent)
                    term_map[x.termination] = child

        for f in subfrags.values():
            self._archive.add_entity_and_children(f)
            if observe:
                f.observe(accept_all=True)
        return top_frag

    def _new_node(self, flow, direction, term, value=1.0, parent=None):
        print('# Creating node (%2d) with term %s' % (self._count, term))
        self._count += 1
        frag = ed.create_fragment(flow, direction, value=value, parent=parent)
        frag.terminate(term, term_flow=flow)

        for dep in term.dependencies(ref_flow=flow):
            child = ed.create_fragment(dep.flow, dep.direction, parent=frag, value=dep.value,
                                       comment='Dependency; %s' % term.link)
            child.terminate(self.find_or_create_term(dep, background=True))
        return frag

    def create_fragment_from_node(self, process_ref, ref_flow=None, include_elementary=False, observe=True):
        """

        :param process_ref:
        :param ref_flow:
        :param include_elementary:
        :param observe: whether to "observe" the fragments with the process's exchange values (default is yes
         via the API-- unobserved exchanges will not show up in an API traversal)
        :return:
        """
        if self._recursion_check is not None:
            raise FragRecursionError('We appear to be inside a recursion already')
        try:
            self._recursion_check = set()
            frag = self._create_fragment_from_node(process_ref, ref_flow=ref_flow,
                                                   include_elementary=include_elementary, observe=observe)
        finally:
            self._recursion_check = None
        return frag

    def _create_fragment_from_node(self, process, ref_flow=None, include_elementary=False, observe=True):
        if process.uuid in self._recursion_check:
            raise FragRecursionError('Encountered the same process!')
        self._recursion_check.add(process.uuid)

        fg_exchs = [x for x in process.inventory(ref_flow=ref_flow)]
        rx = process.reference(ref_flow)
        comment = 'Created from inventory query to %s' % process.origin
        top_frag = ed.create_fragment(rx.flow, rx.direction,
                                      comment='Reference fragment; %s ' % comment)
        top_frag.terminate(process)

        if include_elementary:
            for x in process.elementary(fg_exchs):
                elem = ed.create_fragment(x.flow, x.direction, parent=top_frag, value=x.value,
                                          comment='FG Emission; %s' % comment,
                                          StageName='Foreground Emissions')
                elem.to_foreground()

        for x in process.intermediate(fg_exchs):
            if x.termination is None:
                ed.create_fragment(x.flow, x.direction, parent=top_frag, value=x.value,
                                   comment='Cut-off; %s' % comment)

            else:
                child_frag = ed.create_fragment(x.flow, x.direction, parent=top_frag, value=x.value,
                                                comment='Subfragment; %s' % comment)
                if process.is_in_background(termination=x.termination, ref_flow=x.flow):
                    bg = self.find_or_create_term(x, background=True)
                    child_frag.terminate(bg)
                else:
                    # definitely some code duplication here with find_or_create_term--- but can't exactly use because
                    # in the event of 'create', we want the subfragment to also be recursively traversed.
                    try:
                        subfrag = next(f for f in self._archive.fragments(background=False) if
                                       f.term.terminates(x))
                    except StopIteration:

                        try:
                            term_ref = self._archive.catalog_ref(process.origin, x.termination, entity_type='process')
                            subfrag = self._create_fragment_from_node(term_ref, ref_flow=x.flow,
                                                                      include_elementary=include_elementary,
                                                                      observe=observe)
                        except FragRecursionError:
                            subfrag = self.find_or_create_term(x, background=True)

                    child_frag.terminate(subfrag)
        self._archive.add_entity_and_children(top_frag)
        if observe:
            top_frag.observe(accept_all=True)
        return top_frag

    def clone_fragment(self, frag, **kwargs):
        """

        :param frag: the fragment (and subfragments) to clone
        :param kwargs: suffix (default: ' (copy)', applied to Name of top-level fragment only)
                       comment (override existing Comment if present; applied to all)
        :return:
        """
        clone = ed.clone_fragment(frag, **kwargs)
        self._archive.add_entity_and_children(clone)
        return clone

    def traverse(self, fragment, scenario=None, **kwargs):
        frag = self._archive.retrieve_or_fetch_entity(fragment)
        return frag.top().traverse(scenario, observed=True)

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        quantity_ref.ensure_lcia()
        fragmentflows = self.traverse(fragment, scenario=scenario, **kwargs)
        return frag_flow_lcia(fragmentflows, quantity_ref, scenario=scenario, refresh=refresh)

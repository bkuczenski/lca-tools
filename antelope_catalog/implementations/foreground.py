from lcatools.implementations import BasicImplementation
from lcatools.interfaces import ForegroundInterface, comp_dir, BackgroundRequired, CONTEXT_STATUS_

from lcatools.entities.editor import FlowEditor
from ..foreground.fragment_editor import FragmentEditor


class FragRecursionError(Exception):
    """
    used to cutoff recursive traversals
    """
    pass


class OutOfOrderException(Exception):
    """
    exchange's parent node not yet created
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
        self._flow_ed = FlowEditor(self._archive.qdb)

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
    def new_flow(self, name, ref_quantity, context=None, **kwargs):
        """

        :param name:
        :param ref_quantity:
        :param context: [None] pending context refactor
        :param kwargs:
        :return:
        """
        if CONTEXT_STATUS_ == 'compat':
            if context is not None and 'compartment' not in kwargs:
                kwargs['compartment'] = str(context)
        ref_q = self._archive.qdb.get_canonical(ref_quantity)
        f = self._flow_ed.new_flow(name=name, quantity=ref_q, **kwargs)
        self._archive.add_entity_and_children(f)
        return f

    def new_fragment(self, *args, **kwargs):
        """

        :param args: flow, direction (w.r.t. parent)
        :param kwargs: uuid=None, parent=None, comment=None, value=None, balance=False; **kwargs passed to LcFragment
        :return:
        """
        frag = ed.create_fragment(*args, **kwargs)
        self._archive.add_entity_and_children(frag)
        return frag

    def name_fragment(self, fragment, name, **kwargs):
        self._archive.name_fragment(fragment, name)

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
        fx = process_ref.foreground(ref_flow=ref_flow)
        px = []
        '''
        New plan
        Constraints:
         - every distinct termination matches exactly one foreground node (enforced with term_map)
         - every exchange value matches exactly one child fragment (enforced with has_child())
         - child flows may only be added to fragments created in this function call (enforced with _new_frags)
        workflow:
         - index existing terminations
         - check/create reference node
         - for each exchange:
           * lookup parent-- should be fragment (reference or child)
           * lookup termination
             = if missing, create new child + store
             = if child, split new subfragment + replace
             = if subfragment, use
           * if parent has matching child already, continue
           * else create new child fragment; terminate as above
         - observe

        '''
        # index the entire local collection of fragments to find existing terminations and existing subfragments
        term_map = dict(((k.term.term_node.external_ref, k) for k in self._archive.fragments(show_all=True)
                        if k.term.is_process))
        _new_frags = set()

        def _grab_parent(_exch):
            if _exch.process.external_ref in term_map:
                return term_map[_exch.process.external_ref]
            else:
                print('Out of order %s' % _exch)
                raise OutOfOrderException

        def _create_child(_par, _exch):
            if _exch.termination in term_map:
                _n = term_map[_exch.termination]
                if _n.reference_entity is None:
                    _c = ed.create_fragment(_exch.flow, _exch.direction, parent=_par, termination=_n,
                                            value=_exch.value,
                                            comment='Subfragment; %s' % _exch.termination)
                else:
                    # upgrade to a subfragment
                    print('### Splitting subfragment %s' % _n.term.term_node)
                    _s = ed.split_subfragment(_n)
                    term_map[_exch.termination] = _s
                    _c = ed.create_fragment(_exch.flow, _exch.direction, parent=_par, termination=_s,
                                            value=_exch.value,
                                            comment='Subfragment; %s' % _exch.termination)
            else:
                _c = self._new_node(_exch, parent=_par)
                term_map[_exch.termination] = _c
                _new_frags.add(_c)
            return _c

        def _try_exch(_p, _x):
            if _p not in _new_frags:
                print('Parent fragment is out of scope')
                return
            if _p.has_child(_x.flow, _x.direction, _x.termination):
                print('Parent-child relationship already exists')
                return

            _create_child(_p, x)

        if process_ref.external_ref in term_map:
            print('This fragment has already been created.')
            top_frag = term_map[process_ref.external_ref]
            if top_frag.reference_entity is None:
                return top_frag
            else:
                return ed.split_subfragment(top_frag)

        top_frag = self._new_node(fx[0])
        term_map[fx[0].process.external_ref] = top_frag
        _new_frags.add(top_frag)

        for x in fx[1:]:
            try:
                parent = _grab_parent(x)
            except OutOfOrderException:
                px.append(x)
                continue

            _try_exch(parent, x)

        _rec_count = 0
        while len(px) > 0:
            if _rec_count > 9:
                print('Abandoned out-of-order fragments!')
                print([str(x) for x in px])
                break

            fx = px
            px = []
            _rec_count += 1
            for x in fx:
                try:
                    parent = _grab_parent(x)
                except OutOfOrderException:
                    px.append(x)
                    continue

                _try_exch(parent, x)

        for f in _new_frags:
            self._archive.add_entity_and_children(f)
            if observe:
                f.observe(accept_all=True)
        return top_frag

    def _new_node(self, exch, parent=None):
        """
        Call only when no fragment exists with the given termination
        :param exch:
        :param parent:
        :return:
        """
        if parent is None:
            term = exch.process
        else:
            term = self._archive.catalog_ref(exch.process.origin, exch.termination, 'process')

        print('# Creating node (%2d) with term %s' % (self._count, term.external_ref))
        self._count += 1
        frag = ed.create_fragment(exch.flow, exch.direction, value=exch.value, parent=parent)  # will flip direction
        frag.terminate(term, term_flow=exch.flow)

        for dep in term.dependencies(ref_flow=exch.flow):
            child = ed.create_fragment(dep.flow, dep.direction, parent=frag, value=dep.value,
                                       comment='Dependency; %s' % term.link)
            child.terminate(self.find_or_create_term(dep, background=True))
        return frag

    def create_fragment_from_node(self, process_ref, ref_flow=None, include_elementary=False, observe=True):
        """
        Unclear where this lies with respect to create_forest-- should we deprecate?

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
        """
        Interior process used recursively to create a fragment.

        The tree cannot include
        :param process:
        :param ref_flow:
        :param include_elementary:
        :param observe:
        :return:
        """
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

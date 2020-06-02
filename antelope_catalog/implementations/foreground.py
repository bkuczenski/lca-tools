from lcatools.implementations import BasicImplementation
from lcatools.interfaces import ForegroundInterface, CONTEXT_STATUS_, EntityNotFound, comp_dir  # , BackgroundRequired

from lcatools.entities.quantities import new_quantity
from lcatools.entities.flows import new_flow
from lcatools.entities.fragments import InvalidParentChild
from lcatools.entities.fragment_editor import create_fragment, clone_fragment, _fork_fragment  # interpose,


class NotForeground(Exception):
    pass


class UnknownRefQuantity(Exception):
    pass


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
    #_count = 0
    #_frags_with_flow = defaultdict(set)  # we actually want this to be shared among
    #_recursion_check = None

    def __init__(self, *args, **kwargs):
        super(ForegroundImplementation, self).__init__(*args, **kwargs)
        self._observations = []

    '''
    Add some useful functions from other interfaces to the foreground
    '''
    def get_local(self, external_ref, **kwargs):
        """
        The special characteristic of a foreground is its access to the catalog-- so-- use it
        lookup locally; fallback to catalog query- should make origin a kwarg
        :param external_ref:
        :param kwargs:
        :return:
        """
        e = self._fetch(external_ref, **kwargs)
        if e is not None:
            return e
        try:
            origin, external_ref = external_ref.split('/', maxsplit=1)
        except ValueError:
            origin = 'foreground'
        return self._archive.catalog_ref(origin, external_ref)

    def count(self, entity_type):
        return self._archive.count_by_type(entity_type)

    def flows(self, **kwargs):
        for f in self._archive.search('flow', **kwargs):
            yield f

    def get_canonical(self, quantity):
        """
        By convention, a foreground archive's Term Manager is the catalog's LCIA engine, which is the Qdb of record
        for the foreground.
        :param quantity:
        :return:
        """
        return self._archive.tm.get_canonical(quantity)

    '''
    fg implementation begins here
    '''
    def fragments(self, show_all=False, **kwargs):
        if hasattr(self._archive, 'fragments'):
            # we only want reference fragments by default
            for f in self._archive.fragments(show_all=show_all, **kwargs):
                yield f
        else:
            raise NotForeground('The resource does not contain fragments: %s' % self._archive.ref)

    def context(self, item):
        return self._archive.tm[item]

    def frag(self, string, **kwargs):
        """
        :param string:
        :param kwargs: many=False
        :return:
        """
        return self._archive.frag(string, **kwargs)

    '''
    Create and modify fragments
    '''
    def new_quantity(self, name, ref_unit=None, **kwargs):
        """

        :param name:
        :param ref_unit:
        :param kwargs:
        :return:
        """
        q = new_quantity(name, ref_unit, **kwargs)
        self._archive.add(q)
        return q

    def add_or_retrieve(self, external_ref, reference, name, group=None, strict=False, **kwargs):
        """
        Gets a flow with the given external_ref, and creates it if it doesn't exist from the given spec.

        Note that the full spec is mandatory so it could be done by tuple.  With strict=False, an entity will be
        returned if it exists.
        :param external_ref:
        :param reference: a string, either a unit or known quantity
        :param name: the object's name
        :param group: used as context for flow
        :param strict: [False] if True it will raise a TypeError (ref) or ValueError (name) if an entity exists but
        does not match the spec.  n.b. I think I should check the reference entity even if strict is False but.. nah
        :return:
        """
        try:
            t = self.get(external_ref)
            if strict:
                if t.entity_type == 'flow':
                    if t.reference_entity != self.get_canonical(reference):
                        raise TypeError("ref quantity (%s) doesn't match supplied (%s)" % (t.reference_entity, reference))
                elif t.entity_type == 'quantity':
                    if t.unit != reference:
                        raise TypeError("ref unit (%s) doesn't match supplied (%s)" % (t.unit, reference))
                if t['Name'] != name:
                    raise ValueError("Name (%s) doesn't match supplied(%s)" % (t['Name'], name))
            return t

        except EntityNotFound:
            try:
                return self.new_flow(name, ref_quantity=reference, external_ref=external_ref, context=group, **kwargs)
            except UnknownRefQuantity:
                # assume reference is a unit string specification
                return self.new_quantity(name, ref_unit=reference, external_ref=external_ref, group=group, **kwargs)

    def new_flow(self, name, ref_quantity=None, context=None, **kwargs):
        """

        :param name:
        :param ref_quantity: defaults to "Number of items"
        :param context: [None] pending context refactor
        :param kwargs:
        :return:
        """
        if CONTEXT_STATUS_ == 'compat':
            if context is not None and 'compartment' not in kwargs:
                kwargs['compartment'] = str(context)
        if ref_quantity is None:
            ref_quantity = 'Number of items'
        try:
            ref_q = self.get_canonical(ref_quantity)
        except EntityNotFound:
            raise UnknownRefQuantity(ref_quantity)
        f = new_flow(name, ref_q, **kwargs)
        self._archive.add_entity_and_children(f)
        return self.get(f.link)

    def find_term(self, term_ref, origin=None, **kwargs):
        """

        :param term_ref:
        :param origin:
        :param kwargs:
        :return:
        """
        if term_ref is None:
            return
        if hasattr(term_ref, 'entity_type'):
            found_ref = term_ref
        else:
            # first context
            cx = self._archive.tm[term_ref]
            if cx is not None:
                found_ref = cx
            else:
                found_ref = self.get_local('/'.join(filter(None, (origin, term_ref))))
                ''' # this is now internal to get()
                except EntityNotFound:
                    if origin is None:
                        try:
                            origin, external_ref = term_ref.split('/', maxsplit=1)
                        except ValueError:
                            origin = 'foreground'
                            external_ref = term_ref

                        found_ref = self._archive.catalog_ref(origin, external_ref)
                    else:
                        found_ref = self._archive.catalog_ref(origin, term_ref)
                '''

        if found_ref.entity_type in ('flow', 'process', 'fragment', 'context'):
            return found_ref
        raise TypeError('Invalid entity type for termination: %s' % found_ref.entity_type)

    def new_fragment(self, flow, direction, external_ref=None, **kwargs):
        """
        :param flow:
        :param direction:
        :param external_ref: if provided, observe and name the fragment after creation
        :param kwargs: uuid=None, parent=None, comment=None, value=None, units=None, balance=False;
          **kwargs passed to LcFragment
        :return:
        """
        if isinstance(flow, str):
            try:
                f = self.get(flow)
            except EntityNotFound:
                f = self.get_local(flow)
            if f.entity_type != 'flow':
                raise TypeError('%s is not a flow' % flow)
            flow = f
        frag = create_fragment(flow, direction, origin=self.origin, **kwargs)
        self._archive.add_entity_and_children(frag)
        if external_ref is not None:
            self.observe(frag, name=external_ref)
        return frag

    def name_fragment(self, fragment, name, auto=None, force=None, **kwargs):
        return self._archive.name_fragment(fragment, name, auto=auto, force=force)

    def observe(self, fragment, exchange_value=None, name=None, scenario=None, units=None, auto=None, force=None,
                accept_all=None, **kwargs):
        if accept_all is not None:
            print('%s: cannot "accept all"' % fragment)
        if name is not None and scenario is None:  #
            if fragment.external_ref != name:
                print('Naming fragment %s -> %s' % (fragment.external_ref, name))
                self._archive.name_fragment(fragment, name, auto=auto, force=force)
        if fragment.observable(scenario):
            if fragment not in self._observations:
                self._observations.append(fragment)
            if exchange_value is None:
                exchange_value = fragment.cached_ev  # do not expose accept_all via the interface; instead allow implicit
            fragment.observe(scenario=scenario, value=exchange_value, units=units)
        else:
            if exchange_value is not None:
                print('Note: Ignoring exchange value %g for unobservable fragment %s [%s]' % (exchange_value,
                                                                                              fragment.external_ref,
                                                                                              scenario))
        return fragment.link

    @property
    def observed_flows(self):
        for k in self._observations:
            yield k

    def fragments_with_flow(self, flow, direction=None, reference=None, background=None, **kwargs):
        """
        Requires flow identity
        :param flow:
        :param direction:
        :param reference:
        :param background:
        :param kwargs:
        :return:
        """
        flow = self[flow]  # retrieve by external ref
        for f in self._archive.fragments_with_flow(flow):
            if background is not None:
                if f.is_background != background:
                    continue
            if direction is not None:
                if f.direction != direction:
                    continue
            if reference is False and f.parent is None:
                continue
            if reference and f.parent:
                continue
            yield f

    def clone_fragment(self, frag, **kwargs):
        """

        :param frag: the fragment (and subfragments) to clone
        :param kwargs: suffix (default: ' (copy)', applied to Name of top-level fragment only)
                       comment (override existing Comment if present; applied to all)
        :return:
        """
        clone = clone_fragment(frag, **kwargs)
        self._archive.add_entity_and_children(clone)
        return clone

    def split_subfragment(self, fragment, replacement=None, **kwargs):
        """
        Given a non-reference fragment, split it off into a new reference fragment, and create a surrogate child
        that terminates to it.

        without replacement:
        Old:   ...parent-->fragment
        New:   ...parent-->surrogate#fragment;   (fragment)

        with replacement:
        Old:   ...parent-->fragment;  (replacement)
        New:   ...parent-->surrogate#replacement;  (fragment);  (replacement)

        :param fragment:
        :param replacement: [None] if non-None, the surrogate is terminated to the replacement instead of the fork.
        :return:
        """
        if fragment.reference_entity is None:
            raise AttributeError('Fragment is already a reference fragment')
        if replacement is not None:
            if replacement.reference_entity is not None:
                raise InvalidParentChild('Replacement is not a reference fragment')

        surrogate = _fork_fragment(fragment, comment='New subfragment')
        self._archive.add_entity_and_children(surrogate)

        fragment.unset_parent()
        if replacement is None:
            surrogate.terminate(fragment)
        else:
            surrogate.terminate(replacement)

        return fragment

    def delete_fragment(self, fragment, **kwargs):
        """
        Remove the fragment and all its subfragments from the archive (they remain in memory)
        This does absolutely no safety checking.
        :param fragment:
        :return:
        """
        if isinstance(fragment, str):
            try:
                fragment = self.get(fragment)
            except EntityNotFound:
                return False
        self._archive.delete_fragment(fragment)
        for c in fragment.child_flows:
            self.delete_fragment(c)
        return True

    def save(self, **kwargs):
        self._archive.save(**kwargs)
        return True

    def clear_unit_scores(self, lcia_method=None):
        self._archive.clear_unit_scores(lcia_method)

    def clear_scenarios(self, terminations=True):
        for f in self._archive.entities_by_type('fragment'):
            f.clear_scenarios(terminations=terminations)

    def create_process_model(self, process, ref_flow=None, set_background=True, **kwargs):
        rx = process.reference(ref_flow)
        if process.reference_value(ref_flow) < 0:  # put in to handle Ecoinvent treatment processes
            dirn = comp_dir(rx.direction)
        else:
            dirn = rx.direction
        frag = self.new_fragment(rx.flow, dirn, value=1.0, **kwargs)
        frag.terminate(process, term_flow=rx.flow)
        if set_background:
            frag.set_background()
        # self.fragment_from_exchanges(process.inventory(rx), parent=frag,
        #                              include_context=include_context, multi_flow=multi_flow)
        return frag

    def extend_process(self, fragment, scenario=None, **kwargs):
        term = fragment.termination(scenario)
        if not term.is_process:
            raise TypeError('Termination is not to process')
        fragment.unset_background()
        process = term.term_node
        self.fragment_from_exchanges(process.inventory(ref_flow=term.term_flow), parent=fragment, scenario=scenario,
                                     **kwargs)



    '''
    def extend_process_model(self, fragment, include_elementary=False, terminate=True, **kwargs):
        """
        "Build out" a fragment, creating child flows from its terminal node's intermediate flows
        :param fragment:
        :param include_elementary:
        :param terminate:
        :param kwargs:
        :return:
        """
        fragment.unset_background()
        process = fragment.term.term_node
        rx = fragment.term.term_flow
        for ex in process.inventory(rx):
            if not include_elementary:
                if ex.type in ('context', 'elementary'):
                    continue
            ch = self.new_fragment(ex.flow, ex.direction, value=ex.value, parent=fragment)
            ch.set_background()
            if ex.type in ('cutoff', 'self'):
                continue
            if terminate:
                ch.terminate(self._archive.catalog_ref(process.origin, ex.termination, entity_type='process'),
                             term_flow=ex.flow)
        fragment.observe(accept_all=True)
        return fragment
    '''

    '''# Create or update a fragment from a list of exchanges.

    This needs to be an interface method.
    '''
    def fragment_from_exchanges(self, _xg, parent=None, ref=None, scenario=None,
                                set_background=True,
                                include_context=False,
                                multi_flow=False):
        """
        If parent is None, first generated exchange is reference flow; and subsequent exchanges are children.
        Else, all generated exchanges are children of the given parent, and if a child flow exists, update it.

        We can take two approaches: in the simple view, the child flow is unique, and the termination can be updated
        by a subsequent specification.  This obviously fails if a fragment has multiple children with the same flow--
        as is in fact the case with ecoinvent.  So for that case we just need to match on termination as well; but
        that means a termination cannot get updated automatically through this process.

        This is all tricky if we expect it to work with both ExchangeRefs and actual exchanges (which, obviously, we
        should) because: ExchangeRefs may have only a string for process and flow, but Exchanges will have entities
        for each.  We need to get(flow_ref) and find_term(term_ref) but we can simply use flow entities and catalog
        refs if we have process.origin.  For now we will just swiss-army-knife it.

        :param _xg: Generates a list of exchanges or exchange references
        :param parent: if None, create parent from first exchange
        :param ref: if parent is created, assign it a name (if parent is non-None, ref is ignored
        :param set_background: [True] whether to regard process-terminated fragments as background fragments
        :param include_context: [False] whether to model context-terminated flows as child fragments
        :param multi_flow: [False] if True, child flows are matched on flow, direction, and termination
        :return:
        """
        if parent is None:
            x = next(_xg)
            parent = self.new_fragment(x.flow, x.direction, value=x.value, units=x.unit, Name=str(x.process), **x.args)
            if ref is None:
                print('Creating new fragment %s (%s)' % (x.process.name, parent.uuid))
            else:
                print('Creating new fragment %s' % ref)
                self.observe(parent, name=ref)
            update = False
        else:
            update = True

        for y in _xg:
            if hasattr(y.flow, 'entity_type') and y.flow.entity_type == 'flow':
                flow = y.flow
            else:
                flow = self[y.flow]
                if flow is None:
                    print('Skipping unknown flow %s' % y.flow)
                    continue
            if hasattr(y.process, 'origin'):
                term = self.find_term(y.termination, origin=y.process.origin)
            else:
                term = self.find_term(y.termination)
            if term is not None and term.entity_type == 'context' and include_context is False:
                continue
            if term == y.process:
                term = None  # don't terminate self-term
            if update:
                try:
                    # TODO: children_with_flow needs to be scenario aware
                    if multi_flow:
                        c_up = next(parent.children_with_flow(flow, direction=y.direction, termination=term,
                                                              recurse=False))
                    else:
                        c_up = next(parent.children_with_flow(flow, direction=y.direction, recurse=False))

                    # update value
                    v = y.value
                    if y.unit is not None:
                        v *= c_up.flow.reference_entity.convert(y.unit)

                    if c_up.exchange_value(scenario) != v:
                        print('Updating %s exchange value %.3f' % (c_up, v))

                        self.observe(c_up, exchange_value=v, scenario=scenario)
                    if multi_flow:
                        continue  # cannot update terms in the multi-flow case

                    # set term
                    if term is not None:
                        if term != c_up.term.term_node:
                            print('Updating %s termination %s' % (c_up, term))
                            c_up.clear_termination(scenario)
                            if term.entity_type == 'fragment':
                                c_up.terminate(term, scenario=scenario)
                            if term.entity_type == 'process' and set_background:
                                c_up.set_background()
                    continue
                except StopIteration:
                    print('No child flow found; creating new %s %s' % (flow, y.direction))
                    pass

            c = self.new_fragment(flow, y.direction, value=y.value, units=y.unit, parent=parent, **y.args)

            if term is not None:
                c.terminate(term, scenario=scenario)
                if term.entity_type == 'process' and set_background:
                    c.set_background()
            self.observe(c)  # use cached implicitly via fg interface

        return parent

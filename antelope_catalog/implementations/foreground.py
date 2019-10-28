from collections import defaultdict

from lcatools.implementations import BasicImplementation
from lcatools.interfaces import ForegroundInterface, CONTEXT_STATUS_, EntityNotFound  # , comp_dir, BackgroundRequired

from lcatools.entities.quantities import new_quantity
from lcatools.entities.flows import new_flow
from lcatools.entities.fragments import InvalidParentChild
from lcatools.entities.fragment_editor import create_fragment, clone_fragment, interpose, _fork_fragment


class NotForeground(Exception):
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
    _count = 0
    _frags_with_flow = defaultdict(set)
    _recursion_check = None

    def _get_canonical(self, quantity):
        """
        By convention, a foreground archive's Term Manager is the catalog's LCIA engine, which is the Qdb of record
        for the foreground.
        :param quantity:
        :return:
        """
        return self._archive.tm.get_canonical(quantity)

    def fragments(self, show_all=False, **kwargs):
        if hasattr(self._archive, 'fragments'):
            # we only want reference fragments by default
            for f in self._archive.fragments(show_all=show_all, **kwargs):
                yield f
        else:
            raise NotForeground('The resource does not contain fragments: %s' % self._archive.ref)

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
        ref_q = self._get_canonical(ref_quantity)
        f = new_flow(name, ref_q, **kwargs)
        self._archive.add_entity_and_children(f)
        return f

    def new_fragment(self, flow, direction, **kwargs):
        """
        :param flow:
        :param direction:
        :param kwargs: uuid=None, parent=None, comment=None, value=None, units=None, balance=False;
          **kwargs passed to LcFragment
        :return:
        """
        if isinstance(flow, str):
            f = self.__getitem__(flow)
            if f is None:
                raise EntityNotFound(flow)
            if f.entity_type != 'flow':
                raise TypeError('%s is not a flow' % flow)
            flow = f
        frag = create_fragment(flow, direction, origin=self.origin, **kwargs)
        self._archive.add_entity_and_children(frag)
        return frag

    def name_fragment(self, fragment, name, **kwargs):
        self._archive.name_fragment(fragment, name)
        return fragment

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
        self._archive.delete_fragment(fragment)
        for c in fragment.child_flows:
            self.delete_fragment(c)
        return True

    def save(self, **kwargs):
        self._archive.save(**kwargs)
        return True

    def create_process_model(self, process, ref_flow=None, include_elementary=False, terminate=True, **kwargs):
        rx = process.reference(ref_flow)
        frag = self.new_fragment(rx.flow, rx.direction, value=1.0)
        frag.terminate(process)
        self.extend_process_model(frag, include_elementary=include_elementary, terminate=terminate, **kwargs)
        return frag

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
                ch.terminate(self._archive.catalog_ref(process.origin, ex.termination, entity_type='process'))
        fragment.observe(accept_all=True)
        return fragment

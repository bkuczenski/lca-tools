from collections import defaultdict

from lcatools.implementations import IndexImplementation
from lcatools.interfaces import ForegroundInterface, CONTEXT_STATUS_  # , comp_dir, BackgroundRequired

from lcatools.entities.flows import new_flow
from lcatools.entities.fragments import InvalidParentChild
from lcatools.entities.fragment_editor import create_fragment, clone_fragment, _fork_fragment


class NotForeground(Exception):
    pass


class ForegroundImplementation(IndexImplementation, ForegroundInterface):
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

    def get_canonical(self, quantity):
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
        ref_q = self.get_canonical(ref_quantity)
        f = new_flow(name, ref_q, **kwargs)
        self._archive.add_entity_and_children(f)
        return f

    def new_fragment(self, *args, **kwargs):
        """

        :param args: flow, direction (w.r.t. parent)
        :param kwargs: uuid=None, parent=None, comment=None, value=None, units=None, balance=False;
          **kwargs passed to LcFragment
        :return:
        """
        frag = create_fragment(*args, origin=self.origin, **kwargs)
        self._archive.add_entity_and_children(frag)
        return frag

    def name_fragment(self, fragment, name, **kwargs):
        self._archive.name_fragment(fragment, name)

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

    def split_subfragment(self, fragment, replacement=None):
        """
        Given a non-reference fragment, split it off into a new reference fragment, and create a surrogate child
        that terminates to it.
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

    def delete_fragment(self, fragment):
        """
        Remove the fragment and all its subfragments from the archive (they remain in memory)
        This does absolutely no safety checking.
        :param fragment:
        :return:
        """
        self._archive.delete_fragment(fragment)
        for c in fragment.child_flows:
            self.delete_fragment(c)

    def save(self):
        self._archive.save()

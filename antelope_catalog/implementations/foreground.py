from lcatools.implementations import BasicImplementation
from lcatools.interfaces import ForegroundInterface, CONTEXT_STATUS_  # , comp_dir, BackgroundRequired

from lcatools.entities.flows import new_flow
from lcatools.entities.fragment_editor import create_fragment  # , clone_fragment, split_subfragment


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

    def __init__(self,  *args, **kwargs):
        """

        :param fg_archive: must be a ForegroundArchive, which will be the object of management
        :param qdb: quantity database, used for compartments and id'ing flow properties--
        """
        super(ForegroundImplementation, self).__init__(*args, **kwargs)

        self._recursion_check = None  # prevent recursive loops on frag-from-node

    def fragments(self, **kwargs):
        if hasattr(self._archive, 'fragments'):
            # we only want reference fragments
            for f in self._archive.fragments(show_all=False, **kwargs):
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
        ref_q = self._archive.qdb.get_canonical(ref_quantity)
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


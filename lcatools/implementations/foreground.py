from .basic import BasicImplementation
from lcatools.interfaces import ForegroundInterface
from lcatools.exchanges import comp_dir

from lcatools.entities.editor import FragmentEditor


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
    def __init__(self,  *args, **kwargs):
        """

        :param fg_archive: must be a ForegroundArchive, which will be the object of management
        :param qdb: quantity database, used for compartments and id'ing flow properties--
        """
        super(ForegroundImplementation, self).__init__(*args, **kwargs)

        self._ed = FragmentEditor(interactive=False)  # only need Qdb if we want to create/edit flows

    @property
    def ed(self):
        return self.ed

    '''
    Create and modify fragments
    '''
    def new_fragment(self, *args, **kwargs):
        """

        :param args: flow, direction
        :param kwargs: uuid=None, parent=None, comment=None, value=None, balance=False; **kwargs passed to LcFragment
        :return:
        """
        frag = self.ed.create_fragment(*args, **kwargs)
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
        except StopIteration:
            if background is None:
                background = False
            bg = self.ed.create_fragment(exchange.flow, comp_dir(exchange.direction), background=background)
            bg.terminate(self._archive.catalog_ref(exchange.process.origin, exchange.termination,
                                                   entity_type='process'))
            self._archive.add_entity_and_children(bg)
        return bg

    def create_fragment_from_node(self, process, ref_flow=None, include_elementary=False):
        fg_exchs = process.foreground(ref_flow=ref_flow)
        rx = fg_exchs[0]
        comment = 'Created from foreground query to %s' % process.origin
        top_frag = self.ed.create_fragment(rx.flow, rx.direction,
                                           comment='Reference fragment; %s ' % comment)
        top_frag.terminate(process)

        if include_elementary:
            for x in process.elementary(fg_exchs[1:]):
                self.ed.create_fragment(x.flow, x.direction, parent=top_frag, value=x.value,
                                        comment='FG Emission; %s' % comment)

        for x in process.intermediate(fg_exchs[1:]):
            if x.termination is None:
                self.ed.create_fragment(x.flow, x.direction, parent=top_frag, value=x.value,
                                        comment='Cut-off; %s' % comment)

            else:
                child_frag = self.ed.create_fragment(x.flow, x.direction, parent=top_frag, value=x.value,
                                                     comment='Subfragment; %s' % comment)
                if process.is_in_background(termination=x.termination, ref_flow=x.flow):
                    bg = self.find_or_create_term(x, background=True)
                    child_frag.terminate(bg)
                else:
                    subfrag = self.ed.create_fragment_from_node(x.termination, ref_flow=x.flow,
                                                                include_elementary=include_elementary)
                    child_frag.terminate(subfrag)
        self._archive.add_entity_and_children(top_frag)
        return top_frag

    def clone_fragment(self, frag, **kwargs):
        """

        :param frag: the fragment (and subfragments) to clone
        :param kwargs: suffix (default: ' (copy)', applied to Name of top-level fragment only)
                       comment (override existing Comment if present; applied to all)
        :return:
        """
        clone = self.ed.clone_fragment(frag, **kwargs)
        self._archive.add_entity_and_children(clone)
        return clone

"""
I don't really want a foreground interface--- I want a foreground construction yard.  the query is not the right
route for this maybe?

I want to do the things in the editor: create and curate flows, terminate flows in processes, recurse on processes.

anything else?
child fragment flow generators and handlers. scenario tools.

scenarios currently live in the fragments- those need to be tracked somewhere. notably: on traversal, when parameters
are encountered.

Should that be here? how about monte carlo? where does that get done?

when do I go to bed?
"""

from .abstract_query import AbstractQuery


class ForegroundRequired(Exception):
    pass


_interface = 'foreground'


class ForegroundInterface(AbstractQuery):
    def new_fragment(self, *args, **kwargs):
        """

        :param args: flow, direction
        :param kwargs: uuid=None, parent=None, comment=None, value=None, balance=False; **kwargs passed to LcFragment
        :return:
        """
        return self._perform_query(_interface, 'new_fragment', ForegroundRequired, *args, **kwargs)

    def find_or_create_term(self, exchange, background=None):
        """
        Finds a fragment that terminates the given exchange
        :param exchange:
        :param background: [None] - any frag; [True] - background frag; [False] - foreground frag
        :return:
        """
        return self._perform_query(_interface, 'find_or_create_term', ForegroundRequired,
                                   exchange, background=background)

    def create_fragment_from_node(self, process, ref_flow=None, include_elementary=False):
        """

        :param process:
        :param ref_flow:
        :param include_elementary:
        :return:
        """
        return self._perform_query(_interface, 'create_fragment_from_node', ForegroundRequired,
                                   process, ref_flow=ref_flow, include_elementary=include_elementary)

    def clone_fragment(self, frag, **kwargs):
        """

        :param frag: the fragment (and subfragments) to clone
        :param kwargs: suffix (default: ' (copy)', applied to Name of top-level fragment only)
                       comment (override existing Comment if present; applied to all)
        :return:
        """
        return self._perform_query(_interface, 'clone_fragment', ForegroundRequired, frag, **kwargs)

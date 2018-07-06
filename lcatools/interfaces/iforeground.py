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

    def name_fragment(self, fragment, name, **kwargs):
        """

        :param fragment:
        :param name:
        :return:
        """
        return self._perform_query(_interface, 'name_fragment', ForegroundRequired, fragment, name, **kwargs)

    def find_or_create_term(self, exchange, background=None):
        """
        Finds a fragment that terminates the given exchange
        :param exchange:
        :param background: [None] - any frag; [True] - background frag; [False] - foreground frag
        :return:
        """
        return self._perform_query(_interface, 'find_or_create_term', ForegroundRequired,
                                   exchange, background=background)

    def create_fragment_from_node(self, process_ref, ref_flow=None, include_elementary=False):
        """

        :param process_ref: a ProcessRef
        :param ref_flow:
        :param include_elementary:
        :return:
        """
        return self._perform_query(_interface, 'create_fragment_from_node', ForegroundRequired,
                                   process_ref, ref_flow=ref_flow, include_elementary=include_elementary)

    def clone_fragment(self, frag, **kwargs):
        """

        :param frag: the fragment (and subfragments) to clone
        :param kwargs: suffix (default: ' (copy)', applied to Name of top-level fragment only)
                       comment (override existing Comment if present; applied to all)
        :return:
        """
        return self._perform_query(_interface, 'clone_fragment', ForegroundRequired, frag, **kwargs)

    def observe(self, fragment, exch_value, scenario=None, **kwargs):
        """
        Observe a fragment's exchange value with respect to its parent activity level.  Only applicable for
        non-balancing fragments whose parents are processes or foreground nodes (child flows of subfragments have
        their exchange values determined at traversal)
        :param fragment:
        :param exch_value:
        :param scenario:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'observe', ForegroundRequired('No access to fragment data'),
                                   fragment, exch_value, scenario=scenario, **kwargs)

    def set_balance_flow(self, fragment, **kwargs):
        """
        Specify that a given fragment is a balancing flow for the parent node, with respect to the specified fragment's
        flow's reference quantity.

        :param fragment:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'set_balance_flow', ForegroundRequired, fragment, **kwargs)

    def unset_balance_flow(self, fragment, **kwargs):
        """
        Specify that a given fragment's balance status should be removed.  The fragment's observed EV will remain at
        the most recently observed level.
        :param fragment:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'unset_balance_flow', ForegroundRequired, fragment, **kwargs)

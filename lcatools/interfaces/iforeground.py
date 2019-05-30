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
    def new_flow(self, name, ref_quantity, context=None, **kwargs):
        """
        Creates a new flow entity and adds it to the foreground
        :param name: required flow name
        :param ref_quantity:
        :param context: [None] Required for elementary flows. Should be a string
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'new_flow', ForegroundRequired('Foreground access required'),
                                   name, ref_quantity, context=context,
                                   **kwargs)

    def fragments(self, show_all=False, **kwargs):
        if show_all:
            raise ValueError('Cannot retrieve non-parent fragments via interface')
        for i in self._perform_query(_interface, 'fragments', ForegroundRequired('Foreground access required'),
                                     **kwargs):
            yield self.make_ref(i)

    def new_fragment(self, *args, **kwargs):
        """

        :param args: flow, direction
        :param kwargs: uuid=None, parent=None, comment=None, value=None, balance=False; **kwargs passed to LcFragment
        :return:
        """
        return self._perform_query(_interface, 'new_fragment', ForegroundRequired('Foreground access required'),
                                   *args, **kwargs)

    def name_fragment(self, fragment, name, **kwargs):
        """
        Assign a fragment a non-UUID external ref to facilitate its easy retrieval.  I suspect this should be
        constrained to reference fragments.
        :param fragment:
        :param name:
        :return:
        """
        return self._perform_query(_interface, 'name_fragment', ForegroundRequired('Foreground access required'),
                                   fragment, name, **kwargs)

    def fragments_with_flow(self, flow, direction=None, reference=None, background=None, **kwargs):
        """
        Generates fragments made with the specified flow, optionally filtering by direction, reference status, and
        background status.  For all three filters, the default None is to generate all fragments.
        :param flow:
        :param direction: [None | 'Input' | 'Output']
        :param reference: [None | False | True]
        :param background: [None | False | True]
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'fragments_with_flow', ForegroundRequired('Foreground access required'),
                                   flow, direction=direction, reference=reference, background=background, **kwargs)

    '''
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
    '''

    def clone_fragment(self, frag, **kwargs):
        """

        :param frag: the fragment (and subfragments) to clone
        :param kwargs: suffix (default: ' (copy)', applied to Name of top-level fragment only)
                       comment (override existing Comment if present; applied to all)
        :return:
        """
        return self._perform_query(_interface, 'clone_fragment', ForegroundRequired('Foreground access required'),
                                   frag, **kwargs)

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
        :param replacement:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'split_subfragment', ForegroundRequired('Foreground access required'),
                                   fragment, replacement=replacement, **kwargs)

    def delete_fragment(self, fragment, **kwargs):
        """
        Remove the fragment and all its subfragments from the archive (they remain in memory)
        This does absolutely no safety checking.

        :param fragment:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'delete_fragment', ForegroundRequired('Foreground access required'),
                                   fragment, **kwargs)

    def save(self, **kwargs):
        """
        Save the foreground to local storage.  Revert is not supported for now
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'save', ForegroundRequired('Foreground access required'), **kwargs)

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
        return self._perform_query(_interface, 'set_balance_flow', ForegroundRequired('Foreground access required'),
                                   fragment, **kwargs)

    def unset_balance_flow(self, fragment, **kwargs):
        """
        Specify that a given fragment's balance status should be removed.  The fragment's observed EV will remain at
        the most recently observed level.
        :param fragment:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'unset_balance_flow', ForegroundRequired('Foreground access required'),
                                   fragment, **kwargs)

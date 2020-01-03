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
    def new_quantity(self, name, ref_unit=None, **kwargs):
        """
        Creates a new quantity entity and adds it to the foreground
        :param name:
        :param ref_unit:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'new_quantity', ForegroundRequired('Foreground access required'),
                                   name, ref_unit=ref_unit, **kwargs)

    def new_flow(self, name, ref_quantity=None, context=None, **kwargs):
        """
        Creates a new flow entity and adds it to the foreground
        :param name: required flow name
        :param ref_quantity: [None] implementation must handle None / specify a default
        :param context: [None] Required if flow is strictly elementary. Should be a tuple
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'new_flow', ForegroundRequired('Foreground access required'),
                                   name, ref_quantity=ref_quantity, context=context,
                                   **kwargs)

    def fragments(self, show_all=False, **kwargs):
        if show_all:
            raise ValueError('Cannot retrieve non-parent fragments via interface')
        for i in self._perform_query(_interface, 'fragments', ForegroundRequired('Foreground access required'),
                                     **kwargs):
            yield self.make_ref(i)

    def frag(self, string, many=False, **kwargs):
        """
        Return the unique fragment whose ID starts with string.

        Default: If the string is insufficiently specific (i.e. there are multiple matches), raise
        :param string:
        :param many: [False] if true, return a generator and don't raise an error
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'frag', ForegroundRequired('Foreground access required'),
                                   string, many=many, **kwargs)

    def find_term(self, term_ref, origin=None, **kwargs):
        """
        Find a termination for the given reference.  Essentially do type and validity checking and return something
        that can be used as a valid termination.
        :param term_ref: either an entity, entity ref, or string
        :param origin: if provided, interpret term_ref as external_ref
        :param kwargs:
        :return: either a context, or a process_ref, or a flow_ref, or a fragment or fragment_ref, or None
        """
        return self._perform_query(_interface, 'find_term', ForegroundRequired('blah'),
                                   term_ref, origin=origin, **kwargs)

    def new_fragment(self, flow, direction, **kwargs):
        """
        Create a fragment and add it to the foreground.

        If creating a child flow ('parent' kwarg is non-None), then supply the direction with respect to the parent
        fragment. Otherwise, supply the direction with respect to the newly created fragment.  Example: for a fragment
        for electricity production:

        >>> fg = ForegroundInterface(...)
        >>> elec = fg.new_flow('Electricity supply fragment', 'kWh')
        >>> my_frag = fg.new_fragment(elec, 'Output')
        >>> child = fg.new_fragment(elec, 'Input', parent=my_frag, balance=True)
        >>> child.terminate(elec_production_process)

        :param flow: a flow entity/ref, or an external_ref known to the foreground
        :param direction:
        :param kwargs: uuid=None, parent=None, comment=None, value=None, balance=False; **kwargs passed to LcFragment
        :return: the fragment? or a fragment ref? <== should only be used in the event of a non-local foreground
        """
        return self._perform_query(_interface, 'new_fragment', ForegroundRequired('Foreground access required'),
                                   flow, direction, **kwargs)

    def name_fragment(self, fragment, name, auto=None, force=None, **kwargs):
        """
        Assign a fragment a non-UUID external ref to facilitate its easy retrieval.  I suspect this should be
        constrained to reference fragments.  By default, if the requested name is taken, a ValueError is raised
        :param fragment:
        :param auto: if True, if name is taken, apply an auto-incrementing numeric suffix until a free name is found
        :param force: if True, if name is taken, de-name the prior fragment and assign the name to the current one
        :return:
        """
        print('name_fragment DEPRECATED - use observe()')
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
        :param kwargs: save_unit_scores [False]: whether to save cached LCIA results (for background fragments only)
        :return:
        """
        return self._perform_query(_interface, 'save', ForegroundRequired('Foreground access required'), **kwargs)

    def observe(self, fragment, exch_value, name=None, scenario=None, **kwargs):
        """
        Observe a fragment's exchange value with respect to its parent activity level.  Only applicable for
        non-balancing fragments whose parents are processes or foreground nodes (child flows of subfragments have
        their exchange values determined at traversal, as do balancing flows).

        A fragment should be named when it is observed.  This should replace name_fragment. In a completed model, all
        observable fragments should have names.
        :param fragment:
        :param exch_value:
        :param name:
        :param scenario:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'observe', ForegroundRequired('No access to fragment data'),
                                   fragment, exch_value, name=name, scenario=scenario, **kwargs)

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

    def create_process_model(self, process, ref_flow=None, include_elementary=False, terminate=True, **kwargs):
        """
        Create a fragment from a process_ref.  If process has only one reference exchange, it will be used automatically.
        By default, a child fragment is created for each exchange not terminated to context, and exchanges terminated
        to nodes are so terminated in the fragment.
        :param process:
        :param ref_flow: specify which exchange to use as a reference
        :param include_elementary: [False] if true, create subfragments terminating to context for elementary flows.
         otherwise leaves them unspecified (fragment LCIA includes unobserved exchanges)
        :param terminate: [True] if false, create all flows as cutoff flows.
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'create_process_model', ForegroundRequired('Foreground access required'),
                                   process, ref_flow=ref_flow, include_elementary=include_elementary,
                                   terminate=terminate,
                                   **kwargs)

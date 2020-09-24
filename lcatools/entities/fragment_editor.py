from collections import defaultdict

from antelope import check_direction, comp_dir
from .fragments import LcFragment, DependentFragment


def create_fragment(flow, direction, parent=None, **kwargs):
    """
    User-facing pass-through function. Identical to internal function except that reference fragment direction is
    reversed for UX purposes- to give direction with respect to newly created reference fragment (internal direction
    is always given with respect to parent)
    :param flow:
    :param direction:
    :param parent:
    :param kwargs: uuid, name, comment, value, balance=False, units, ...
    :return:
    """
    if parent is None:
        return _create_fragment(flow, comp_dir(direction), parent=parent, **kwargs)
    else:
        return _create_fragment(flow, direction, parent=parent, **kwargs)


def _create_fragment(flow, direction, uuid=None, parent=None, name=None, comment=None, value=None, balance=False,
                     **kwargs):
    """

    :param flow:
    :param direction:
    :param uuid:
    :param parent:
    :param name:
    :param comment:
    :param value:
    :param balance:
    :param kwargs:
    :return:
    """
    direction = check_direction(direction)
    if name is None:
        name = flow['Name']
    name = kwargs.pop('Name', name)

    if comment is None:
        comment = ''
    comment = kwargs.pop('Comment', comment)
    if parent is None:
        if value is None:
            value = 1.0
        if uuid is None:
            frag = LcFragment.new(name, flow, direction, Comment=comment, exchange_value=value,
                                  **kwargs)
        else:
            frag = LcFragment(uuid, flow, direction, Comment=comment, exchange_value=value, Name=name,
                              **kwargs)
    else:
        if parent.term.is_null:
            parent.to_foreground()
        if balance or parent.term.is_subfrag:
            # exchange value set during traversal
            value = None

        if uuid is None:
            frag = LcFragment.new(name, flow, direction, parent=parent, Comment=comment,
                                  exchange_value=value, balance_flow=balance, **kwargs)
        else:
            frag = LcFragment(uuid, flow, direction, parent=parent, Comment=comment, exchange_value=value,
                              balance_flow=balance, Name=name, **kwargs)

        # traverse -- may not need to do this anymore if we switch to live traversals for everything
        # parent.traverse(None)  # in fact, let's skip it

    # this cannot be done internally-- really we need create_fragment_from_exchange to do this
    # if flow.context.elementary:
    #     frag.terminate(flow.context)

    return frag


def _transfer_evs(frag, new):
    if frag.observed_ev != 0 and new.observable():
        new.observed_ev = frag.observed_ev
    for scen in frag.exchange_values():
        if scen != 0 and scen != 1 and new.observable(scen):
            new.set_exchange_value(scen, frag.exchange_value(scen))


def clone_fragment(frag, suffix=' (copy)', comment=None, _parent=None, origin=None):
    """
    Creates duplicates of the fragment and its children. returns the new reference fragment.
    :param frag:
    :param _parent: used internally
    :param suffix: attached to top level fragment
    :param origin:
    :param comment: can be used in place of source fragment's comment
    :return:
    """
    if suffix is None:
        suffix = ''
    if origin is None:
        origin = frag.origin
    the_comment = comment or frag['Comment']
    new = _create_fragment(parent=_parent, origin=origin,
                           Name=frag['Name'] + suffix, StageName=frag['StageName'],
                           flow=frag.flow, direction=frag.direction, comment=the_comment,
                           value=frag.cached_ev, balance=frag.balance_flow,
                           background=frag.is_background)

    _transfer_evs(frag, new)

    for t_scen, term in frag.terminations():
        if term.term_node is frag:
            new.to_foreground(scenario=t_scen)
        else:
            new.terminate(term.term_node, term_flow=term.term_flow, descend=term.descend,
                          scenario=t_scen)

    for c in frag.child_flows:
        clone_fragment(c, _parent=new, suffix='', origin=origin)
    return new


def _fork_fragment(fragment, comment=None):
    """
    create a new fragment between the given fragment and its parent. if flow is None, uses the same flow.
    direction is the same as the given fragment. exchange value is shifted to new frag; given frag's ev is
    set to 1.

    The newly created fragment is not terminated and the original fragment's parent is not corrected.  Both of
    these must be done by the calling function before the fragment will have a sensible topology.

    :param fragment:
    :param comment:
    :return: the new fragment
    """
    old_parent = fragment.reference_entity
    subfrag = _create_fragment(parent=old_parent, flow=fragment.flow, direction=fragment.direction,
                               comment=comment, value=fragment.cached_ev,
                               balance=fragment.balance_flow, origin=fragment.origin)
    _transfer_evs(fragment, subfrag)
    fragment.clear_evs()
    return subfrag


def interpose(fragment):
    """
    Insert a new foreground node in-line between the fragment and its parent, terminating it to the foreground and
    making the specified fragment a child flow.
    given fragment sets the new frag as its parent.
    """
    interp = _fork_fragment(fragment, comment='Interposed node')

    interp.to_foreground()
    fragment.set_parent(interp)

    return interp


def split_subfragment(fragment):
    """
    This method is like interpose except a new reference fragment is created.  The new node becomes a
    cutoff w/r/t its parent, and then gets terminated to the new reference fragment as a subfragment.

    Exchange value and balancing status stays with parent.

    :param fragment:
    :return:
    """
    if fragment.reference_entity is None:
        raise AttributeError('Fragment is already a reference fragment')

    surrogate = _fork_fragment(fragment, comment='New subfragment')

    fragment.unset_parent()
    surrogate.terminate(fragment)

    return fragment



'''
class FragmentEditor(EntityEditor):
    def create_fragment(self, flow, direction, uuid=None, parent=None, comment=None, value=None, balance=False,
                        units=None, **kwargs):
        """

        :param parent:
        :param flow: required. flow or catalog_ref
        :param direction: required. 'Input' or 'Output'
          (note that direction reversal for reference fragments happens internally!)
        :param uuid: [None]
        :param comment:
        :param value:
        :param balance:
        :param units: [None] if both value and unit are non-None, interpret value as given in units and convert
        :param kwargs:
        :return:
        """
        self._print('Creating fragment with flow %s' % flow)
        if direction not in directions:
            direction = None
        if self._interactive:
            direction = direction or {'i': 'Input', 'o': 'Output'}[cyoa('flow is (I)nput or (O)utput?', 'IO').lower()]
        if direction is None:
            raise ValueError('Must supply direction')
        if 'Name' in kwargs:
            name = kwargs.pop('Name')
        else:
            name = flow['Name']

        # TODO: add me to ContextRefactor
        if value is not None and units is not None:
            value *= flow.reference_entity.convert(units)

        comment = comment or self.ifinput('Enter FragmentFlow comment: ', '')
        if parent is None:
            # direction reversed for UX! user inputs direction w.r.t. fragment, not w.r.t. parent
            if value is None:
                value = 1.0

            if uuid is None:
                frag = LcFragment.new(name, flow, comp_dir(direction), Comment=comment, exchange_value=value,
                                      **kwargs)
            else:
                frag = LcFragment(uuid, flow, comp_dir(direction), Comment=comment, exchange_value=value, Name=name,
                                  **kwargs)
        else:
            if parent.term.is_null:
                parent.to_foreground()
            if balance or parent.term.is_subfrag:
                if self._interactive:
                    print('Exchange value set during traversal')
                value = 1.0
            else:
                if value is None:
                    val = self.ifinput('Exchange value (%s per %s): ' % (flow.unit(), parent.unit), '1.0')
                    if val == '1.0':
                        value = 1.0
                    else:
                        value = parse_math(val)

            if uuid is None:
                frag = LcFragment.new(name, flow, direction, parent=parent, Comment=comment,
                                      exchange_value=value, balance_flow=balance, **kwargs)
            else:
                frag = LcFragment(uuid, flow, direction, parent=parent, Comment=comment, exchange_value=value,
                                  balance_flow=balance, Name=name, **kwargs)

        """ # can't hack this without a qdb
        if self._qdb.is_elementary(frag.flow):
            frag.terminate(frag.flow)
        """
        return frag

    @staticmethod
    def _update_ev(frag, scenario):
        val = frag.exchange_value(scenario)
        cur = str(val)
        upd = ifinput('New value: ', cur)
        if upd != cur:
            new_val = parse_math(upd)
            if new_val != val:
                if scenario is None:
                    frag.reset_cache()
                    frag.cached_ev = new_val
                else:
                    frag.set_exchange_value(scenario, new_val)

    def revise_exchanges(self, frag, scenario=None):
        """
        interactively update reference exchange values (or values for a particular scenario) for the children of
        a given fragment
        :param frag:
        :param scenario:
        :return:
        """
        print('Reference flow: [%s] %s' % (comp_dir(frag.direction), frag.unit))
        if scenario is None:
            print('Update reference flow')
        else:
            print('Update reference flow for scenario "%s"' % scenario)
        self._update_ev(frag, scenario)
        for c in frag.child_flows:
            print('   Child flow: %s ' % c)
            if scenario is None:
                print('Update default value')
            else:
                print('Update value for scenario "%s"' % scenario)
            self._update_ev(c, scenario)

    @staticmethod
    def transfer_evs(frag, new):
        if frag.observed_ev != 0 and new.observable():
            new.observed_ev = frag.observed_ev
        for scen in frag.exchange_values():
            if scen != 0 and scen != 1 and new.observable(scen):
                new.set_exchange_value(scen, frag.exchange_value(scen))

    def clone_fragment(self, frag, suffix=' (copy)', comment=None, origin=None, _parent=None):
        """
        Creates duplicates of the fragment and its children. returns the new reference fragment.
        :param frag:
        :param _parent: used internally
        :param suffix: attached to top level fragment
        :param comment: can be used in place of source fragment's comment
        :param origin: [None] defaults to frag.origin
        :return:
        """
        if _parent is None:
            direction = comp_dir(frag.direction)  # this gets re-reversed in create_fragment
        else:
            direction = frag.direction
        if suffix is None:
            suffix = ''
        if origin is None:
            origin = frag.origin
        the_comment = comment or frag['Comment']
        new = self.create_fragment(parent=_parent, origin=origin,
                                   Name=frag['Name'] + suffix, StageName=frag['StageName'],
                                   flow=frag.flow, direction=direction, comment=the_comment,
                                   value=frag.cached_ev, balance=frag.is_balance,
                                   background=frag.is_background)

        self.transfer_evs(frag, new)

        for t_scen, term in frag.terminations():
            if term.is_null:
                continue
            elif term.term_node is frag:
                new.to_foreground(scenario=t_scen)
            else:
                new.terminate(term.term_node, term_flow=term.term_flow, direction=term.direction,
                              descend=term.descend, inbound_ev=term.inbound_exchange_value,
                              scenario=t_scen)

        for c in frag.child_flows:
            self.clone_fragment(c, _parent=new, suffix='')
        return new

    def _fork_fragment(self, fragment, comment=None):
        """
        create a new fragment between the given fragment and its parent. if flow is None, uses the same flow.
        direction is the same as the given fragment. exchange value is shifted to new frag; given frag's ev is
        set to 1.

        The newly created fragment is not terminated and the original fragment's parent is not corrected.  Both of
        these must be done by the calling function before the fragment will have a sensible topology.

        :param fragment:
        :param comment:
        :return: the new fragment
        """
        old_parent = fragment.reference_entity
        subfrag = self.create_fragment(parent=old_parent, flow=fragment.flow, direction=fragment.direction,
                                       comment=comment, value=fragment.cached_ev,
                                       balance=fragment.is_balance)
        self.transfer_evs(fragment, subfrag)
        fragment.clear_evs()
        return subfrag

    def interpose(self, fragment):
        """
        Insert a new foreground node in-line between the fragment and its parent, terminating it to the foreground and
        making the specified fragment a child flow.
        given fragment sets the new frag as its parent.
        """
        interp = self._fork_fragment(fragment, comment='Interposed node')

        interp.to_foreground()
        fragment.set_parent(interp)

        return interp

    def split_subfragment(self, fragment):
        """
        This method is like interpose except a new reference fragment is created.  The new node becomes a
        cutoff w/r/t its parent, and then gets terminated to the new reference fragment as a subfragment.

        Exchange value and balancing status stays with parent.

        :param fragment:
        :return:
        """
        surrogate = self._fork_fragment(fragment, comment='New subfragment')

        fragment.unset_parent()
        surrogate.terminate(fragment)

        return fragment
<<<<<<< HEAD:antelope_catalog/foreground/fragment_editor.py
'''


def set_child_exchanges(fragment, scenario=None, reset_cache=False):
    """
    This is really client code, doesn't use any private capabilities
    Set exchange values of child flows based on inventory data for the given scenario.  The termination must be
     a foreground process.

    In order for this function to work, flows in the node's exchanges have to have the SAME external_ref as the
    child flows, though origins can differ.  There is no other way for the exchanges to be set automatically from
    the inventory.  Requiring that the flows have the same name, CAS number, compartment, etc. is too fragile /
    arbitrary.  The external refs must match.

    This works out okay for databases that use a consistent set of flows internally -- ILCD, thinkstep, and
    ecoinvent all seem to have that characteristic but ask me again in a year.

    To automatically set child exchanges for different scenarios that use processes from different databases,
    encapsulate each term node inside a sub-fragment, and then specify different subfragment terminations for the
    different scenarios.  Then, map each input / output in the sub-fragment to the correct foreground flow using
    a conserving child flow.

    In that case, the exchange values will be set during traversal, and each sub-fragment's internal exchange
    values can be set automatically using set_child_exchanges.

    :param fragment:
    :param scenario: [None] for the default scenario, set observed ev
    :param reset_cache: [False] if True, for the default scenario set cached ev
    :return:
    """
    term = fragment.termination(scenario)
    if not term.term_node.entity_type == 'process':
        raise DependentFragment('Child flows are set during traversal')

    if scenario is None and fragment.reference_entity is None:
        # this counts as observing the reference flow
        if fragment.observed_ev == 0:
            fragment.observed_ev = fragment.cached_ev

    children = defaultdict(list)  # need to allow for differently-terminated child flows -- distinguish by term.id

    for k in fragment.child_flows:
        key = (k.flow.external_ref, k.direction)
        children[key].append(k)
    if len(children) == 0:
        return

    for x in term.term_node.inventory(ref_flow=term.term_flow):
        if x.value is None:
            fragment.dbg_print('skipping None-valued exchange: %s' % x)
            continue

        key = (x.flow.external_ref, x.direction)
        if key in children:
            try:
                if len(children[key]) > 1:
                    child = next(c for c in children[key] if c.termination(scenario).id == x.termination)
                else:
                    child = next(c for c in children[key])
            except StopIteration:
                continue

            fragment.dbg_print('setting %s [%10.3g]' % (child, x.value))
            if scenario is None:
                if reset_cache:
                    child.reset_cache()
                    child.cached_ev = x.value
                else:
                    child.observed_ev = x.value
            else:
                child.set_exchange_value(scenario, x.value)

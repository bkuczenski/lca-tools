from .fragments import LcFragment

from lcatools.interfaces import directions, comp_dir
from lcatools.interact import parse_math, cyoa, ifinput
from lcatools.entities.editor import EntityEditor


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
                                   value=frag.cached_ev, balance=frag.balance_flow,
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
                                       balance=fragment.balance_flow)
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

        interp.term.self_terminate()
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

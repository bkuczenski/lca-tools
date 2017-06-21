from lcatools.interact import ifinput, pick_one, pick_compartment, cyoa, parse_math, menu_list
from lcatools.entities import LcQuantity, LcFlow, LcFragment
from lcatools.providers.qdb import Qdb
from lcatools.exchanges import directions, comp_dir
from lcatools.terminations import FlowTermination


class EntityEditor(object):
    """
    Abstract class with some common functions
    """
    @staticmethod
    def _edit_entity(entity):
        print('Select field to edit:')
        field = menu_list(*entity.keys())
        if field == -1 or field is None:
            return True
        new = ifinput('Enter new value for %s: ' % field, entity[field])
        if len(new) > 0:
            entity[field] = new
        else:
            print('Not updating.')

    def __init__(self, qdb=None, interactive=False):
        """
        The class needs a quantity database to give the user access to properly defined quantities and compartments.
        :param qdb: [None] if None, uses the old FlowDB
        """
        if qdb is None:
            qdb = Qdb()
        self._qdb = qdb
        self._interactive = interactive

    def set_interactive(self):
        self._interactive = True

    def unset_interactive(self):
        self._interactive = False

    def input(self, query, default):
        if self._interactive:
            return input(query)
        return default

    def ifinput(self, query, default):
        if self._interactive:
            return ifinput(query, default)
        return default

    def _print(self, string):
        if self._interactive:
            print(string)


class FlowEditor(EntityEditor):
    """
    Not even much of an object in itself, more like a collection of functions for creating and modifying LcEntities.
    I suppose it needs an instantiation so that it can store customization info. But really these are orphan
    functions and this class is just created to give them a home.
    """
    def new_quantity(self, name=None, unit=None, comment=None):
        name = name or self.input('Enter quantity name: ', 'New Quantity')
        unit = unit or self.input('Unit by string: ', 'unit')
        comment = comment or self.ifinput('Quantity Comment: ', '')
        q = LcQuantity.new(name, unit, Comment=comment)
        return q

    def new_flow(self, flow=None, name=None, cas=None, quantity=None, comment=None, compartment=None, local_unit=None):
        if flow is None:
            name = name or self.input('Enter flow name: ', 'New flow')
            cas = cas or self.ifinput('Enter CAS number (or none): ', '')
            if quantity is None:
                if self._interactive:
                    print('Choose reference quantity or none to create new: ')
                    q = pick_one([fp for fp in self._qdb.flow_properties])
                    if q is None:
                        q = self.new_quantity()
                    quantity = q
                else:
                    print('Using mass as reference quantity')
                    quantity = self._qdb.get_quantity('mass')
            comment = comment or self.input('Enter flow comment: ', '')
            if compartment is None:
                if self._interactive:
                    print('Choose compartment:')
                    compartment = pick_compartment(self._qdb.c_mgr.compartments).to_list()
                else:
                    print('Designating Intermediate flow')
                    compartment = self._qdb.c_mgr.find_matching('Intermediate Flows').to_list()
            else:
                compartment = self._qdb.c_mgr.find_matching(compartment).to_list()
            if local_unit is not None:
                local_conv = quantity.convert(to=local_unit)
                if local_conv is None:
                    print('Falling back to default unit: %s' % quantity.unit())
                    local_unit = None

            flow = LcFlow.new(name, quantity, CasNumber=cas, Compartment=compartment, Comment=comment,
                              local_unit=local_unit)
            # flow.add_characterization(q, reference=True)
        else:
            quantity = flow.reference_entity

        if self._interactive:
            flow.profile()
            while ifinput('Add characterizations for this flow? y/n', 'n') != 'n':
                ch = cyoa('[n]ew or [e]xisting quantity? ', 'en', 'e')
                if ch == 'n':
                    cq = self.new_quantity()
                else:
                    cq = pick_one(self._qdb.quantities())
                    if cq is None:
                        cq = self.new_quantity()
                val = parse_math(input('Value (1 %s = x %s): ' % (quantity.unit(), cq.unit())))
                flow.add_characterization(cq, value=val)

        return flow

    def edit_flow(self, flow):
        ch = cyoa('Edit (P)roperties or (C)haracterizations? ', 'pc').lower()
        if ch == 'p':
            self._edit_entity(flow)
        elif ch == 'c':
            self.edit_characterizations(flow)

    @staticmethod
    def edit_characterizations(flow):
        char = pick_one(cf for cf in flow.characterizations())
        val = float(ifinput('enter new characterization value: ', char.value))
        char.value = val


class FragmentEditor(FlowEditor):
    def create_fragment(self, flow, direction, uuid=None, parent=None, comment=None, value=None, balance=False,
                        **kwargs):
        """

        :param parent:
        :param flow: required. flow or catalog_ref
        :param direction: required. 'Input' or 'Output'
        :param uuid: [None]
        :param comment:
        :param value:
        :param balance:
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

            # traverse -- may not need to do this anymore if we switch to live traversals for everything
            parent.traversal_entry(None)

        if self._qdb.is_elementary(frag.flow):
            frag.terminate(frag.flow)
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

    def interpose(self, frag):
        """
        create a new fragment between the given fragment and its parent. if flow is None, uses the same flow.
        direction is the same as the given fragment. exchange value is shifted to new frag; given frag's ev is
        set to 1.
        given fragment sets the new frag as its parent.
        """
        interp = self.create_fragment(parent=frag.reference_entity, flow=frag.flow, direction=frag.direction,
                                      comment='Interposed flow', value=frag.cached_ev)
        interp.term.self_terminate()
        self.transfer_evs(frag, interp)
        frag.clear_evs()
        frag.reference_entity = interp
        return interp

    def clone_fragment(self, frag, suffix=' (copy)', comment=None, _parent=None):
        """
        Creates duplicates of the fragment and its children. returns the reference fragment.
        :param frag:
        :param _parent: used internally
        :param suffix: attached to top level fragment
        :param comment: can be used in place of source fragment's comment
        :return:
        """
        if _parent is None:
            direction = comp_dir(frag.direction)  # this gets re-reversed in create_fragment
        else:
            direction = frag.direction
        if suffix is None:
            suffix = ''
        the_comment = comment or frag['Comment']
        new = self.create_fragment(parent=_parent,
                                   Name=frag['Name'] + suffix, StageName=frag['StageName'],
                                   flow=frag.flow, direction=direction, comment=the_comment,
                                   value=frag.cached_ev, balance=frag.balance_flow,
                                   background=frag.is_background)

        self.transfer_evs(frag, new)

        for t_scen, term in frag.terminations():
            if term.term_node is frag:
                new.to_foreground(scenario=t_scen)
            else:
                new.terminate(term.term_node, term_flow=term.term_flow, direction=term.direction,
                              descend=term.descend, inbound_ev=term.inbound_exchange_value,
                              scenario=t_scen)

        for c in frag.child_flows:
            self.clone_fragment(c, _parent=new, suffix='')
        return new

    def split_subfragment(self, fragment):
        """
        This method takes a child fragment and creates a new subfragment with the same termination; then
        replaces the child with a surrogate that points to the new subfragment.  All terminations move.
        exchange value stays with parent.
        :param fragment:
        :return:
        """
        old_parent = fragment.reference_entity
        fragment.reference_entity = None
        surrogate = self.create_fragment(parent=old_parent, flow=fragment.flow, direction=fragment.direction,
                                         comment='Moved to subfragment', value=fragment.cached_ev,
                                         balance=fragment.balance_flow)
        self.transfer_evs(fragment, surrogate)
        fragment.clear_evs()

        surrogate.terminate(fragment)
        return surrogate

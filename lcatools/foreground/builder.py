"""
This module contains utility functions for building fragments. It should probably be called 'fragment_builder.py'
but that name was taken.
"""

from lcatools.interact import pick_one, cyoa, ifinput, pick_list, menu_list, pick_one_or, pick_compartment
from lcatools.foreground.manager import ForegroundManager
from lcatools.foreground.fragment_flows import LcFragment, parse_math
from lcatools.entities import LcFlow, LcQuantity
from lcatools.exchanges import Exchange, comp_dir
from lcatools.catalog import CatalogRef, ExchangeRef


def select_archive(F):
    F.show_all()
    ch1 = input('Search which catalog? or blank to search all loaded catalogs')
    if len(ch1) == 0:
        index = None
    else:
        try:
            index = int(ch1)
        except ValueError:
            index = F._catalog.get_index(ch1)
    F.current_archive = index
    return index


class ForegroundBuilder(ForegroundManager):

    def __init__(self, *args, **kwargs):
        super(ForegroundBuilder, self).__init__(*args, **kwargs)
        self.current_archive = None

    def new_quantity(self, name=None, unit=None, comment=None):
        name = name or input('Enter quantity name: ')
        unit = unit or input('Unit by string: ')
        comment = comment or ifinput('Quantity Comment: ', '')
        q = LcQuantity.new(name, unit, Comment=comment)
        self._catalog[0].add(q)
        return q

    def new_flow(self, flow=None, name=None, cas=None, quantity=None, comment=None, compartment=None):
        if flow is None:
            name = name or input('Enter flow name: ')
            cas = cas or ifinput('Enter CAS number (or none): ', '')
            print('Choose reference quantity or none to create new: ')
            if quantity is None:
                q = pick_one(self.flow_properties)
                if q is None:
                    q = self.new_quantity()
                quantity = q
            comment = comment or input('Enter flow comment: ')
            if compartment is None:
                print('Choose compartment:')
                compartment = pick_compartment(self.db.compartments).to_list()
            flow = LcFlow.new(name, quantity, CasNumber=cas, Compartment=compartment, Comment=comment)
            # flow.add_characterization(q, reference=True)
        else:
            quantity = flow.reference_entity

        self._catalog[0].add(flow)
        flow.profile()
        while ifinput('Add characterizations for this flow? y/n', 'n') != 'n':
            ch = cyoa('[n]ew or [e]xisting quantity? ', 'en', 'e')
            if ch == 'n':
                cq = self.new_quantity()
            else:
                cq = pick_one(self[0].quantities())
                if cq is None:
                    cq = self.new_quantity()
            val = parse_math(input('Value (1 %s = x %s): ' % (quantity.unit(), cq.unit())))
            flow.add_characterization(cq, value=val)

        return flow

    def edit_flow(self):
        flow = pick_one(self._catalog[0].flows())
        ch = cyoa('Edit (P)roperties or (C)haracterizations? ', 'pc').lower()
        if ch == 'p':
            self._edit_entity(flow)
        elif ch == 'c':
            self.edit_characterizations(flow)

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

    @staticmethod
    def edit_characterizations(flow):
        char = pick_one(cf for cf in flow.characterizations())
        val = float(ifinput('enter new characterization value: ', char.value))
        char.value = val

    def find_flow(self, name, index=None, elementary=False):
        if name is None:
            name = input('Enter flow name search string: ')
        res = self.search(index, 'flow', Name=name, show=False)
        if elementary is not None:
            res = list(filter(lambda x: self.db.is_elementary(x.entity()) == elementary, res))
        pick = pick_one(res)
        print('Picked: %s' % pick)
        return pick

    def new_fragment(self, flow, direction, termination=None, **kwargs):
        if isinstance(flow, CatalogRef):
            flow = flow.entity()
        frag = self[0].create_fragment(flow, direction, **kwargs)
        if termination is not None:
            frag.terminate(termination)  # None scenario
        return frag

    def create_fragment(self, parent=None, flow=None, direction=None, comment=None, value=None, balance=False,
                        **kwargs):
        """

        :param parent:
        :param flow:
        :param direction:
        :param comment:
        :param value:
        :param balance:
        :param kwargs:
        :return:
        """
        if flow is None:
            ch = cyoa('(N)ew flow or (S)earch for flow? ', 'ns')
            if ch == 'n':
                flow = self.new_flow()
            elif ch == 's':
                index = self.current_archive or select_archive(self)
                elem = {'i': False,
                        'e': True,
                        'a': None}[cyoa('(I)ntermediate, (E)lementary, or (A)ll flows? ', 'aei', 'I').lower()]
                flow = self.find_flow(None, index=index, elementary=elem)
                if flow is None:
                    return None
                flow = flow.entity()
            else:
                raise ValueError
        print('Creating fragment with flow %s' % flow)
        direction = direction or {'i': 'Input', 'o': 'Output'}[cyoa('flow is (I)nput or (O)utput?', 'IO').lower()]
        comment = comment or ifinput('Enter FragmentFlow comment: ', '')
        if parent is None:
            # direction reversed for UX! user inputs direction w.r.t. fragment, not w.r.t. parent
            if value is None:
                value = 1.0

            frag = self.new_fragment(flow, comp_dir(direction), Comment=comment, exchange_value=value, **kwargs)
        else:
            if parent.term.is_null:
                self.terminate_to_foreground(parent)
            if value is None:
                val = ifinput('Exchange value (%s per %s): ' % (flow.unit(), parent.unit), '1.0')
                if val == '1.0':
                    value = 1.0
                else:
                    value = parse_math(val)

            frag = self[0].add_child_fragment_flow(parent, flow, direction, Comment=comment, exchange_value=value,
                                                   **kwargs)
            if balance:
                frag.set_balance_flow()
                self.traverse(parent)

        if self.db.is_elementary(frag.flow):
            self.terminate_to_foreground(frag)
        return frag

    def aggregate_subfrags(self, fragment, scenario=None, descend=False):
        """
        set all subfragment terminations to aggregate
        :param fragment:
        :param scenario:
        :param descend: what you actually set it to (default False)
        :return:
        """
        term = fragment.termination(scenario)
        if not term.is_null:
            if term.term_node.entity_type == 'fragment':
                term.descend = descend
        for c in self.child_flows(fragment):
            self.aggregate_subfrags(c, scenario=scenario)

    def merge_backgrounds(self, old, new):
        """
        terminations linking to the old fragment will be transferred to the new one. not strictly limited to
        bg fragments
        :param old:
        :param new:
        :return:
        """
        for t in self[0].linked_terms(old):
            t.update(new)

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
        for c in self.child_flows(frag):
            print('   Child flow: %s ' % c)
            if scenario is None:
                print('Update default value')
            else:
                print('Update value for scenario "%s"' % scenario)
            self._update_ev(c, scenario)

    @staticmethod
    def transfer_evs(frag, new):
        if frag.observed_ev != 0:
            new.observed_ev = frag.observed_ev
        for scen in frag.exchange_values():
            if scen != 0 and scen != 1:
                new.set_exchange_value(scen, frag.exchange_value(scen))

    def clone_fragment(self, frag, parent=None, suffix=' (copy)'):
        """
        Creates duplicates of the fragment and its children. returns the reference fragment.
        :param frag:
        :param parent: used internally
        :param suffix: attached to top level fragment
        :return:
        """
        if parent is None:
            parent = frag.reference_entity
        if parent is None:
            direction = comp_dir(frag.direction)  # this gets re-reversed in create_fragment
        else:
            direction = frag.direction
        new = self.create_fragment(parent=parent,
                                   Name=frag['Name'] + suffix, StageName=frag['StageName'],
                                   flow=frag.flow, direction=direction, comment=frag['Comment'],
                                   value=frag.cached_ev, balance=frag._balance_flow)

        self.transfer_evs(frag, new)

        for t_scen in frag.terminations():
            term = frag.termination(t_scen)
            if term.term_node is frag:
                self.terminate_to_foreground(new, scenario=t_scen)
            else:
                new.term_from_term(term, scenario=t_scen)

        for c in self.child_flows(frag):
            self.clone_fragment(c, parent=new, suffix='')
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
                                         balance=fragment._balance_flow)
        self.transfer_evs(fragment, surrogate)
        fragment.clear_evs()

        surrogate.terminate(fragment)

    def _del_fragment(self, fragment):
        for c in self.child_flows(fragment):
            self._del_fragment(c)
        self[0]._del_f(fragment)

    def del_fragment(self, fragment):
        if isinstance(fragment, str):
            fragment = self.frag(fragment)
        print('%s' % fragment)
        self._show_frag_children(fragment)

        ts = self[0].linked_terms(fragment)
        if len(ts) > 0:
            print('Links to this fragment (terminations will be replaced with None):')
            for i in ts:
                print('%s' % i._parent)
        if ifinput('Are you sure?? y/n:', 'n') == 'y':
            for i in ts:
                i.update(None)

            self._del_fragment(fragment)

    def add_child(self, frag):
        return self.create_fragment(parent=frag)

    '''
    def find_termination(self, ref, index=None, direction=None):
        if isinstance(ref, LcFragment):
            if index is None:
                index = self.current_archive or select_archive(self)
            terms = self._catalog.terminate_fragment(index, ref)
        elif isinstance(ref, Exchange):
            if index is None:
                index = self.current_archive or select_archive(self)
            terms = self._catalog.terminate_flow(self.ref(index, ref.flow), comp_dir(ref.direction))
        else:
            if direction is None:
                direction = {'i': 'Input', 'o': 'Output'}[cyoa('(I)nput or (O)utput?', 'IO').lower()]
            terms = self._catalog.terminate_flow(ref, direction)
        pick = pick_one(terms)
        print('Picked: %s' % pick)
        return pick
    '''

    def terminate_by_search(self, frag, index=None):
        print('%s' % frag)
        print('Search for termination.')
        string = ifinput('Enter search term: ', frag['Name'])
        return pick_one(self.search(index, 'p', Name=string, show=False))

    def add_termination(self, frag, term, scenario=None, background_children=True):
        if isinstance(term, ExchangeRef):
            print('Terminating from exchange\n')
            frag.term_from_exch(term, scenario=scenario)
        else:
            print('Terminating from process ref\n')
            frag.terminate(term, scenario=scenario)
        self.build_child_flows(frag, background_children=background_children)

    def auto_terminate(self, frag, index=None, scenario=None, background_children=True):
        if scenario is None and not frag.term.is_null:
            return  # nothing to do-- (ecoinvent) already terminated by exchange
        ex = pick_one(self.find_termination(frag, index=index))
        if ex is None:
            ex = self.terminate_by_search(frag, index=index)
        if ex is not None:
            self.add_termination(frag, ex, scenario=scenario, background_children=background_children)
        else:
            print('Not terminated.')

    def foreground(self, frag, index=None):
        self.fragment_to_foreground(frag)
        self.auto_terminate(frag, index=index)

    def curate_stages(self, frag, stage_names=None):
        def _recurse_stages(f):
            stages = [f['StageName']]
            for m in self.child_flows(f):
                stages.extend(_recurse_stages(m))
            return stages

        if stage_names is None:
            stage_names = set(_recurse_stages(frag))

        print("Select stage for %s \n(or enter '' to quit)" % frag)
        ch = pick_one_or(sorted(stage_names), default=frag['StageName'])
        if ch == '':
            return
        if ch not in stage_names:
            stage_names.add(ch)
        frag['StageName'] = ch
        for c in self.child_flows(frag):
            self.curate_stages(c, stage_names=stage_names)

    def background_scenario(self, scenario, index=None):
        if index is None:
            index = self.current_archive or select_archive(self)
        for bg in self[0].fragments(background=True):
            print('\nfragment %s' % bg)
            if scenario in bg.terminations():
                print('Terminated to %s' % bg.termination(scenario).term_node)
            ch = cyoa('(K)eep current or (S)earch for termination? ', 'ks', 'k')
            if ch == 's':
                p_ref = self.terminate_by_search(bg, index=index)
                if p_ref is not None:
                    bg.terminate(p_ref, scenario=scenario)

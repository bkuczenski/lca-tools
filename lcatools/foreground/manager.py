import json
import os
import re

from collections import defaultdict

from lcatools.foreground.foreground import ForegroundArchive
from lcatools.catalog import CatalogInterface, ExchangeRef, CatalogRef
from lcatools.exchanges import comp_dir, Exchange, ExchangeValue
from lcatools.entities import LcQuantity, LcFlow
from lcatools.flowdb.flowdb import FlowDB
from lcatools.lcia_results import LciaResult, LciaResults, InconsistentQuantity
from lcatools.foreground.dynamic_grid import dynamic_grid
from lcatools.interact import pick_reference, ifinput, pick_one, pick_compartment, parse_math

MANIFEST = ('catalog.json', 'entities.json', 'fragments.json', 'flows.json')


class NoLoadedArchives(Exception):
    pass


class AmbiguousReference(Exception):
    pass


class AmbiguousTermination(Exception):
    pass


class NoTermination(Exception):
    pass


class BackgroundError(Exception):
    pass


class ForegroundManager(object):
    """
    This class is used for building LCA models based on catalog refs.

    It consists of:

     * a catalog containing inventory and LCIA data
     * a Flow-Quantity database

    It manages:
     - adding and loading archives to the catalog
     - searching the catalog

    It maintains:
     - a result set generated from search
     - a select set for comparisons

    Fragments also get tacked on here for now

    The interface subclass provides UI for these activities
    """
    def __init__(self, fg_dir, cat_file=None, cfs=None, force_create_new=False):

        import time
        t0 = time.time()
        if fg_dir is None:
            self._catalog = CatalogInterface.new()
            self.db = FlowDB()
            print('Setup Catalog and FlowDB... (%.2f s)' % (time.time() - t0))

        else:
            self._ensure_foreground(fg_dir, force_create_new=force_create_new)
            if cat_file is None:
                cat_file = os.path.join(fg_dir, 'catalog.json')
            if os.path.exists(cat_file):
                self._catalog = CatalogInterface.new(fg_dir=fg_dir, catalog_file=cat_file)
            else:
                self._catalog = CatalogInterface.new(fg_dir=fg_dir)
            self._catalog.load(0)
            if os.path.exists(self[0].compartment_file):
                self.db = FlowDB(compartments=self[0].compartment_file)
            else:
                self.db = FlowDB()
            print('Setup Catalog and FlowDB... (%.2f s)' % (time.time() - t0))

            self[0].load_fragments(self._catalog)
            print('Fragments loaded... (%.2f s)' % (time.time() - t0))

        self._cfs = []
        self.unmatched_flows = defaultdict(set)
        if cfs is not None:
            print('Loading LCIA data... (%.2f s)' % (time.time() - t0))
            for c in cfs:
                self.load_lcia_cfs(c)
                print('finished %s... (%.2f s)' % (c, time.time() - t0))

        self.compute_unit_scores()
        print('finished... (%.2f s)' % (time.time() - t0))

    def load_lcia_cfs(self, nick, quantity=None):
        if self._catalog[nick] is None:
            self._catalog.load(nick)
        self.merge_compartments(nick)
        if quantity is None:
            self.unmatched_flows[nick] |= self.db.import_archive_cfs(self._catalog[nick])
        else:
            if not isinstance(quantity, LcQuantity):
                quantity = quantity.entity()  # assume catalog ref
            self.unmatched_flows[nick] |= self.db.import_quantity(self._catalog[nick], quantity)
        print('%d unmatched flows found from source %s... \n' %
              (len(self.unmatched_flows[nick]), self._catalog.name(nick)))
        self._cfs.append(nick)

    def show(self, loaded=True):
        if loaded:
            n = self._catalog.show_loaded()
            if n == 0:
                raise NoLoadedArchives('No archives loaded!')
        else:
            self._catalog.show()

    def show_all(self):
        self.show(loaded=False)

    def load(self, item):
        self._catalog.load(item)

    def add_archive(self, *args, **kwargs):
        self._catalog.add_archive(*args, **kwargs)

    def save(self):
        self._catalog.save_foreground()

    @property
    def flows(self):
        for f in sorted(self[0].flows(), key=lambda x: x['Name']):
            print('%s' % f)

    def __getitem__(self, item):
        return self._catalog.__getitem__(item)

    def search(self, *args, show=True, **kwargs):
        return self._catalog.search(*args, show=show, **kwargs)

    def find_termination(self, ref, *args, index=None, **kwargs):
        if isinstance(ref, CatalogRef):
            if ref.entity_type == 'flow':
                return self._catalog.terminate_flow(ref, *args, **kwargs)
            else:
                print("don't know how to terminate %s" % ref)
        elif isinstance(ref, ExchangeRef):
            return self._catalog.terminate(ref, **kwargs)
        else:
            if index is None:
                print('Please specify an index.')
                return
            index = self._catalog.get_index(index)
            if isinstance(ref, Exchange):
                return self._catalog.terminate(ExchangeRef(self._catalog, index, ref), **kwargs)
            elif isinstance(ref, LcFlow):
                return self._catalog.terminate_flow(self.ref(index, ref), *args, **kwargs)
            elif ref.entity_type == 'fragment':
                return self._catalog.terminate_fragment(index, ref, **kwargs)

    def ref(self, *args):
        return self._catalog.ref(*args)

    @staticmethod
    def _ensure_foreground(folder, force_create_new=False):
        if not os.path.exists(folder):
            os.makedirs(folder)
        if force_create_new or not os.path.exists(os.path.join(folder, 'entities.json')):
            ForegroundArchive.new(folder)

    def _add_local_synonym(self, existing, new):
        syns = self._load_local_synonyms()
        syns[existing].append(new)
        self.db.flowables.add_synonym(existing, new)

    def _install_local_synonyms(self, syns):
        for k, v in syns:
            for val in v:
                self.db.flowables.add_synonym(k, val)

    def _load_local_synonyms(self):
        if os.path.exists(self[0].synonyms_file):
            with open(self[0].synonyms_file, 'r') as fp:
                syns = json.load(fp)
            return syns
        return defaultdict(list)

    def _save_local_synonyms(self, syns):
        with open(self[0].synonyms_file, 'w') as fp:
            json.dump(syns, fp)

    def workon(self, folder, force_create_new=False):
        """
        Select the current foreground.  Create folder if needed.
        If folder/entities.json does not exist, creates and saves a new foreground in folder.
        loads and installs archive from folder/entities.json
        :param folder:
        :param force_create_new: [False] if True, overwrite existing entities and fragments with a new
         foreground from the template.
        :return:
        """
        self._ensure_foreground(folder, force_create_new=force_create_new)
        self._catalog.reset_foreground(folder)
        self._catalog.load(0)
        if os.path.exists(self[0].catalog_file):
            self._catalog.open(self[0].catalog_file)
        if os.path.exists(self[0].compartment_file):
            self.db.load_compartments(self[0].compartment_file)

        self[0].load_fragments(self._catalog)
        self._load_local_synonyms()

        self.clear_unit_scores()
        self.compute_unit_scores()

    def add_to_foreground(self, ref):
        print('Add to foreground: %s' % ref)
        if isinstance(ref, CatalogRef):
            ref = ref.entity()
        if isinstance(ref, list):
            for k in ref:
                self.add_to_foreground(k)
        else:
            self._catalog[0].add_entity_and_children(ref)

    def merge_compartments(self, item):
        if self._catalog.is_loaded(0):
            self.db.save_compartments(self[0].compartment_file)
        index = self._catalog.get_index(item)
        for f in self[index].flows():
            if self._catalog.is_loaded(0):
                self.db.find_matching_compartment(f['Compartment'], interact=True)

    def find_flowable(self, string):
        return self.db.flowables.search(string)

    def add_synonym(self, name, flow):
        """
        Set a flow UUID to be a synonym with an entry in the flow db.
        :param name:
        :param flow:
        :return:
        """
        self._add_local_synonym(name, flow.get_uuid())

    def parse_flow(self, flow):
        return self.db.parse_flow(flow)

    def cfs_for_flowable(self, string, **kwargs):
        flowables = self.find_flowable(string)
        if len(flowables) > 1:
            for f in flowables:
                print('%6d %s [%s]' % (len(self.db.all_cfs(f, **kwargs)), self.db.flowables.name(f),
                                       self.db.flowables.cas(f)))
        elif len(flowables) == 1:
            f = flowables.pop()
            print('%s [%s]' % (self.db.flowables.name(f), self.db.flowables.cas(f)))
            for cf in self.db.all_cfs(f, **kwargs):
                print('%s' % cf)

    def cfs_for_flowable_grid(self, string, **kwargs):
        flowables = self.find_flowable(string)
        if len(flowables) > 1:
            for f in flowables:
                print('%6d %s [%s]' % (len([cf for cf in self.db.all_cfs(f, **kwargs)]),
                                       self.db.flowables.name(f),
                                       self.db.flowables.cas(f)))
        elif len(flowables) == 1:
            f = flowables.pop()
            self.db.cfs_for_flowable(f, **kwargs)

    # inspection methods
    def gen_exchanges(self, process_ref, ref_flow, direction):
        """
        This method takes in an exchange definition and gets all the complementary exchanges (i.e. the
        process_ref's intermediate exchanges, excluding the reference flow

        :param process_ref:
        :param ref_flow:
        :param direction:
        :return: an exchange generator (NOT ExchangeRefs because I haven't figured out how to be consistent on that yet)
        """
        for x in self.db.filter_exch(process_ref, elem=False, ref_flow=ref_flow):
            if not (x.flow == ref_flow and x.direction == direction):
                yield x

    def gen_elem(self, process_ref, ref_flow):
        for x in self.db.filter_exch(process_ref, elem=True, ref_flow=ref_flow):
            yield x

    def inventory(self, node, **kwargs):
        if node.entity_type == 'fragment':
            print('%s' % node)
            for x in sorted(self.get_fragment_inventory(node, **kwargs), key=lambda z: (z.direction, z.value)):
                print(x)
        else:
            print('%s' % node.fg())
            node.fg().inventory()

    def intermediate(self, process_ref, **kwargs):
        exch = self.db.filter_exch(process_ref, elem=False, **kwargs)
        if len(exch) == 0:
            print('No intermediate exchanges')
            return
        print('Intermediate exchanges:')
        for i in exch:
            print('%s' % i)

    def elementary(self, process_ref, **kwargs):
        exch = self.db.filter_exch(process_ref, elem=True, **kwargs)
        if len(exch) == 0:
            print('No elementary exchanges')
            return
        print('Elementary exchanges:')
        for i in exch:
            print('%s' % i)

    def compare_inventory(self, p_refs, elementary=False, **kwargs):
        def _key(exc):
            return ('%s [%s]' % (exc.flow['Name'], exc.flow.reference_entity.reference_entity.unitstring()),
                    exc.direction, exc.flow)
        ints = dict()
        int_set = set()
        elems = dict()
        elem_set = set()

        for p in p_refs:
            ints[p] = self.db.filter_exch(p, elem=False, **kwargs)
            int_set = int_set.union(_key(x) for x in ints[p])
            if elementary:
                elems[p] = self.db.filter_exch(p, elem=True, **kwargs)
                elem_set = elem_set.union(_key(x) for x in elems[p])

        int_rows = sorted(int_set, key=lambda x: x[1])
        int_flows = [x[2] for x in int_rows]

        dynamic_grid(p_refs, int_rows,
                     lambda x, y: {t for t in ints[y] if _key(t) == x},
                     ('Direction', lambda x: x[1]),
                     ('Flow', lambda x: x[0]),
                     returns_sets=True, suppress_col_list=elementary)

        if elementary:
            ele_rows = sorted(elem_set, key=lambda x: x[1])
            ele_flows = [x[2] for x in ele_rows]

            dynamic_grid(p_refs, ele_rows, lambda x, y: {t for t in elems[y] if _key(t) == x},
                         ('Direction', lambda x: x[1]),
                         ('Flow', lambda x: x[0]), returns_sets=True)
            return int_flows, ele_flows
        return int_flows

    def compare_allocation(self, p_ref):
        def _key(exc):
            return '%s' % exc.flow, exc.direction

        ints = dict()
        elems = dict()
        int_set = set()
        elem_set = set()
        cols = []
        for p in p_ref.entity().reference_entity:
            cols.append(p)
            ints[p] = self.db.filter_exch(p_ref, elem=False, ref_flow=p.flow)
            int_set = int_set.union(_key(x) for x in ints[p])
            elems[p] = self.db.filter_exch(p_ref, elem=True, ref_flow=p.flow)
            elem_set = elem_set.union(_key(x) for x in elems[p])

        int_rows = sorted(int_set, key=lambda x: x[1])

        dynamic_grid(cols, int_rows,
                     lambda x, y: {t for t in ints[y] if _key(t) == x},
                     ('Direction', lambda x: x[1]),
                     ('Flow', lambda x: x[0]), returns_sets=True, suppress_col_list=True)
        ele_rows = sorted(elem_set, key=lambda x: x[1])

        dynamic_grid(cols, ele_rows, lambda x, y: {t for t in elems[y] if _key(t) == x},
                     ('Direction', lambda x: x[1]),
                     ('Flow', lambda x: x[0]), returns_sets=True)

    @property
    def lcia_methods(self):
        return [q for q in self[0].lcia_methods()]

    @property
    def flow_properties(self):
        ls = set([q.get_uuid() for q in self.lcia_methods])
        return [q for q in self[0].quantities() if q.get_uuid() not in ls]

    def _rename_stage(self, frag, old, new):
        if frag['StageName'] == old:
            frag['StageName'] = new
        for x in self.child_flows(frag):
            self._rename_stage(x, old, new)

    def rename_stage(self, old_name, new_name, fragment=None):
        """
        Change the name of a stage everywhere it appears. By default, searches through all foreground fragments.
        you may also specify a single fragment, in which case it will apply only to that fragment and its descendents.
        :param old_name:
        :param new_name:
        :param fragment:
        :return:
        """
        if fragment is not None:
            fragments = [fragment]
        else:
            fragments = self[0].fragments(background=False)
        for f in fragments:
            self._rename_stage(f, old_name, new_name)

    def _prep_quantities(self, quantities):
        if quantities is None:
            qs = self._catalog[0].lcia_methods()
            if len(qs) == 0:
                print('No foreground LCIA methods')
                return None
            return qs
        if not (isinstance(quantities, list) or isinstance(quantities, tuple)):
            quantities = [quantities]
        out = []
        for k in quantities:
            if isinstance(k, CatalogRef):
                out.append(k.entity())
            else:
                out.append(k)
        return out

    def fg_lcia(self, process_ref, quantities=None, dist=3, scenario=None, **kwargs):
        """
        :param process_ref:
        :param quantities: defaults to foreground lcia quantities
        :param dist: [1] how far afield to search for cfs (see CLookup.find() from flowdb)
        :param scenario: (not presently used) - some day the flow-quantity database will be scenario-sensitive
        :return:
        """
        if self._catalog.fg is None:
            print('Missing a foreground!')
            return None
        if not self._catalog.is_loaded(0):
            self._catalog.load(0)
        if not self._catalog.is_loaded(process_ref.index):
            self._catalog.load(process_ref.index)
        exch = self.db.filter_exch(process_ref, elem=True, **kwargs)
        qs = self._prep_quantities(quantities)
        results = LciaResults(process_ref.entity())
        for q in qs:
            q_result = LciaResult(q)
            if q in self.db.known_quantities():
                for x in exch:
                    if not x.flow.has_characterization(q):
                        cf = self.db.lookup_single_cf(x.flow, q, dist=dist, location=process_ref['SpatialScope'])
                        if cf is None:
                            x.flow.add_characterization(q)
                        else:
                            x.flow.add_characterization(cf)
                    fac = x.flow.factor(q)
                    q_result.add_score(process_ref.id, x, fac, process_ref['SpatialScope'])
            results[q.get_uuid()] = q_result
        return results

    def bg_lcia(self, p_ref, quantities=None, **kwargs):
        qs = self._prep_quantities(quantities)
        if qs is None:
            return dict()

        if p_ref is None:
            # cutoff
            result = LciaResults(p_ref.entity())
            for q in quantities:
                result[q.get_uuid()] = LciaResult(q)
            return result
        return p_ref.archive.bg_lookup(p_ref.id, quantities=qs, flowdb=self.db, **kwargs)

    def compare_lcia_results(self, p_refs, quantities=None, background=False, **kwargs):
        """
        p_refs should be an array of catalog_refs
        :param p_refs:
        :param quantities: list of qs
        :param background: whether to use bg_lcia instead of fg_lcia
        :return:
        """
        results = dict()
        qs = self._prep_quantities(quantities)
        for p in p_refs:
            if background:
                results[p] = self.bg_lcia(p, quantities=quantities, **kwargs)
            else:
                results[p] = self.fg_lcia(p, quantities=quantities, **kwargs)

        dynamic_grid(p_refs, qs, lambda x, y: results[y][x.get_uuid()],
                     ('Ref Units', lambda x: x.reference_entity),
                     ('LCIA Method', lambda x: x['Name']))

    def lcia(self, p_ref):
        self.compare_lcia_results([p_ref])

    def show_detailed_lcia(self, p_ref, quantity, show_all=False):
        """

        :param p_ref:
        :param quantity:
        :param show_all: [False] show all exchanges, or only characterized exchanges
        :return:
        """
        result = self.fg_lcia(p_ref, quantity)[quantity.get_uuid()]
        result.show_details(p_ref, show_all=show_all)
        '''
        print('%s' % quantity)
        print('-' * 60)
        agg_lcia = result.LciaScores[p_ref.get_uuid()]
        for x in sorted(agg_lcia.LciaDetails, key=lambda t: t.result):
            if x.result != 0 or show_all:
                print('%10.3g x %-10.3g = %-10.3g %s' % (x.exchange.value, x.factor.value, x.result, x.factor.flow))
        print('=' * 60)
        print('             Total score: %g [%s]' % (agg_lcia.cumulative_result,
                                                     quantity.reference_entity.unitstring()))
        '''

    # fragment methods
    def _show_frag_children(self, frag, level=0):
        level += 1
        for k in self.child_flows(frag):
            print('%s%s' % ('  ' * level, k))
            self._show_frag_children(k, level)

    def fragments(self, *args, show_all=False, background=False, **kwargs):
        """

        :param background:
        :param show_all:
        :return:
        """
        for f in self[0].fragments(show_all=False, background=background, **kwargs):
            if len(args) != 0:
                if not bool(re.search(args[0], str(f), flags=re.IGNORECASE)):
                    continue
            print('%s' % f)
            if show_all:
                self._show_frag_children(f)

    def background(self, *args):
        self.fragments(*args, background=True)

    def frag(self, string, strict=True):
        """
        strict=True is slow
        :param string:
        :param strict:
        :return:
        """
        if strict:
            k = [f for f in self[0].fragments(show_all=True) if f.get_uuid().startswith(string.lower())]
            if len(k) > 1:
                for i in k:
                    print('%s' % i)
                raise AmbiguousReference()
            return k[0]
        else:
            return next(f for f in self[0].fragments(show_all=True) if f.get_uuid().startswith(string.lower()))

    def traverse(self, fragment, scenario=None, observed=False):
        if isinstance(fragment, str):
            fragment = self.frag(fragment)
        ffs = fragment.traversal_entry(lambda x: self.child_flows(x), scenario, observed=observed)
        return ffs

    def fragment_lcia(self, fragment, scenario=None, observed=False, scale=None):
        if isinstance(fragment, str):
            fragment = self.frag(fragment)
        r = fragment.fragment_lcia(lambda x: self.child_flows(x), scenario=scenario, observed=observed)
        if scale is not None:
            r.scale(scale)
        return r

    def draw(self, fragment, scenario=None, observed=False):
        if isinstance(fragment, str):
            fragment = self.frag(fragment)
        fs = fragment.show_tree(lambda x: self.child_flows(x), scenario=scenario, observed=observed)
        self.balance(fragment, scenario=scenario, observed=observed)
        return fs

    def observe(self, fragment, scenario=None):
        if isinstance(fragment, str):
            fragment = self.frag(fragment)
        print('%s' % fragment)
        if scenario is None:
            print('{Observing for model reference}\n')
            prompt = 'Observed value'
        else:
            print('{Observing for scenario "%s"}' % scenario)
            prompt = 'Scenario value'

        for c in self.child_flows(fragment):
            print('%s' % c)
            print(' Cached EV: %6.4g\n Observed EV: %6.4g [%s]' % (c.cached_ev, c.observed_ev, c.flow.unit()))
            if scenario is None:
                string_ev = '%10g' % c.observed_ev
            else:
                string_ev = '%10g' % c.exchange_value(scenario)
                print(' Scenario EV: %s [%s]' % (string_ev,
                                                 c.flow.unit()))
            val = ifinput('%s ("=" to use cached): ' % prompt, string_ev)
            if val != string_ev:
                if val == '=':
                    new_val = c.cached_ev
                else:
                    new_val = parse_math(val)
                if scenario is None:
                    c.observed_ev = new_val
                else:
                    c.set_exchange_value(scenario, new_val)

    def child_flows(self, fragment):
        """
        This is a lambda method used during traversal in order to generate the child fragment flows from
        a given fragment.
        :param fragment:
        :return: fragments listing fragment as parent
        """
        for x in self[0].fragments(show_all=True):
            if fragment is x.reference_entity:
                yield x

    def scenarios(self, fragment=None, _scens=None):
        if fragment is None:
            scens = set()
            for i in self[0].fragments(show_all=True):
                scens = scens.union(self.scenarios(i))
            return scens

        if _scens is None:
            _scens = set()
        _scens = _scens.union(fragment.scenarios)
        for c in self.child_flows(fragment):
            _scens = self.scenarios(c, _scens=_scens)
        return _scens

    def show_scenario(self, scenario):
        print('Parameters for scenario "%s"' % scenario)
        for f in self[0].fragments(show_all=True, background=None):
            printed = False
            if f.exchange_value(scenario) != f.cached_ev:
                if printed is False:
                    printed = True
                    print('%s' % f)
                print(' Exchange value: %10.4g (default %10.4g)' % (f.exchange_value(scenario), f.cached_ev))
            if f.termination(scenario) != f.term:
                if printed is False:
                    print('%s' %f)
                print('%15s: %s\n%15s: %s' % ('Termination', f.termination(scenario).term_node.entity()['Name'],
                                              'Default', f.term.term_node.entity()['Name']))

    '''
    def auto_terminate(self, index, fragment, scenario=None, use_first=False):
        """

        :param index:
        :param fragment:
        :param scenario:
        :param use_first: [False] if True, resolve AmbiguousTerminations by using the first result
        :return:
        """
        term_exch = self._catalog.terminate_fragment(index, fragment)
        if len(term_exch) > 1 and not use_first:
            raise AmbiguousTermination('%d found' % len(term_exch))
        elif len(term_exch) == 0:
            raise NoTermination
        term_exch = term_exch[0]
        try:
            bg = next(f for f in self[0].fragments(background=True) if f.term.matches(term_exch))
            fragment.terminate(bg, scenario=scenario)
        except StopIteration:
            fragment.term_from_exch(term_exch, scenario=scenario)
            self.build_child_flows(fragment, scenario=scenario)
    '''

    def import_fragment(self, filename):
        if os.path.isdir(filename):
            print('Loading fragment files in directory %s' % filename)
            self[0].import_fragments(self._catalog, filename)
        else:
            self[0].import_fragment(self._catalog, filename)

    def terminate_to_foreground(self, fragment, scenario=None):
        """
        Marks a fragment as its own termination. Note: this creates unresolved issues with flow matching across
        scenarios. but those have always been there and never been solved.
        If the termination already exists, then the *elementary* flows are made into child flows (assuming the
        intermediate flows were already done).
        If the fragment was terminated to a sub-fragment, nothing happens.. the subfragment link is just deleted.
        any i/o flows developed from the subfragment remain, but their exchange values will be used as-specified.
        :param fragment:
        :return:
        """
        if isinstance(fragment, str):
            fragment = self.frag(fragment)
        term = fragment.termination(scenario)
        children = []
        if term.is_fg:
            return children
        if (not term.is_null) and term.term_node.entity_type == 'process':
            fragment.scale_evs(1.0 / term.inbound_exchange_value)
            for elem in self.gen_elem(term.term_node, term.term_flow):
                child = self[0].add_child_ff_from_exchange(fragment, elem, Name=str(fragment.flow),
                                                           StageName='direct emission')
                child.term.self_terminate()
                children.append(child)
        term.self_terminate()
        return children

    def create_fragment_from_process(self, process_ref, ref_flow=None, background=False, background_children=True):
        """
        The major entry into fragment building.  Given only a process ref, construct a fragment from the process,
        using the process's reference exchange as the reference fragment flow.
        :param process_ref:
        :param ref_flow:
        :param background: [False] add as a background fragment and do not traverse children
        :param background_children: [True] automatically terminate child flows with background references (ecoinvent).
        :return:
        """
        process = process_ref.fg()
        if ref_flow is None:
            if len(process.reference_entity) == 0:
                ref = pick_one([x for x in self.db.filter_exch(process_ref, elem=False) if x.direction == 'Output']
                               ).flow
            elif len(process.reference_entity) > 1:
                ref = pick_reference(process)
            else:
                ref = list(process.reference_entity)[0].flow
        else:
            try:
                ref = next(x.flow for x in process.reference_entity if x.flow.match(ref_flow))
            except StopIteration:
                print('Reference flow not found in target process.')
                return None
        ref_exch = next(x for x in process.exchange(ref))
        direction = comp_dir(ref_exch.direction)
        frag = self[0].create_fragment(ref, direction, Name='%s' % process_ref.entity(), background=background)
        frag.terminate(process_ref, flow=ref)
        if not background:
            self.build_child_flows(frag, background_children=background_children)
        return frag

    def get_fragment_inventory(self, fragment, scenario=None, scale=None, observed=False):
        """
        Aggregates inputs and outputs (un-terminated flows) from a fragment; returns a list of exchanges.
        :param fragment: ff
        :param scenario:
        :return:
        """
        io_ffs = fragment.io_flows(lambda x: self.child_flows(x), scenario, observed=observed)
        if scale is not None:
            for i in io_ffs:
                i.scale(scale)

        accum = defaultdict(float)
        ent = dict()
        ev = fragment.exchange_value(scenario)
        if fragment.direction == 'Input':  # this is input to parent flow, so output to us
            ev = -ev
        accum[fragment.flow.get_uuid()] = ev
        for i in io_ffs:
            ent[i.fragment.flow.get_uuid()] = i.fragment.flow
            if i.fragment.direction == 'Input':
                accum[i.fragment.flow.get_uuid()] += i.magnitude
            else:
                accum[i.fragment.flow.get_uuid()] -= i.magnitude

        in_ex = accum.pop(fragment.flow.get_uuid())
        if in_ex * ev < 0:  # i.e. if the signs are different
            raise ValueError('Fragment requires more reference flow than it generates')
        frag_exchs = []
        for k, v in accum.items():
            val = abs(v)
            if fragment.reference_entity is None:
                if ev != in_ex:
                    val *= (ev / in_ex)
            if v == 0:
                continue
            elif v < 0:
                dirn = 'Output'
            else:
                dirn = 'Input'
            frag_exchs.append(ExchangeValue(fragment, ent[k], dirn, value=val))
        return sorted(frag_exchs, key=lambda x: x.direction)

    def build_child_flows(self, fragment, scenario=None, background_children=False):
        """
        Given a terminated fragment, construct child flows corresponding to the termination's complementary
        exchanges.

        :param fragment: the parent fragment
        :param scenario:
        :param background_children: if true, automatically terminate child flows to background.
        :return:
        """
        if fragment.is_background:
            return None  # no child flows for background nodes
        term = fragment.termination(scenario=scenario)
        if term.is_null or term.is_fg:
            return None

        if term.term_node.entity_type == 'process':
            if scenario is not None:
                print('Automatic building of child flows is not supported for scenario terminations of process nodes.')
                return []
            if len([c for c in self.child_flows(fragment)]) != 0:
                print('Warning: fragment already has child flows. Continuing will result in (possibly many) ')
                if ifinput('duplicate child flows.  Continue? y/n', 'n') != 'y':
                    return []

            int_exch = self.gen_exchanges(term.term_node, term.term_flow, term.direction)
            # in process case- possible to specify multiple identical children from the same node
            """
             this means that build_child_flows will have undesirable results if called for
             a process- terminated node with similar exchanges for multiple scenarios.
             maybe changing node terminations should only be permitted for subfragments. something to
             think about.
            """

        elif term.term_node.entity_type == 'fragment':

            int_exch = self.get_fragment_inventory(term.term_node, scenario=scenario)
            # in subfragment case- child flows aggregate so we don't want to create duplicate children
            for x in int_exch:
                match = [c for c in self.child_flows(fragment) if c.flow == x.flow and c.direction == x.direction]
                if len(match) > 1:
                    raise AmbiguousReference('Multiple child flows matching %s' % x)
                elif len(match) == 1:
                    int_exch.remove(x)  # don't make a new child if one is already found

        else:
            raise AmbiguousTermination('Cannot figure out entity type for %s' % term)

        children = []
        for exch in int_exch:
            child = self[0].add_child_ff_from_exchange(fragment, exch)
            if background_children:
                self.terminate_to_background(child)
            children.append(child)
        return children

    def clear_unit_scores(self, scenario=None):
        for f in self[0].fragments(show_all=True):
            if scenario is None:
                for term in f.terminations():
                    f.termination(term).clear_score_cache()
            else:
                if scenario in f.terminations():
                    f.termination(scenario).clear_score_cache()

    def compute_unit_scores(self, scenario=None):
        for f in self[0].fragments(show_all=True):
            self.compute_fragment_unit_scores(f, scenario=scenario)

    '''
    if self.db.is_elementary(fragment.flow):
        cfs = self.db.factors_for_flow(fragment.term.term_flow, [l for l in self[0].lcia_methods()])
        fragment.term.flowdb_results(LciaResult.from_cfs(fragment, cfs))
    '''

    def compute_fragment_unit_scores(self, fragment, scenario=None):
        """
        lcia gets run as: lcia(self.term_node, self.term_flow, q_run)
        for background or default: x = fragment
        :param fragment:
        :param scenario:
        :return:
        """
        term = fragment.termination(scenario)
        l_methods = [q for q in self[0].lcia_methods()]
        if fragment.is_background:
            def lcia(x, y, z):
                return self.bg_lcia(x, ref_flow=y, quantities=z)
        elif term.is_fg:
            def lcia(x, y, z):
                return LciaResult.from_cfs(x, self.db.factors_for_flow(y, z))
        else:
            def lcia(x, y, z):
                return self.fg_lcia(x, ref_flow=y, quantities=z, scenario=scenario)
        term.set_score_cache(lcia, l_methods)

    def terminate_to_background(self, fragment):
        """
        Given an existing fragment, create (or locate) a background reference that terminates it. If the fragment is
        terminated, transfer the termination to the background reference.
        :param fragment:
        :return:
        """
        if fragment.term.is_bg:
            return fragment  # nothing to do
        else:
            if fragment.term.is_null:
                frag_exch = Exchange(fragment, fragment.flow, fragment.direction)
                try:
                    bg = next(f for f in self[0].fragments(background=False) if f.term.terminates(frag_exch))
                    print('found termination in foreground')
                    fragment.terminate(bg)
                except StopIteration:
                    try:
                        bg = next(f for f in self[0].fragments(background=True) if f.term.terminates(frag_exch))
                        print('found cutoff flow')
                        fragment.terminate(bg)
                    except StopIteration:
                        bg = self[0].add_background_ff_from_fragment(fragment)
                        print('created null termination in background')
                        fragment.terminate(bg)
            else:
                term_exch = fragment.term.to_exchange()
                try:
                    bg = next(f for f in self[0].fragments(background=False) if f.term.matches(term_exch))
                    print('found termination match in foreground')
                    fragment.terminate(bg)
                except StopIteration:
                    try:
                        bg = next(f for f in self[0].fragments(background=True) if f.term.matches(term_exch))
                        print('found termination match in background')
                        fragment.terminate(bg)
                    except StopIteration:
                        bg = self[0].add_background_ff_from_fragment(fragment)
                        print('created termination in background')
                        fragment.terminate(bg)
        return bg

    def fragment_to_foreground(self, fragment, background_children=True):
        """
        Move a background fragment into the foreground. Add the node's child flows to the foreground.

        Given a fragment that is terminated to background, recall the background termination and make it into a
          foreground termination.  (the background reference will not be deleted).  Proceed to add the foreground
          node's child flows.
        :param fragment:
        :param background_children:
        :return:
        """
        if fragment.is_background:
            fragment.to_foreground()
            self.build_child_flows(fragment, background_children=background_children)
            return fragment
        elif fragment.term.is_bg:
            bg = fragment.term.term_node
            fragment.terminate(bg.term.term_node, flow=bg.term.term_flow, direction=bg.term.direction)
            self.build_child_flows(fragment, background_children=background_children)
            return fragment
        elif fragment.term.is_null:
            fragment.term.self_terminate()
        return fragment  # nothing to do

    def balance(self, frag, scenario=None, observed=False):
        qs = defaultdict(float)
        if frag.reference_entity is None:
            in_ex = frag.exchange_value(scenario, observed=observed)
        else:
            in_ex = 1.0
        for cf in frag.flow.characterizations():
            if cf.value is not None:
                if frag.direction == 'Input':  # output from term
                    qs[cf.quantity] -= cf.value * in_ex
                else:
                    qs[cf.quantity] += cf.value * in_ex
        for c in self.child_flows(frag):
            for cf in c.flow.characterizations():
                mag = c.exchange_value(scenario, observed=observed) * (cf.value or 0.0)
                if mag != 0:
                    if c.direction == 'Output':
                        qs[cf.quantity] -= mag
                    else:
                        qs[cf.quantity] += mag

        for k, v in qs.items():
            print('%10.4g %s' % (v, k))
        return qs

    def show_balance(self, frag, quantity=None, scenario=None, observed=False):
        def _p_line(f, m, d):
            try:
                # will fail if m is None or non-number
                print(' %+10.4g  %6s  %.5s %s' % (m, d, f.get_uuid(), f['Name']))
            finally:
                pass
        if quantity is None:
            quantity = frag.flow.reference_entity

        print('%s' % quantity)
        mag = frag.flow.cf(quantity)
        if frag.reference_entity is None:
            mag *= frag.exchange_value(scenario, observed=observed)
        if frag.direction == 'Input':
            mag *= -1

        net = mag

        _p_line(frag, mag, comp_dir(frag.direction))

        for c in sorted(self.child_flows(frag), key=lambda x: x.direction):
            mag = c.exchange_value(scenario, observed=observed) * c.flow.cf(quantity)
            if c.direction == 'Output':
                mag *= -1
            if mag is None or mag != 0:
                _p_line(c, mag, c.direction)
            net += mag

        print('----------\n %+10.4g net' % net)

    @staticmethod
    def profile(flow):
        flow.profile()


'''
class OldForegroundManager(object):
    """
    The foreground manager is the one-liner that you load to start building and editing LCI foreground models.

    It consists of:
     * a catalog of LcArchives, of which the 0th one is a ForegroundArchive to store fragments;

     * a database of flows, which functions as a FlowQuantity interface - tracks quantities, flowables, compartments

    A foreground is constructed from scratch by giving a directory specification. The directory is used for
    serializing the foreground; the same serialization can be used to invoke an Antelope instance.

    The directory contains:
      - entities.json: the foreground archive
      - fragments.json: serialized FragmentFlows
      - catalog.json: a serialization of the catalog (optional - not necessary if it uses only reference data)

    The foreground manager directs the serialization process and writes the files, but the components serialize
    and de-serialize themselves.
    """

    def save(self):
        self._catalog['FG'].save()
        self._save(self._catalog.serialize(), 'catalog.json')
        self._save(self._flowdb.serialize(), 'flows.json')

    def _create_new_catalog(self):
        pass

    def _exists(self, item):
        return os.path.exists(os.path.join(self._folder, item))

    def _load(self, item):
        with open(os.path.join(self._folder, item)) as fp:
            return json.load(fp)

    def _save(self, j, item):
        with open(os.path.join(self._folder, item), 'w') as fp:
            json.dump(j, fp, indent=2)

    def _check_entity_files(self):
        """
        This function ensures that entities.json and fragments.json exist-- if entities does not exist,
        creates and serializes a new foreground archive.
        If fragments does not exist, asks the foreground archive to create it.
        :return:
        """
        if self._exists('entities.json'):
            if self._exists('fragments.json'):
                return
            a = ForegroundArchive(self._folder, None)
            a.save_fragments()
            return
        ForegroundArchive.new(self._folder)

    def _create_or_load_catalog(self):
        if self._exists('catalog.json'):
            catalog = CatalogInterface.from_json(self._load('catalog.json'))
        else:
            catalog = CatalogInterface()
            catalog.load_archive(self._folder, 'ForegroundArchive', nick='FG')
        return catalog

    def _create_or_load_flowdb(self):
        if self._exists('flows.json'):
            flowdb = LcFlows.from_json(self._catalog, self._load('flows.json'))
        else:
            flowdb = LcFlows()
            for q in self.fg.quantities():
                flowdb.add_quantity(CatalogRef(self._catalog, 0, q.get_uuid()))
            for f in self.fg.flows():
                flowdb.add_flow(CatalogRef(self._catalog, 0, f.get_uuid()))
        return flowdb

    @property
    def fg(self):
        return self._catalog['FG']

    @property
    def db(self):
        return self._flowdb

    def __init__(self, folder):
        """

        :param folder: directory to store the foreground.
        """
        if not os.path.isdir(folder):
            try:
                os.makedirs(folder)
            except PermissionError:
                print('Permission denied.')

        if not os.path.isdir(folder):
            raise EnvironmentError('Must provide a working directory.')

        self._folder = folder
        self._check_entity_files()
        self._catalog = self._create_or_load_catalog()
        self._catalog.show()
        self._flowdb = self._create_or_load_flowdb()
        self.save()

    def __getitem__(self, item):
        return self._catalog.__getitem__(item)

    def show(self, loaded=True):
        if loaded:
            self._catalog.show_loaded()
        else:
            self._catalog.show()

    def _add_entity(self, index, entity):
        if self._catalog[index][entity.get_uuid()] is None:
            self._catalog[index].add(entity)
        c_r = self._catalog.ref(index, entity)
        if c_r.entity() is not None:
            return c_r
        return None

    def cat(self, i):
        """
        return self._catalog[i]
        :param i:
        :return:
        """
        return self._catalog[i]

    def search(self, *args, **kwargs):
        return self._catalog.search(*args, **kwargs)

    def terminate(self, *args, **kwargs):
        if len(args) == 1:
            ref = args[0]
            return self._catalog.terminate(ref.index, ref, **kwargs)
        else:
            return self._catalog.terminate(*args, **kwargs)

    def add_catalog(self, ref, ds_type, nick=None, **kwargs):
        self._catalog.load_archive(ref, ds_type, nick=nick, **kwargs)

    def get_flow(self, flow):
        return self._flowdb.flow(flow)

    def get_quantity(self, q):
        return self._flowdb.quantity(q)

    def foreground_flow(self, cat_ref):
        if cat_ref.entity_type == 'flow':
            new_ref = self._add_entity(0, cat_ref.entity())
            self._flowdb.add_flow(cat_ref)
            self._flowdb.add_ref(cat_ref, new_ref)

    def foreground_quantity(self, cat_ref):
        if cat_ref.entity_type == 'quantity':
            new_ref = self._add_entity(0, cat_ref.entity())
            self._flowdb.add_quantity(cat_ref)
            self._flowdb.add_ref(cat_ref, new_ref)
'''

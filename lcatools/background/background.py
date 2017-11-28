"""
Tarjan's strongly connected components algorithm is recursive.  Python doesn't do well with deep recursion, so
ultimately this code will need to be implemented on a more grown-up language.  For now, however, the recursion
limit test that ships with python reported a segfault at a recursion limit exceeding 19100 -- bigger than ecoinvent!
So for the time being we are safe.
may need to use threading to go higher (see http://stackoverflow.com/questions/2917210/)
Validate recursion depth on a given system using PYTHONROOT/Tools/scripts/find_recursionlimit.py
"""
import sys  # for recursion limit
import re  # for product_flows search

import numpy as np
import scipy as sp
from scipy.sparse import csc_matrix, csr_matrix

from lcatools.background.tarjan_stack import TarjanStack
from lcatools.background.product_flow import ProductFlow, NoMatchingReference
from lcatools.background.emission import Emission


MAX_SAFE_RECURSION_LIMIT = 18000  # this should be validated using


class RepeatAdjustment(Exception):
    pass


class MatrixProto(object):
    """
    # Exchanges: parent = column; term = row;
    Value is modified to encode exchange direction: outputs must be negated at creation, inputs entered directly
    """
    def __init__(self, parent, value):
        assert isinstance(parent, ProductFlow)
        self._parent = parent
        self._value = value
        self._adjusted = False

    @property
    def parent(self):
        return self._parent

    @property
    def value(self):
        return self._value

    def adjust_val(self):
        if self._adjusted is False:
            self._value /= self.parent.inbound_ev
            self._adjusted = True
        else:
            raise RepeatAdjustment


class MatrixEntry(MatrixProto):
    def __init__(self, parent, term, value):
        assert isinstance(term, ProductFlow)
        super(MatrixEntry, self).__init__(parent, value)
        self._term = term

    @property
    def term(self):
        return self._term


class CutoffEntry(MatrixProto):
    """
    # Cutoffs: parent = column; emission = row of B includes direction information; value is entered unmodified
    """
    def __init__(self, parent, emission, value):
        assert isinstance(emission, Emission)
        super(CutoffEntry, self).__init__(parent, value)
        self._term = emission

    @property
    def emission(self):
        return self._term


class NoAllocation(Exception):
    pass


'''
def is_elementary(flow):
    """
    in future, this sholud lookup to a standalone compartment manager
    :param flow:
    :return:
    """
    comp = flow['Compartment'][0]
    if comp == 'air' or comp == 'water' or comp == 'soil' or comp == 'natural resource':
        # Ecoinvent + USLCI
        return True
    elif comp == 'resource':
        # USLCI
        return True
    return False
'''


class BackgroundEngine(object):
    """
    Class for converting a collection of linked processes into a coherent technology matrix.
    """
    def __init__(self, static_archive):
        """
        """
        self.fg = static_archive
        self._lowlinks = dict()  # dict mapping product_flow key to lowlink -- which is a key into TarjanStack.sccs

        self.tstack = TarjanStack()  # ordering of sccs

        # hold exchanges before updating component graph
        self._interior_incoming = []  # terminated entries -> added to the component graph
        self._cutoff_incoming = []  # entries with no termination -> emissions

        # _interior_incoming entries get sorted into:
        self._interior = []  # MatrixEntries whose parent (column) is background - A*
        self._foreground = []  # MatrixEntries whose parent is upstream of the background - Af + Ad
        self._bg_emission = []  # CutoffEntries whose parent is background - B*
        self._cutoff = []  # CutoffEntries whose parent is foreground - Bf

        self._surplus_coproducts = dict()  # maps surplus coproducts to their reference products

        self._product_flows = dict()  # maps product_flow.key to index-- being position in _pf_index
        self._pf_index = []  # maps index to product_flow in order added

        self._a_matrix = None  # includes only interior exchanges -- dependencies in _interior
        self._b_matrix = None  # SciPy.csc_matrix for bg only

        self._rec_limit = len([p for p in self.fg.processes()])
        if self.required_recursion_limit > MAX_SAFE_RECURSION_LIMIT:
            raise EnvironmentError('This database may require too high a recursion limit-- time to learn lisp.')

        self._emissions = dict()  # maps emission key to index
        self._ef_index = []  # maps index to emission

    @property
    def surplus_coproducts(self):
        return self._surplus_coproducts

    @property
    def fully_allocated(self):
        return len(self._surplus_coproducts) == 0

    @property
    def required_recursion_limit(self):
        return max(sys.getrecursionlimit(), self._rec_limit)

    @property
    def mdim(self):
        return len(self._emissions)

    @property
    def emissions(self):
        return self._ef_index

    def index(self, product_flow):
        return self._product_flows[product_flow.key]

    def product_flow(self, index):
        return self._pf_index[index]

    def _lowlink(self, product_flow):
        return self._lowlinks[product_flow.key]

    def _add_product_flow(self, pf):
        self._product_flows[pf.key] = pf.index
        self._set_lowlink(pf, pf.index)
        self._pf_index.append(pf)
        self.tstack.add_to_stack(pf)

    def _set_lowlink(self, pf, lowlink):
        """
        Sets lowlink to be the lower of the existing lowlink or the supplied lowlink
        :param pf:
        :param lowlink:
        :return:
        """
        if pf.key in self._lowlinks:
            self._lowlinks[pf.key] = min(self._lowlink(pf), lowlink)
        else:
            self._lowlinks[pf.key] = lowlink

    def check_product_flow(self, flow, termination):
        """
        returns the product flow if it exists, or None if it doesn't
        :param flow:
        :param termination: the process whose reference flow is flow
        :return:
        """
        if termination is None:
            raise ValueError('Must supply a termination')
        k = (flow.external_ref, termination.external_ref)
        if k in self._product_flows:
            return self.product_flow(self._product_flows[k])
        else:
            return None

    def _create_product_flow(self, flow, termination):
        index = len(self._pf_index)
        try:
            pf = ProductFlow(index, flow, termination)
        except NoMatchingReference:
            print('### !!! NO MATCHING REFERENCE !!! ###')  # fix this if it comes up again
            return None
        self._add_product_flow(pf)
        return pf

    def _add_emission(self, flow, direction):
        key = (flow.external_ref, direction)
        if key in self._emissions:
            return self._ef_index[self._emissions[key]]
        else:
            index = len(self._ef_index)
            ef = Emission(index, flow, direction)
            self._emissions[ef.key] = index
            self._ef_index.append(ef)
            return ef

    def terminate(self, exch, strategy):
        """
        Find the ProductFlow that terminates a given exchange.  If an exchange has an explicit termination, use it.
        Otherwise, consult a local cache; and ask the archive [slow] if the cache is not populated.
        :param exch:
        :param strategy:
        :return:
        """
        if exch.termination is not None:
            return self.fg.get(exch.termination)
        else:
            terms = [t for t in self.fg.terminate(exch.flow, direction=exch.direction)]
            if len(terms) == 0:
                return None
            elif len(terms) == 1:
                term = terms[0]
            else:
                if strategy == 'first':
                    term = terms[0]
                elif strategy == 'last':
                    term = terms[-1]
                elif strategy == 'cutoff':
                    return None
                elif strategy == 'mix':
                    raise NotImplementedError('MIX not presently supported (for some reason)')
                    # return self.fg.mix(exch.flow, exch.direction)
                else:
                    raise KeyError('Unknown multi-termination strategy %s' % strategy)
            return self.fg.get(term.external_ref)  # required to get full exchange list

    @staticmethod
    def construct_sparse(nums, nrows, ncols):
        """

        :param nums:
        :param nrows:
        :param ncols:
        :return:
        """
        if len(nums) == 0:
            return csr_matrix((nrows, ncols))
        else:
            try:
                return csr_matrix((nums[:, 2], (nums[:, 0], nums[:, 1])), shape=(nrows, ncols))
            except IndexError:
                print('nrows: %s  ncols: %s' % (nrows, ncols))
                print(nums)
                raise

    def compute_lci(self, product_flow, **kwargs):
        if self.is_in_background(product_flow):
            num_ad = sp.array([[self.tstack.bg_dict(product_flow.index), 0, 1.0]])
            ad = self.construct_sparse(num_ad, self.tstack.ndim, 1)
            x, bx = self.compute_bg_lci(ad, **kwargs)
            return bx
        else:
            af, ad, bf = self.make_foreground(product_flow)
            x_tilde = np.linalg.inv(np.eye(af.shape[0]) - af.todense())[:, 0]
            ad_tilde = ad * x_tilde
            x, bx = self.compute_bg_lci(ad_tilde, **kwargs)
            bf_tilde = csc_matrix(bf * x_tilde)
        return bx + bf_tilde

    def compute_bg_lci(self, ad, threshold=1e-8, count=100):
        """
        Computes background LCI via iterative matrix multiplication.
        :param ad: a vector of background activity levels
        :param threshold: [1e-8] size of the increment (1-norm) relative to the total LCI to finish early
        :param count: [100] maximum number of iterations to perform
        :return:
        """
        x = csr_matrix(ad)  # tested this with ecoinvent: convert to sparse: 280 ms; keep full: 4.5 sec
        total = self.construct_sparse([], *x.shape)
        mycount = 0
        sumtotal = 0.0

        while mycount < count:
            total += x
            x = self._a_matrix.dot(x)
            inc = sum(abs(x).data)
            if inc == 0:
                print('exact result')
                break
            sumtotal += inc
            if inc / sumtotal < threshold:
                break
            mycount += 1
        print('completed %d iterations' % mycount)

        b = self._b_matrix * total
        return total, b

    def _construct_b_matrix(self):
        """
        b matrix only includes emissions from background + downstream processes.
        [foreground processes LCI will have to be computed the foreground way]
        :return:
        """
        if self._b_matrix is not None:
            raise ValueError('B matrix already specified!')
        num_bg = sp.array([[co.emission.index, self.tstack.bg_dict(co.parent.index), co.value]
                           for co in self._bg_emission])
        self._b_matrix = self.construct_sparse(num_bg, self.mdim, self.tstack.ndim)

    def _construct_a_matrix(self):
        ndim = self.tstack.ndim
        num_bg = sp.array([[self.tstack.bg_dict(i.term.index), self.tstack.bg_dict(i.parent.index), i.value]
                           for i in self._interior])
        self._a_matrix = self.construct_sparse(num_bg, ndim, ndim)

    def foreground_flows(self, search=None, outputs=True):
        for k in self.tstack.foreground_flows(outputs=outputs):
            if search is None:
                yield k
            else:
                if bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    yield k

    def background_flows(self, search=None):
        for k in self.tstack.background_flows():
            if search is None:
                yield k
            else:
                if bool(re.search(search, str(k), flags=re.IGNORECASE)):
                    yield k

    def foreground_dependencies(self, product_flow):
        for fg in self._foreground:
            if fg.parent.index == product_flow.index:
                yield fg

    def foreground_emissions(self, product_flow):
        for co in self._cutoff:
            if co.parent.index == product_flow.index:
                yield co

    def foreground(self, pf):
        """
        Computes a list of indices for foreground nodes that are downstream of the named pf (inclusive).
        :param pf: ProductFlow OR ProductFlow.index
        :return: ordered list of product flows
        """
        if isinstance(pf, int):
            pf = self.product_flow(pf)
        if self.is_in_background(pf):
            return []
        return self.tstack.foreground(pf)

    def is_in_background(self, pf):
        """
        Tells whether a Product Flow OR index is part of the background SCC.
        :param pf: product_flow OR product_flow.index
        :return: bool
        """
        return self.tstack.is_background(pf)

    def make_foreground(self, product_flow=None):
        """
        make af, ad, bf for a given list of product flows, or entire if input list is omitted.
        :param product_flow: a single ProductFlow to generate the foreground. If omitted, generate entire foreground.
         if the product_flow is itself in the background, create a foreground model based on its inventory.
        :return: af, ad, bf sparse csc_matrixes

        Not dealing with cutoffs because they are out-of-band here. cutoffs belong to the Big Foreground, not to the
        little archive Foregrounds.  A background database with cutoffs will properly situate the cutoffs in the B
        matrix, where they are treated equivalently.
        """
        af_exch = []
        ad_exch = []
        fg_cutoff = []
        if product_flow is None:
            pdim = self.tstack.pdim

            def fg_dict(x):
                return self.tstack.fg_dict(x)

            bf_exch = self._cutoff
            if self.tstack.pdim == 0:
                return None, None, None
            for fg in self._foreground:
                if self.is_in_background(fg.term.index):
                    ad_exch.append(fg)
                else:
                    af_exch.append(fg)
        else:
            if self.is_in_background(product_flow):
                _af = self.construct_sparse([], 1, 1)
                bg_index = self.tstack.bg_dict(product_flow.index)
                _ad = self._a_matrix[:, bg_index]
                _bf = self._b_matrix[:, bg_index]
                return _af, _ad, _bf

            product_flows = self.foreground(product_flow)
            pdim = len(product_flows)
            bf_exch = []
            _fg_dict = dict((pf.index, n) for n, pf in enumerate(product_flows))

            def fg_dict(x):
                return _fg_dict[x]

            for fg in self._foreground:
                if fg.parent.index in _fg_dict:
                    if self.is_in_background(fg.term.index):
                        ad_exch.append(fg)
                    elif fg.term.index in _fg_dict:
                        af_exch.append(fg)
                    else:
                        fg_cutoff.append(fg)
            for co in self._cutoff:
                if co.parent.index in _fg_dict:
                    bf_exch.append(co)

        num_af = sp.array([[fg_dict(i.term.index), fg_dict(i.parent.index), i.value] for i in af_exch])
        num_ad = sp.array([[self.tstack.bg_dict(i.term.index), fg_dict(i.parent.index), i.value] for i in ad_exch])
        num_bf = sp.array([[co.emission.index, fg_dict(co.parent.index), co.value] for co in bf_exch])
        ndim = self.tstack.ndim
        _af = self.construct_sparse(num_af, pdim, pdim)
        _ad = self.construct_sparse(num_ad, ndim, pdim)
        _bf = self.construct_sparse(num_bf, self.mdim, pdim)
        if len(fg_cutoff) > 0:
            for co in fg_cutoff:
                # this should never happen
                print('Losing FG Cutoff %s' % co)
        return _af, _ad, _bf

    def _update_component_graph(self):
        self.tstack.add_to_graph(self._interior_incoming)  # background should be brought up to date
        while len(self._interior_incoming) > 0:
            k = self._interior_incoming.pop()
            k.adjust_val()
            if self.is_in_background(k.parent.index):
                self._interior.append(k)
            else:
                self._foreground.append(k)

        while len(self._cutoff_incoming) > 0:
            k = self._cutoff_incoming.pop()
            k.adjust_val()
            if self.is_in_background(k.parent.index):
                self._bg_emission.append(k)
            else:
                self._cutoff.append(k)

        if self._a_matrix is None and self.tstack.background is not None:
            self._construct_a_matrix()
            self._construct_b_matrix()

        # self.make_foreground()

    def add_all_ref_products(self, multi_term='first', default_allocation=None, net_coproducts=False):
        for p in self.fg.processes():
            for x in p.references():
                j = self.check_product_flow(x.flow, p)
                if j is None:
                    self._add_ref_product(x.flow, p, multi_term, default_allocation, net_coproducts)
        self._update_component_graph()

    def add_ref_product(self, flow, term, multi_term='first', default_allocation=None, net_coproducts=False):
        """
        Here we are adding a reference product - column of the A + B matrix.  The termination must be supplied.
        :param flow: a product flow
        :param term: a process that includes the product flow among its reference exchanges (input OR output).
        :param multi_term: ['first'] specify how to handle ambiguous terminations.  Possible answers are:
         'cutoff' - call the flow a cutoff and ignore it
         'mix' - create a new "market" process that mixes the inputs
         'first' - take the first match (alphabetically by process name)
         'last' - take the last match (alphabetically by process name)
         Not currently implemented.
        :param default_allocation: an LcQuantity to use for allocation if unallocated processes are encountered
        :param net_coproducts: [False] for unallocated multi-output processes, compute net demand of coproducts instead
         of allocating. (Works for USLCI; still buggy for ecoinvent)
        :return:
        """
        j = self.check_product_flow(flow, term)

        if j is None:
            j = self._add_ref_product(flow, term, multi_term, default_allocation, net_coproducts)
            self._update_component_graph()
        return j

    def _add_ref_product(self, flow, term, multi_term, default_allocation, net_coproducts):
        old_recursion_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(self.required_recursion_limit)

        j = self._create_product_flow(flow, term)
        self._traverse_term_exchanges(j, multi_term, default_allocation, net_coproducts)

        sys.setrecursionlimit(old_recursion_limit)
        return j

    def _traverse_term_exchanges(self, parent, multi_term, default_allocation, net_coproducts):
        """
        Implements the Tarjan traversal
        :param parent: a ProductFlow
        :param default_allocation:
        :param net_coproducts:
        :return:
        """
        rx = parent.process.reference(parent.flow)
        no_alloc = False
        cutoff_refs = False
        if len(parent.process.reference_entity) > 1:
            if not parent.process.is_allocated(rx):
                if default_allocation is not None:
                    parent.process.allocate_by_quantity(default_allocation)
                else:
                    no_alloc = True

        if no_alloc:
            if net_coproducts:
                exchs = [x for x in parent.process.inventory(rx)]
                cutoff_refs = True
            else:
                print('Cutting off at un-allocated multi-output process:\n %s\n %s' % (parent.process, rx))
                exchs = []
        else:
            exchs = [x for x in parent.process.inventory(rx)]

        for exch in exchs:  # unallocated exchanges
            if cutoff_refs:
                val = pval = exch.value
            else:
                try:
                    val = pval = exch[rx]
                except TypeError:
                    continue
            if val is None or val == 0:
                # don't add zero entries (or descendants) to sparse matrix
                continue
            # interior flow-- enforce normative direction
            if exch.direction == 'Output':
                pval *= -1
            if exch in parent.process.reference_entity:
                if cutoff_refs:
                    # for net coproducts- all coproducts after the first are simply created as free sources
                    i = self.check_product_flow(exch.flow, parent.process)
                    if i == parent:
                        # don't add ourself as a coproduct
                        continue
                    if i is None:
                        # TODO: need to figure out why this causes SCC recursion errors
                        i = self._create_product_flow(exch.flow, parent.process)
                        net = self._add_emission(exch.flow, exch.direction)
                        # TODO: This should be 1.0 instead of val, but entries get auto-normalized in adjust_val()
                        self.add_cutoff(i, net, val)
                        self._surplus_coproducts[i] = parent
                    # then the first also generates the coproducts; activity levels of free sources will be net demand
                    self.add_interior(parent, i, pval)
                # in either case, we're done with the exchange
                continue
            # normal non-reference exchange. Either a dependency (if interior) or a cutoff (if exterior).
            term = self.terminate(exch, multi_term)
            if term is None:
                # cutoff -- add the exchange value to the exterior matrix
                emission = self._add_emission(exch.flow, exch.direction)  # check, create, and add all at once
                self.add_cutoff(parent, emission, val)
                continue

            # so it's interior-- does it exist already?
            i = self.check_product_flow(exch.flow, term)
            if i is None:
                # not visited -- need to visit
                i = self._create_product_flow(exch.flow, term)
                if i is None:
                    print('Cutting off at Parent process: %s\n%s\n' % (parent.process.external_ref, parent))
                    continue
                self._traverse_term_exchanges(i, multi_term, default_allocation, net_coproducts)
                # carry back lowlink, if lower
                self._set_lowlink(parent, self._lowlink(i))
            elif self.tstack.check_stack(i):
                # visited and currently on stack - carry back index if lower
                self._set_lowlink(parent, self.index(i))
            else:
                # visited, not on stack- nothing to do
                pass
            # add the exchange value to the interior matrix
            self.add_interior(parent, i, pval)

        # name an SCC if we've found one
        if self._lowlink(parent) == self.index(parent):
            self.tstack.label_scc(self.index(parent), parent.key)

    def add_cutoff(self, parent, emission, val):
        """
        Create an exchange for a cutoff flow (incl. elementary flows)
        :param parent: product flow- B matrix column
        :param emission: emission - B matrix row
        :param val: raw exchange value
        """
        self._cutoff_incoming.append(CutoffEntry(parent, emission, val))

    def add_interior(self, parent, term, val):
        """
        Enforces the convention that interior exchanges are inputs; reference flows are outputs; symmetrically to
        inbound_ev determination in ProductFlow constructore

        :param parent: product flow - A matrix column
        :param term: product flow - A matrix row
        :param val: raw (direction-adjusted) exchange value
        :return:
        """
        if parent is term:
            print('self-dependency detected! %s' % parent.process)
            parent.adjust_ev(val)
        else:
            self._interior_incoming.append(MatrixEntry(parent, term, val))

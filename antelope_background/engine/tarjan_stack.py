from collections import defaultdict

from .product_flow import ProductFlow


class TarjanStack(object):
    """
    Stores the current stack and provides a record of named SCCs
    """
    def __init__(self):
        self._stack = []
        self._stack_hash = set()
        self._sccs = defaultdict(set)  # dict mapping lowest index (lowlink = SCC ID) to the set of scc peers
        self._scc_of = dict()  # dict mapping product flow to SCC ID (reverse mapping of _sccs)

        self._component_cols_by_row = defaultdict(set)  # nonzero columns in given row (upstream dependents)
        self._component_rows_by_col = defaultdict(set)  # nonzero rows in given column (downstream dependencies)

        self._background = None  # single scc_id representing largest scc
        self._downstream = set()  # sccs on which background depends

        self._bg_processes = []  # ordered list of background nodes
        self._fg_processes = []  # ordered list of foreground nodes
        self._bg_index = dict()  # maps product_flow.index to a* / b* column -- STATIC
        self._fg_index = dict()  # maps product_flow.index to af / ad/ bf column -- VOLATILE

    def check_stack(self, product_flow):
        """
        :param product_flow:
        :return:
        """
        return product_flow in self._stack_hash

    def add_to_stack(self, product_flow):
        if not isinstance(product_flow, ProductFlow):
            raise TypeError('TarjanStack should consist only of ProductFlows')
        if self.check_stack(product_flow):
            raise ValueError('ProductFlow already in stack')
        self._stack.append(product_flow)
        self._stack_hash.add(product_flow)

    def pop_from_stack(self):
        pf = self._stack.pop()
        self._stack_hash.remove(pf)
        return pf

    def label_scc(self, index, key):
        """

        :param index: the index of the lowest link in the SCC-- becomes scc ID
        :param key: the identifier for the lowest link in the SCC (necessary to ID the link)
        :return:
        """
        while 1:
            node = self._stack.pop()
            self._stack_hash.remove(node)
            self._sccs[index].add(node)
            self._scc_of[node] = index
            if node.key == key:
                break

    def _set_background(self):
        ml = 0
        ind = None
        for i, t in self._sccs.items():
            if len(t) > ml:
                ml = len(t)
                ind = i
        if ml > 1:
            self._background = ind
            self._set_downstream()
            self._generate_bg_index()

    def _set_downstream(self, upstream=None):
        """
        recursive function to tag all nodes downstream of the named node.
        :param upstream: [None] if none, use background
        :return:
        """
        if upstream is None:
            if self._background is None:
                return
            upstream = self._background

        for dep in self._component_rows_by_col[upstream]:
            if dep != upstream:  # skip self-dependencies
                self._downstream.add(dep)
                self._set_downstream(dep)

    def _generate_bg_index(self):
        if self._background is None:
            self._bg_processes = []
            self._bg_index = dict()
        else:
            bg = []
            for i in self.scc(self._background):
                bg.append(i)
            for i in self._downstream:
                for j in self.scc(i):
                    bg.append(j)

            self._bg_processes = bg
            self._bg_index = dict((pf.index, n) for n, pf in enumerate(bg))  # mapping of *pf* index to a-matrix index

    def _generate_foreground_index(self):
        """
        Perform topological sort of fg nodes. Store the results of the sort by node
        This is terribly inefficient-- mainly the list-comprehension that iterates through nodes INSIDE of a while loop.
        Let's fix it, but not today. For now, we just add_all_ref_products and only run this once.
        :return:
        """
        fg_nodes = set()
        fg_ordering = []
        self._fg_processes = []
        for k in self._sccs.keys():
            if k != self._background and k not in self._downstream:
                if len(self._component_cols_by_row[k]) == 0:  # no columns depend on row: fg outputs
                    fg_ordering.append(k)
                else:
                    fg_nodes.add(k)

        while len(fg_nodes) > 0:
            new_outputs = set()
            for k in fg_nodes:
                depends = [j for j in self._component_cols_by_row[k] if j not in fg_ordering and j != k]
                if len(depends) == 0:  # all upstream is fg
                    new_outputs.add(k)
            fg_ordering.extend(list(new_outputs))  # add new outputs to ordering
            for k in new_outputs:
                fg_nodes.remove(k)  # remove new outputs from consideration

        for k in fg_ordering:
            for pf in self.scc(k):
                self._fg_processes.append(pf)

        self._fg_index = dict((pf.index, n) for n, pf in enumerate(self._fg_processes))

    def add_to_graph(self, interiors):
        """
        take a list of interior exchanges (parent, term, exch) and add them to the component graph
        :return:
        """
        for i in interiors:
            row = self.scc_id(i.term)
            col = self.scc_id(i.parent)
            self._component_cols_by_row[row].add(col)
            self._component_rows_by_col[col].add(row)
        self._set_background()
        self._generate_foreground_index()

    @property
    def background(self):
        return self._background

    @property
    def ndim(self):
        return len(self._bg_index)

    @property
    def pdim(self):
        return len(self._fg_index)

    def _foreground_components(self, index):
        """
        Returns a list of foreground SCCs that are downstream of the named index (inclusive). Sorts the list by
        order in _fg_index.
        :param index:
        :return:
        """
        queue = [index]
        fg = []
        while len(queue) > 0:
            current = queue.pop(0)
            if current not in fg:
                queue.extend([k for k in self._component_rows_by_col[current] if not self.is_background(k)])
                fg.append(current)
        return fg

    def foreground(self, pf):
        """
        computes a list of foreground SCCs that are downstream of the supplied product flow.
        Then converts the SCCs into an ordered list of product flows that make up the columns of the foreground.
        :param pf: a product flow.
        :return: topologically-ordered, loop-detecting list of non-background product flows
        """
        index = self.scc_id(pf)
        if index == self._background or index in self._downstream:
            return []

        fg = self._foreground_components(index)
        fg_pf = []
        for c in fg:
            for k in self.scc(c):
                fg_pf.append(k)
        return sorted(fg_pf, key=lambda x: (x.index != pf.index, self._fg_index[x.index]))  # ensure pf is first

    def foreground_flows(self, outputs=False):
        """
        Generator. Yields product flows in the volatile foreground
        :param outputs: [False] (bool) if True, only report strict outputs (nodes on which no other nodes depend)
        :return:
        """
        for pf in self._fg_processes:
            if outputs:
                k = self._scc_of[pf]
                if len(self._component_cols_by_row[k]) > 0:
                    return  # cut out early since fg_processes is ordered
            yield pf

    def background_flows(self):
        """
        Generator. Yields product flows in the db background or downstream.
        :return:
        """
        for i in self._bg_processes:
            yield i

    def bg_node(self, bg_index):
        """
        Returns a ProductFlow corresponding to the supplied background column.
        self.bg_node(self.bg_dict(x.index)) = x
        :param bg_index: row / column number of A* or column of B*
        :return: ProductFlow
        """
        return self._bg_processes[bg_index]

    def bg_dict(self, pf_index):
        """
        Maps a ProductFlow.index to the row/column number in A* or column in B*
        :param pf_index:
        :return:
        """
        try:
            return self._bg_index[pf_index]
        except KeyError:
            return None

    def fg_node(self, fg_index):
        """
        Returns a ProductFlow corresponding to the supplied input column.
        self.fg_node(self.fg_dict(x.index)) = x
        :param fg_index:
        :return:
        """
        return self._fg_processes[fg_index]

    def fg_dict(self, pf_index):
        """
        Maps a ProductFlow.index to the column number in Af or Ad or Bf
        :param pf_index:
        :return:
        """
        try:
            return self._fg_index[pf_index]
        except KeyError:
            return None

    def is_background(self, pf):
        """
        Tells whether a Product Flow OR index is background. Note: SCC IDs are indexes of the first product flow
        encountered in a given SCC, or the only product flow for a singleton (i.e. acyclic foreground) SCC
        :param pf: product_flow OR product_flow.index
        :return: bool
        """
        if isinstance(pf, ProductFlow):
            return pf.index in self._bg_index
        return pf in self._bg_index

    def scc_id(self, pf):
        return self._scc_of[pf]

    def sccs(self):
        return self._sccs.keys()

    def nontrivial_sccs(self):
        for k, s in self._sccs.items():
            if len(s) > 1:
                yield k

    def is_background_scc(self, k):
        if k == self._background or k in self._downstream:
            return True
        return False

    def scc(self, index):
        return self._sccs[index]

    def scc_peers(self, pf):
        """
        Returns nodes in the same SCC as product flow
        :param pf:
        :return:
        """
        for i in self._sccs[self._scc_of[pf]]:
            yield i

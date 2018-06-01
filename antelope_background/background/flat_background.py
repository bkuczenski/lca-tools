"""
class for storing static results of a tarjan ordering
"""

from scipy.sparse.csc import csc_matrix
from scipy.sparse.csr import csr_matrix
from scipy.sparse.linalg import inv
from scipy.sparse import eye
from scipy.io import savemat, loadmat

import os
from collections import namedtuple

from antelope_background.engine import BackgroundEngine
from lcatools.exchanges import comp_dir
from lcatools.from_json import from_json, to_json


class NoLciDatabase(Exception):
    pass


class TermRef(object):
    def __init__(self, flow_ref, direction, term_ref, scc_id=None):
        """

        :param flow_ref:
        :param direction: direction w.r.t. term
        :param term_ref:
        :param scc_id: None or 0 for singleton /emission; external_ref of a contained process for SCC
        """
        self._f = flow_ref
        self._d = {'Input': 0, 'Output': 1, 0: 0, 1: 1}[direction]
        self._t = term_ref
        self._s = 0
        self.scc_id = scc_id

    @property
    def term_ref(self):
        return self._t

    @property
    def flow_ref(self):
        return self._f

    @property
    def direction(self):
        return ('Input', 'Output')[self._d]

    @property
    def scc_id(self):
        if self._s == 0:
            return []
        return self._s

    @scc_id.setter
    def scc_id(self, item):
        if item is None:
            self._s = 0
        else:
            self._s = item

    def __array__(self):
        return self.flow_ref, self._d, self.term_ref, self._s

    def __iter__(self):
        return iter(self.__array__())


ExchDef = namedtuple('ExchDef', ('process', 'flow', 'direction', 'term', 'value'))


def _iterate_a_matrix(a, y, threshold=1e-8, count=100, quiet=False):
    y = csr_matrix(y)  # tested this with ecoinvent: convert to sparse: 280 ms; keep full: 4.5 sec
    total = csr_matrix(y.shape)
    if a is None:
        return total

    mycount = 0
    sumtotal = 0.0

    while mycount < count:
        total += y
        y = a.dot(y)
        inc = sum(abs(y).data)
        if inc == 0:
            if not quiet:
                print('exact result')
            break
        sumtotal += inc
        if inc / sumtotal < threshold:
            break
        mycount += 1
    if not quiet:
        print('completed %d iterations' % mycount)

    return total


def _unit_column_vector(dim, inx):
    return csr_matrix(((1,), ((inx,), (0,))), shape=(dim, 1))


def split_af(_af, _inds):
    """
    splits the input matrix into diagonal and off-diagonal portions, with the split being determined by _inds
    :param _af:
    :param _inds:
    :return:
    """
    _af = _af.tocoo()
    _r = _af.row
    _c = _af.col
    _d = _af.data
    _d_non = []
    _d_scc = []
    _shape = _af.shape
    for i in range(len(_d)):
        if _r[i] in _inds and _c[i] in _inds:
            _d_non.append(0)
            _d_scc.append(_d[i])
        else:
            _d_non.append(_d[i])
            _d_scc.append(0)
    _af_non = csc_matrix((_d_non, (_r, _c)), shape=_shape)
    _af_scc = csc_matrix((_d_scc, (_r, _c)), shape=_shape)
    assert (_af_non + _af_scc - _af).nnz == 0
    return _af_non, _af_scc


def _determine_scc_inds(ts):
    scc_inds = set()
    for _s in ts.nontrivial_sccs():
        if ts.is_background_scc(_s):
            continue
        for k in ts.scc(_s):
            scc_inds.add(ts.fg_dict(k.index))
    return scc_inds


def flatten(af, ad, bf, ts):
    """
    Accepts a fully populated background engine as argument

    :param af:
    :param ad:
    :param bf:
    :param ts:
    :return: af_flat, ad_flat, bf_flat
    """
    scc_inds = _determine_scc_inds(ts)

    non, scc = split_af(af, scc_inds)

    scc_inv = inv(eye(ts.pdim).tocsc() - scc)

    return non * scc_inv, ad * scc_inv, bf * scc_inv


class FlatBackground(object):
    """
    Static, ordered background stored in an easily serializable way
    """
    @classmethod
    def from_index(cls, index, **kwargs):
        """
        :param index: an index interface with operable processes() and terminate()
        :param kwargs: origin, quiet
        :return:
        """
        be = BackgroundEngine(index)
        be.add_all_ref_products()
        return cls.from_background_engine(be, **kwargs)

    @classmethod
    def from_background_engine(cls, be, **kwargs):
        af, ad, bf = be.make_foreground()
        af, ad, bf = flatten(af, ad, bf, be.tstack)

        _map_nontrivial_sccs = {k: be.product_flow(k).process.external_ref for k in be.tstack.nontrivial_sccs()}

        def _make_term_ref(pf):
            try:
                _scc_id = _map_nontrivial_sccs[be.tstack.scc_id(pf)]
            except KeyError:
                _scc_id = 0
            return pf.flow.external_ref, pf.direction, pf.process.external_ref, _scc_id

        def _make_term_ext(em):
            return em.flow.external_ref, comp_dir(em.direction), em.compartment[-1], 0

        return cls([_make_term_ref(x) for x in be.foreground_flows(outputs=False)],
                   [_make_term_ref(x) for x in be.background_flows()],
                   [_make_term_ext(x) for x in be.emissions],
                   af, ad, bf,
                   lci_db=be.lci_db,
                   **kwargs)

    @classmethod
    def from_file(cls, file, **kwargs):
        ext = os.path.splitext(file)[1]
        if ext == '.mat':
            return cls.from_matfile(file, **kwargs)
        elif ext == '.hdf':
            return cls.from_hdf5(file, **kwargs)
        else:
            raise ValueError('Unsupported file type %s' % ext)

    @classmethod
    def from_hdf5(cls, fle, quiet=True):
        raise NotImplementedError

    @classmethod
    def from_matfile(cls, file, quiet=True):
        d = loadmat(file)
        if 'A' in d:
            lci_db = (d['A'].tocsr(), d['B'].tocsr())
        else:
            lci_db = None

        ix = from_json(file + '.index.json.gz')

        '''
        def _unpack_term_ref(arr):
            _xt = arr[3][0]
            if len(_xt) == 1:
                _xt = _xt[0]
            return arr[0][0], arr[1][0][0], arr[2][0], _xt
        
        return cls((_unpack_term_ref(f) for f in d['foreground']),
                   (_unpack_term_ref(f) for f in d['background']),
                   (_unpack_term_ref(f) for f in d['exterior']),
                   d['Af'].tocsr(), d['Ad'].tocsr(), d['Bf'].tocsr(),
                   lci_db=lci_db,
                   quiet=quiet)
        '''
        return cls(ix['foreground'], ix['background'], ix['exterior'],
                   d['Af'].tocsr(), d['Ad'].tocsr(), d['Bf'].tocsr(),
                   lci_db=lci_db,
                   quiet=quiet)

    def __init__(self, foreground, background, exterior, af, ad, bf, lci_db=None, quiet=True):
        """

        :param foreground: iterable of foreground Product Flows as TermRef params
        :param background: iterable of background Product Flows as TermRef params
        :param exterior: iterable of Exterior flows as TermRef params
        :param af: sparse, flattened Af
        :param ad: sparse, flattened Ad
        :param bf: sparse, flattened Bf
        :param lci_db: [None] optional (A, B) 2-tuple
        :param quiet: [True] does nothing for now
        """
        self._fg = tuple([TermRef(*f) for f in foreground])
        self._bg = tuple([TermRef(*x) for x in background])
        self._ex = tuple([TermRef(*x) for x in exterior])

        self._af = af
        self._ad = ad
        self._bf = bf

        if lci_db is None:
            self._A = None
            self._B = None
        else:
            self._A = lci_db[0].tocsr()
            self._B = lci_db[1].tocsr()

        self._fg_index = {(k.term_ref, k.flow_ref): i for i, k in enumerate(self._fg)}
        self._bg_index = {(k.term_ref, k.flow_ref): i for i, k in enumerate(self._bg)}
        self._ex_index = {(k.term_ref, k.flow_ref): i for i, k in enumerate(self._ex)}

        self._quiet = quiet

    def index_of(self, term_ref, flow_ref):
        key = (term_ref, flow_ref)
        if key in self._fg_index:
            return self._fg_index[key]
        elif key in self._bg_index:
            return self._bg_index[key]
        elif key in self._ex_index:
            return self._ex_index[key]
        else:
            raise KeyError('Unknown termination %s, %s' % key)

    @property
    def _complete(self):
        return self._A is not None and self._B is not None

    @property
    def ndim(self):
        return len(self._bg)

    @property
    def pdim(self):
        return len(self._fg)

    @property
    def fg(self):
        return self._fg

    @property
    def bg(self):
        return self._bg

    @property
    def ex(self):
        return self._ex

    def is_in_scc(self, process, ref_flow):
        if self.is_in_background(process, ref_flow):
            tr = self._bg[self._bg_index[(process, ref_flow)]]
        else:
            tr = self._fg[self._fg_index[(process, ref_flow)]]
        return len(tr.scc_id) > 0

    def is_in_background(self, process, ref_flow):
        return (process, ref_flow) in self._bg_index

    def foreground(self, process, ref_flow, traverse=False):
        """
        Most of the way toward making exchanges. yields a sequence of 5-tuples defining terminated exchanges.

        NOTE: traverse=True differs from the prior implementation because the old BackgroundEngine returned an Af
        matrix and the foreground routine generated one exchange per matrix entry.

        In contrast, the current implementation traverses the foreground and creates one exchange per traversal link.
        If a fragment references the same subfragment multiple times, this will result in redundant entries for the
        same fragment.  At the moment this is by design but it may be undesirable.

        An easy solution would be to keep a log of nonzero Af indices and 'continue' if one is encountered.
        :param process:
        :param ref_flow:
        :param traverse: [False] if True, generate one exchange for every traversal link. Default is to create one
        exchange for every matrix entry.  traverse=True will produce duplicate exchanges in cases where sub-fragments
        are traversed multiple times.
        :return:
        """
        index = self._fg_index[process, ref_flow]
        yield ExchDef(process, ref_flow, self._fg[index].direction, None, 1.0)

        cols_seen = set()
        cols_seen.add(index)

        q = [index]
        while len(q) > 0:
            current = q.pop(0)
            node = self._fg[current]
            fg_deps = self._af[:, current]
            rows, cols = fg_deps.nonzero()
            for i in range(len(rows)):
                assert cols[i] == 0  # 1-column slice
                assert rows[i] > current  # well-ordered and flattened
                if rows[i] in cols_seen:
                    if traverse:
                        q.append(rows[i])  # allow fragment to be traversed multiple times
                else:
                    cols_seen.add(rows[i])
                    q.append(rows[i])
                term = self._fg[rows[i]]
                dat = fg_deps.data[i]
                if dat < 0:
                    dat *= -1
                    dirn = term.direction  # comp directions w.r.t. parent node
                else:
                    dirn = comp_dir(term.direction)  # comp directions w.r.t. parent node
                yield ExchDef(node.term_ref, term.flow_ref, dirn, term.term_ref, dat)

    @staticmethod
    def _generate_exch_defs(node_ref, data_vec, enumeration):
        rows, cols = data_vec.nonzero()
        for i in range(len(rows)):
            term = enumeration[rows[i]]
            dat = data_vec.data[i]
            if dat < 0:
                dat *= -1
                dirn = term.direction
            else:
                dirn = comp_dir(term.direction)
            yield ExchDef(node_ref, term.flow_ref, dirn, term.term_ref, dat)

    @staticmethod
    def _generate_em_defs(node_ref, data_vec, enumeration):
        """
        Emissions have a natural direction which should not be changed.
        :param node_ref:
        :param data_vec:
        :param enumeration:
        :return:
        """
        rows, cols = data_vec.nonzero()
        for i in range(len(rows)):
            term = enumeration[rows[i]]
            dat = data_vec.data[i]
            dirn = comp_dir(term.direction)
            yield ExchDef(node_ref, term.flow_ref, dirn, term.term_ref, dat)

    def dependencies(self, process, ref_flow):
        if self.is_in_background(process, ref_flow):
            index = self._bg_index[process, ref_flow]
            bg_deps = self._A[:, index]
        else:
            index = self._fg_index[process, ref_flow]
            bg_deps = self._ad[:, index]

        for x in self._generate_exch_defs(process, bg_deps, self._bg):
            yield x

    def emissions(self, process, ref_flow):
        if self.is_in_background(process, ref_flow):
            index = self._bg_index[process, ref_flow]
            ems = self._B[:, index]
        else:
            index = self._fg_index[process, ref_flow]
            ems = self._bf[:, index]

        for x in self._generate_em_defs(process, ems, self._ex):
            yield x

    def _x_tilde(self, process, ref_flow, quiet=True, **kwargs):
        index = self._fg_index[process, ref_flow]
        return _iterate_a_matrix(self._af, _unit_column_vector(self.pdim, index), quiet=quiet, **kwargs)

    def ad(self, process, ref_flow, **kwargs):
        if self.is_in_background(process, ref_flow):
            for x in self.dependencies(process, ref_flow):
                yield x
        else:
            ad_tilde = self._ad.dot(self._x_tilde(process, ref_flow, **kwargs))
            for x in self._generate_exch_defs(process, ad_tilde, self._bg):
                yield x

    def bf(self, process, ref_flow, **kwargs):
        if self.is_in_background(process, ref_flow):
            for x in self.emissions(process, ref_flow):
                yield x
        else:
            bf_tilde = self._bf.dot(self._x_tilde(process, ref_flow, **kwargs))
            for x in self._generate_em_defs(process, bf_tilde, self._ex):
                yield x

    def _compute_bg_lci(self, ad, **kwargs):
        return self._B.dot(_iterate_a_matrix(self._A, ad, **kwargs))

    def _compute_lci(self, process, ref_flow, **kwargs):
        if self.is_in_background(process, ref_flow):
            if not self._complete:
                raise NoLciDatabase
            ad = _unit_column_vector(self.ndim, self._bg_index[process, ref_flow])
            bx = self._compute_bg_lci(ad, **kwargs)
            return bx
        else:
            x_tilde = self._x_tilde(process, ref_flow, **kwargs)
            ad_tilde = self._ad.dot(x_tilde)
            bf_tilde = self._bf.dot(x_tilde)
            if self._complete:
                bx = self._compute_bg_lci(ad_tilde, **kwargs)
                return bx + bf_tilde
            else:
                return bf_tilde

    def lci(self, process, ref_flow, **kwargs):
        for x in self._generate_em_defs(process,
                                        self._compute_lci(process, ref_flow, **kwargs),
                                        self._ex):
            yield x

    def _write_index(self, ix_filename):
        ix = {'foreground': [tuple(f) for f in self._fg],
              'background': [tuple(f) for f in self._bg],
              'exterior': [tuple(f) for f in self._ex]}
        to_json(ix, ix_filename, gzip=True)

    def _write_mat(self, filename, complete=True):
        d = {'Af': csr_matrix((0, 0)) if self._af is None else self._af,
             'Ad': csr_matrix((0, 0)) if self._ad is None else self._ad,
             'Bf': csr_matrix((0, 0)) if self._bf is None else self._bf}
        if complete and self._complete:
            d['A'] = self._A
            d['B'] = self._B
        savemat(filename, d)

    def write_to_file(self, filename, complete=True, filetype='.mat'):
        if filetype is None:
            filetype = os.path.splitext(filename)[1]
        if filetype == '.mat':
            self._write_mat(filename, complete=complete)
        else:
            raise ValueError('Unsupported file type %s' % filetype)
        self._write_index(filename + '.index.json.gz')

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

from .engine import BackgroundEngine
from lcatools.exchanges import comp_dir


TermRef = namedtuple('TermRef', ('flow_ref', 'direction', 'term_ref', 'scc_id'))
ExchDef = namedtuple('ExchDef', ('process', 'flow', 'direction', 'term', 'value'))


def _iterate_a_matrix(a, y, threshold=1e-8, count=100):
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
            print('exact result')
            break
        sumtotal += inc
        if inc / sumtotal < threshold:
            break
        mycount += 1
    print('completed %d iterations' % mycount)

    return total


def _unit_column_vector(dim, inx):
    return csr_matrix((1, (inx, 0)), shape=(dim, 1))


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
    assert (_af_non + _af_scc - _af).nnz() == 0
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

    scc_inv = inv(eye(ts.pdim) - scc)

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
                _scc_id = None
            return pf.flow.external_ref, pf.direction, pf.process.external_ref, _scc_id

        def _make_term_ext(em):
            return em.flow.external_ref, em.direction, em.compartment[-1], None

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
        return cls(d['foreground'], d['background'], d['exterior'], (d['A'], d['B']), d['af'], d['ad'], d['bf'],
                   origin=d['origin'], quiet=quiet)

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
        self._fg = [TermRef(*f) for f in foreground]
        self._bg = [TermRef(*x) for x in background]
        self._ex = [TermRef(*x) for x in exterior]

        self._af = af
        self._ad = ad
        self._bf = bf

        if lci_db is None:
            self._A = None
            self._B = None
        else:
            self._A = lci_db[0]
            self._B = lci_db[1]

        self._fg_index = {(k.term_ref, k.flow_ref): i for i, k in enumerate(self._fg)}
        self._bg_index = {(k.term_ref, k.flow_ref): i for i, k in enumerate(self._bg)}
        self._ex_index = {(k.term_ref, k.flow_ref): i for i, k in enumerate(self._ex)}

        self._quiet = quiet

    @property
    def pdim(self):
        return len(self._fg)

    @property
    def fg(self):
        for x in self._fg:
            yield x

    @property
    def bg(self):
        for x in self._bg:
            yield x

    @property
    def ex(self):
        for x in self._ex:
            yield x

    def is_in_background(self, process, ref_flow):
        return (process, ref_flow) in self._bg_index

    def foreground(self, process, ref_flow):
        """
        Most of the way toward making exchanges. yields a sequence of 5-tuples defining
        :param process:
        :param ref_flow:
        :return:
        """
        index = self._fg_index[process, ref_flow]
        yield ExchDef(process, ref_flow, self._fg[index].direction, None, 1.0)

        q = [index]
        while len(q) > 0:
            current = q.pop(0)
            node = self._fg[current]
            fg_deps = self._af[:, current]
            rows, cols = fg_deps.nonzero()
            for i in range(len(rows)):
                assert cols[i] == 0  # 1-column slice
                assert rows[i] > current  # well-ordered and flattened
                q.append(rows[i])
                term = self._fg[rows[i]]
                dat = fg_deps.data[i]
                if dat < 0:
                    dat *= -1
                    dirn = term.direction  # comp directions w.r.t. parent node
                else:
                    dirn = comp_dir(term.direction)  # comp directions w.r.t. parent node
                yield ExchDef(node.term_ref, term.flow_ref, dirn, term.term_ref, dat)

    def dependencies(self, process, ref_flow):
        if self.is_in_background(process, ref_flow):
            index = self._bg_index[process, ref_flow]
            bg_deps = self._A[:, index]
        else:
            index = self._fg_index[process, ref_flow]
            bg_deps = self._ad[:, index]

        rows, cols = bg_deps.nonzero()
        for i in range(len(rows)):
            term = self._bg[rows[i]]
            dat = bg_deps.data[i]
            if dat < 0:
                dat *= -1
                dirn = term.direction  # comp directions w.r.t. parent node
            else:
                dirn = comp_dir(term.direction)  # comp directions w.r.t. parent node
            yield ExchDef(process, term.flow_ref, dirn, term.term_ref, dat)

    def emissions(self, process, ref_flow):
        if self.is_in_background(process, ref_flow):
            index = self._bg_index[process, ref_flow]
            ems = self._B[:, index]
        else:
            index = self._fg_index[process, ref_flow]
            ems = self._bf[:, index]

        rows, cols = ems.nonzero()
        for i in range(len(rows)):
            em = self._ex[rows[i]]
            dat = ems.data[i]
            if dat < 0:
                dat *= -1
                dirn = em.direction  # comp directions w.r.t. parent node
            else:
                dirn = comp_dir(em.direction)  # comp directions w.r.t. parent node
            yield ExchDef(process, em.flow_ref, dirn, em.term_ref, dat)

    def _x_tilde(self, process, ref_flow, **kwargs):
        index = self._fg_index[process, ref_flow]
        return _iterate_a_matrix(self._af, _unit_column_vector(self.pdim, index), **kwargs)

    def ad(self, process, ref_flow, **kwargs):
        if self.is_in_background(process, ref_flow):
            pass
        return self._ad.dot(self._x_tilde(process, ref_flow, **kwargs))

    def bf(self, process, ref_flow, **kwargs):
        return self._bf.dot(self._x_tilde(process, ref_flow, **kwargs))

    def lci(self, process, ref_flow, **kwargs):
        pass

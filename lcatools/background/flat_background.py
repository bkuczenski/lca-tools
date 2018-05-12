"""
class for storing static results of a tarjan ordering
"""

from scipy.sparse.csc import csc_matrix
from scipy.sparse.linalg import inv
from scipy.sparse import eye
from scipy.io import savemat, loadmat

from datetime import datetime

from lcatools.background.background import BackgroundEngine


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
    for _s in ts.nontrivial_fg_sccs():
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
    def from_archive(cls, archive, **kwargs):
        """
        The archive must be able to generate an index interface with operable processes() and terminate()
        :param archive:
        :param kwargs: origin, quiet
        :return:
        """
        be = BackgroundEngine(archive.make_interface('index'))
        be.add_all_ref_products()
        af, ad, bf = be.make_foreground()
        af, ad, bf = flatten(af, ad, bf, be.tstack)
        return cls(be.foreground_flows(outputs=False), be.background_flows(), be.emissions, be.lci_db, af, ad, bf,
                   **kwargs)

    @classmethod
    def from_matfile(cls, file, quiet=True):
        d = loadmat(file)
        return cls(d['foreground'], d['background'], d['exterior'], (d['A'], d['B']), d['af'], d['ad'], d['bf'],
                   origin=d['origin'], quiet=quiet)

    def __init__(self, foreground, background, exterior, lci_db, af, ad, bf, origin=None, quiet=True):
        """

        :param foreground: iterable of foreground ProductFlows
        :param background: iterable of background ProductFlows
        :param exterior: iterable of Emissions
        :param lci_db: (A, B)
        :param af: sparse, flattened Af
        :param ad: sparse, flattened Ad
        :param bf: sparse, flattened Bf
        :param origin: [None] assigned origin. if None, will use the first origin encountered and append datestamp
        :param quiet: [True] does nothing for now
        """
        new_date = datetime.now().strftime('%Y%m%d')

        self._fg = []
        for f in foreground:
            if origin is None:
                origin = '%s.bg_%s' % (f.process.origin, new_date)
            self._fg.append((f.process.external_ref, f.flow.external_ref, f.direction))
        self._bg = [(x.process.external_ref, x.flow.external_ref, x.direction) for x in background]
        self._em = [(x.flow.external_ref, x.direction, x.compartment) for x in exterior]
        self._A = lci_db[0]
        self._B = lci_db[1]
        self._af = af
        self._ad = ad
        self._bf = bf

        self._quiet = quiet


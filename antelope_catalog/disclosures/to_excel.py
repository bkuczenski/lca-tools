from pandas import DataFrame, ExcelWriter

from scipy.sparse import coo_matrix, csr_matrix, eye, lil_matrix
from scipy.sparse.linalg import spsolve
from numpy import array


def _make_df(mtx, index):
    df = DataFrame(mtx, index=index)
    df[df == 0] = None
    return df


def _df_to_excel(writer, sheetname, mtx, index):
    df = _make_df(mtx, index)
    df.to_excel(writer, sheet_name=sheetname)


def _x_tilde(fg_list, Af):
    e = eye(len(fg_list)).tocsr()
    bs = [i for i, x in enumerate(fg_list) if x.parent.key is None]
    x_tilde = lil_matrix((len(fg_list), len(bs)))
    A = e - Af
    for i, x in enumerate(bs):
        b = e[:, x]
        x_tilde[:, i] = lil_matrix(spsolve(A, b)).transpose()
    return x_tilde.todense()


def flow_key_name(k):
    return '%s [%s]' % (k[0]._name, k[1])


def bg_key_name(k):
    return '%s %s' % (k[0]._name, k[1].unit())


def to_excel(disclosure, xlsfile):
    xlsw = ExcelWriter(xlsfile)
    i, ii, iii, iv, v, vi = disclosure.generate_disclosure()

    fg = [flow_key_name(t) for t in i]
    bg = [bg_key_name(t) for t in ii]
    em = [flow_key_name(t) for t in iii]

    iv = array(iv)
    v = array(v)
    vi = array(vi)

    Af = coo_matrix((iv[:, 2], (iv[:, 0], iv[:, 1])), shape=(len(i), len(i)))
    Ad = coo_matrix((v[:, 2], (v[:, 0], v[:, 1])))
    Bf = coo_matrix((vi[:, 2], (vi[:, 0], vi[:, 1])))

    _df_to_excel(xlsw, 'foreground', Af.todense(), fg)

    _df_to_excel(xlsw, 'background', Ad.todense(), bg)

    _df_to_excel(xlsw, 'emissions', Bf.todense(), em)

    x_tilde = _x_tilde(disclosure._fg.to_list(), Af)
    _df_to_excel(xlsw, 'ad', Ad * x_tilde, bg)
    _df_to_excel(xlsw, 'bf', Bf * x_tilde, em)

    xlsw.save()

"""
General toolkit for comprehensive comparison of local and remote calrecycle implementations
"""

from antelope_catalog import LcCatalog
from antelope_catalog.data_sources.local import make_config
from math import isclose
import os
import json

CALRECYCLE_CAT = '/data/LCI/CalRecycle'

cat = LcCatalog(CALRECYCLE_CAT)
if 'calrecycle.antelope' not in cat.references:
    cat.new_resource('calrecycle.antelope', 'http://www.antelope-lca.net/uo-lca/api/', 'AntelopeV1Client', store=False,
                      interfaces=['index', 'inventory', 'quantity'], quiet=True)

fg = make_config('calrecycle').foreground(cat)


def _get_local_lcia(lcia_id):
    remote_lcia = cat.query('calrecycle.antelope').get('lciamethods/%d' % lcia_id)
    return cat.query('calrecycle.lcia').get(remote_lcia.uuid)


def validate_antelope(lcia_id, **kwargs):
    lcia_m = _get_local_lcia(lcia_id)
    print('Validating for %s' % lcia_m)
    for i in range(55):
        key = 'fragments/%d' % (i+1)
        res_a = cat.query('calrecycle.antelope').get(key).fragment_lcia('lciamethods/%d' % lcia_id)
        res_l = fg[key].fragment_lcia(lcia_m, 'quell_eg')
        if isclose(res_a.total(), res_l.total(), **kwargs):
            print('valid: %s' % key)
        else:
            ratio = res_a.total() / res_l.total()
            print('\n%s : %.10g\n%s\n%s' % (key, ratio, res_a, res_l))
            yield key, ratio


def _sort_set(s):
    return sorted(s, key=lambda x: int(x.split('/')[1]))


def validate_fragments(*args, rel_tol=1e-8, save=False, **kwargs):
    """
    Supply a list of LCIA method IDs known to the remote antelope interface. This will return a list of fragments
    whose LCIA results match to the specified tolerance for all supplied methods

    if save is True, write the results to disk
    otherwise [default], if a save file exists, check to see whether the newly generated results match the saved ones.
    :param args:
    :param rel_tol: argument to isclose (also pass other kwargs)
    :param save: whether to save the results for later validation
    :return:
    """
    all_frags = set(k.external_ref for k in fg.fragments())
    result = {'methods': sorted(args), 'rel_tol': rel_tol, 'kwargs': kwargs}
    for lcia_id in args:
        fail_dict = {fail: ratio for fail, ratio in validate_antelope(lcia_id, rel_tol=rel_tol, **kwargs)}
        result['fail_%d' % lcia_id] = fail_dict
        all_frags -= set(fail_dict.keys())

    result['pass'] = _sort_set(all_frags)

    fname_suffix = '_'.join(str(k) for k in sorted(args))
    dest_file = os.path.join(os.path.dirname(__file__), 'validation_results_%s.json' % fname_suffix)

    if save:
        with open(dest_file, 'w') as fp:
            json.dump(result, fp, indent=2, sort_keys=True)
    else:
        check_result(dest_file, result)
    return result


def check_result(dest_file, result):
    passing = None
    if os.path.exists(dest_file):
        passing = True
        with open(dest_file, 'r') as fp:
            saved = json.load(fp)
        for k, v in saved.items():
            if isinstance(v, dict):
                chk = set(result[k].keys()) == set(v.keys())
                suf = ' (keys)'
            else:
                chk = result[k] == v
                suf = ''
            passing &= chk
            print('%s: %s%s' % (k, chk, suf))
    else:
        print('No saved result file %s' % dest_file)
    return passing

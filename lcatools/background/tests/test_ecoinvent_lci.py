"""
Ecoinvent LCI testing!

In order to run this test, you need to have a catalog that can resolve ecoinvent references.  Configure this by
setting a value for the parameter CATALOG_ROOT below, just after the imports.  If this is not defined, the tests will
be skipped.

"""

import os
import sys
import unittest
import random

from collections import namedtuple

from math import isclose

from lcatools.catalog import LcCatalog
from lcatools.entities import LcProcess
from lcatools.tools import archive_from_json

from lcatools.data_sources.local import CATALOG_ROOT
from lcatools.providers.lc_archive import LcArchive


cat = LcCatalog(CATALOG_ROOT)
if '-lci' in sys.argv:
    _run_ecoinvent = True
    cat = LcCatalog(CATALOG_ROOT)
else:
    _run_ecoinvent = False
    # cat = None


_debug = True

EcoinventNode = namedtuple('EcoinventNode', ['version', 'model', 'node'])


def lci_cache_file(version, model):
    return os.path.join(cat.archive_dir, 'lci_cache_%s_%s.json.gz' % (version, model))


def inv_cache_file(version, model):
    """
    :param version:
    :param model:
    :return:
    """
    return os.path.join(cat.archive_dir, 'inv_cache_%s_%s.json.gz' % (version, model))


def test_ref(version, model):
    return 'test.lci.ecoinvent.%s.%s' % (version, model)


def find_lci_ref(version, model):
    check_ref = 'ecoinvent.lci.%s.%s' % (version, model)
    if check_ref in cat.references:
        return check_ref
    check_ref = '.'.join(['local', check_ref])
    if check_ref in cat.references:
        return check_ref
    return None


def _extract_and_reduce_lci(node):
    filename = lci_cache_file(node.version, node.model)
    ref = test_ref(node.version, node.model)
    if os.path.exists(filename):
        a = archive_from_json(filename, ref=ref)
    else:
        a = LcArchive(filename, ref=ref)

    if ref not in cat.references:
        cat.add_existing_archive(a, interfaces='inventory', static=True)

    if cat.query(ref).get(node.node) is not None:
        return

    lci_ref = find_lci_ref(node.version, node.model)
    if lci_ref is None:
        print('No LCI resource for (%s, %s)' % (node.version, node.model))
        return

    p_ref = cat.query(lci_ref).get(node.node)

    if p_ref is None:
        print('No process found with reference %s' % node.node)
        return

    p_rx = next(p_ref.references())
    exchs = random.sample([_x for _x in p_ref.inventory(ref_flow=p_rx)], 100)

    p_slim = LcProcess(p_ref.uuid, Name=p_ref['Name'])
    p_slim.add_exchange(p_rx.flow, p_rx.direction, value=p_rx.value)
    p_slim.add_reference(p_rx.flow, p_rx.direction)

    for x in exchs:
        p_slim.add_exchange(x.flow, x.direction, value=x.value)

    a.add_entity_and_children(p_slim)
    a.write_to_file(filename, complete=True, gzip=True)


class EcoinventLciTest(unittest.TestCase):
    _nodes = {
        EcoinventNode('3.2', 'apos', '18085d22-72d0-4588-9c69-7dbeb24f8e2f'),
        EcoinventNode('3.2', 'conseq', None)
    }

    @classmethod
    def setUpClass(cls):
        if _run_ecoinvent:
            for node in cls._nodes:
                if node.node is None:
                    continue
                ref = test_ref(node.version, node.model)
                if ref in cat.references:
                    if cat.query(ref).get(node.node) is not None:
                        continue
                # if ref is not in cat, or if node is not present, then we need to populate node
                _extract_and_reduce_lci(node)

    def setUp(self):
        if not _run_ecoinvent:
            self.skipTest('Ecoinvent not setup')

    def test_lci(self):
        for node in self._nodes:
            if node.node is None:
                continue
            lci_result = cat.query(test_ref(node.version, node.model)).get(node.node)
            challenge = cat.query('local.ecoinvent.%s.%s' % (node.version, node.model), debug=_debug).get(node.node)

            c_lci = challenge.lci(ref_flow=lci_result.reference().flow)
            lci_check = {x.key: x for x in c_lci}

            count = 0
            inverted = []
            fail = []

            for i in lci_result.inventory():
                count += 1
                z = lci_check[i.key]
                if isclose(i.value, z.value):
                    continue
                elif isclose(i.value, -z.value):
                    inverted.append(i)
                else:
                    fail.append(i)

            print('Total %d, inverted %d, fail %d' % (count, len(inverted), len(fail)))



"""
        for v, m in ec.valid_lci():
            if not os.path.exists(lci_cache_file(v, m)):
                continue
            lci_a = archive_from_json(lci_cache_file(v, m))
            p = next(lci_a.entities_by_type('process'))

            q = cat.query(ref_(v, m, 'inventory')).lci(p.external_ref)



true_dirn = [_x for _x in bm.foreground(my_p)][0].direction
if my_p.reference().direction != true_dirn:
    mult = -1
else:
    mult = 1

fail = []
for i in lci:
    try:
        ic = inv_check_d[i.key]
    except KeyError:
        continue
    if not isclose(i.value, mult*ic.value, rel_tol=1e-7):
        print('Not close! %s\nInv: %12g Chk: %12g ratio: %.10f\n' % (i.flow, i.value, ic.value, i.value / ic.value))
        fail.append(i)
print('Failed: %d' % len(fail))


"""








if __name__ == '__main__':
    unittest.main()

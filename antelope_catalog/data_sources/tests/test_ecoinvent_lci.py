"""
Ecoinvent LCI testing!

In order to run this test, you need to have a catalog that can resolve ecoinvent references.  Configure this by
setting a value for the parameter CATALOG_ROOT below, just after the imports.  If this is not defined, the tests will
be skipped.

"""

import os
import unittest
import random

from collections import namedtuple

from math import isclose

from ... import LcCatalog
from lcatools.entities import LcProcess
from lcatools.providers import archive_from_json

from lcatools.interfaces import EntityNotFound

from ..local import CATALOG_ROOT, check_enabled
from lcatools.implementations import LcArchive

EcoinventNode = namedtuple('EcoinventNode', ['version', 'model', 'node'])
_debug = True


if __name__ == '__main__':
    _run_ecoinvent = check_enabled('ecoinvent')
else:
    _run_ecoinvent = check_enabled('ecoinvent') or _debug


if _run_ecoinvent:
    cat = LcCatalog(CATALOG_ROOT)
else:
    cat = None


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
    """
    This function performs the (surpassingly slow) task of extracting reduced aggregated LCI results from the large
    bundled ecoinvent 7z LCI archives.  (The operation is slow on RAM-limited machines because the 7z algorithm requires
    tremendous memory)

    The routine requires the process inventory test case to be selected manually.

    The selected inventory is loaded, and then one hundred exchanges are selected at random and the rest are removed.
    This reduces the file size (and load time) of the generated archives without sacrificing the representativeness of
    the computation.

    :param node:
    :return:
    """
    filename = lci_cache_file(node.version, node.model)
    ref = test_ref(node.version, node.model)
    if os.path.exists(filename):
        a = archive_from_json(filename, ref=ref)
    else:
        a = LcArchive(filename, ref=ref)

    if ref not in cat.references:
        cat.add_existing_archive(a, interfaces='inventory', static=True)

    try:
        cat.query(ref).get(node.node)
        return
    except EntityNotFound:
        pass

    cat.get_resource(ref, 'inventory').remove_archive()

    lci_ref = find_lci_ref(node.version, node.model)
    if lci_ref is None:
        print('No LCI resource for (%s, %s)' % (node.version, node.model))
        return

    print('WARNING: extracting and reducing LCI data can be very slow >60s per file')

    p_ref = cat.query(lci_ref).get(node.node)

    if p_ref is None:
        print('No process found with reference %s' % node.node)
        return

    p_rx = next(p_ref.references())
    exchs = random.sample([_x for _x in p_ref.inventory(ref_flow=p_rx)], 100)

    p_slim = LcProcess(p_ref.uuid, Name=p_ref['Name'])
    p_slim.add_exchange(p_rx.flow, p_rx.direction, value=p_ref.reference_value(p_rx.flow))
    p_slim.add_reference(p_rx.flow, p_rx.direction)

    for x in exchs:
        p_slim.add_exchange(x.flow, x.direction, value=x.value, termination=x.termination)

    a.add_entity_and_children(p_slim)
    a.write_to_file(filename, complete=True, gzip=True)


class EcoinventLciTest(unittest.TestCase):
    _nodes = {
        EcoinventNode('3.2', 'apos', '18085d22-72d0-4588-9c69-7dbeb24f8e2f'),
        # EcoinventNode('3.2', 'apos', 'ca4a6d8a-2399-4645-ac20-17343c694f2b'),  # potato seed, for setting- fg scc member
        EcoinventNode('3.2', 'conseq', '6b0f32fe-329d-4c1f-9205-0ea78f4f42e5')
    }

    @classmethod
    def nodes(cls):
        for node in cls._nodes:
            if node.node is None:
                continue
            yield node

    @classmethod
    def setUpClass(cls):
        if _run_ecoinvent:
            for node in cls.nodes():
                if node.node is None:
                    continue
                ref = test_ref(node.version, node.model)
                if ref in cat.references:
                    try:
                        cat.query(ref).get(node.node)
                    except EntityNotFound:
                        _extract_and_reduce_lci(node)
                    continue
                # if ref is not in cat, or if node is not present, then we need to populate node
                _extract_and_reduce_lci(node)

    def setUp(self):
        if not _run_ecoinvent:
            self.skipTest('Ecoinvent not setup')

    def test_lci(self):
        for node in self.nodes():
            lci_result = cat.query(test_ref(node.version, node.model)).get(node.node)
            rx = lci_result.reference()
            challenge = cat.query('local.ecoinvent.%s.%s' % (node.version, node.model), debug=False).get(node.node)

            c_lci = challenge.lci(ref_flow=rx.flow.external_ref, threshold=1e-10)
            lci_check = {x.key: x for x in c_lci}

            count = 0
            inverted = []
            fail = []

            for i in lci_result.inventory(rx):  # inventory method normalizes in ExchangeValue.from_allocated
                count += 1
                z = lci_check[i.key]
                if isclose(i.value, z.value, rel_tol=1e-6):
                    continue
                elif isclose(i.value, -z.value, rel_tol=1e-6):
                    inverted.append(i)
                else:
                    fail.append(i)
                    print('Not close! %s\nInv: %12g Chk: %12g ratio: %.10f\n' % (i.flow, i.value, z.value,
                                                                                 i.value / z.value))
            self.assertTrue(len(fail) < 2)
            self.assertEqual(len(inverted), 0)

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
                print('Not close! %s\nInv: %12g Chk: %12g ratio: %.10f\n' % (i.flow, i.value, ic.value, 
                                                                             i.value / ic.value))
                fail.append(i)
        
        print('Failed: %d' % len(fail))


"""

if __name__ == '__main__':
    unittest.main()

import unittest

from ... import LcCatalog

from ..local import CATALOG_ROOT, RESOURCES_CONFIG, check_enabled
from ..calrecycle_lca import CalRecycleConfig

_debug = True

if __name__ == '__main__':
    _run_calrecycle = check_enabled('calrecycle')
else:
    _run_calrecycle = check_enabled('calrecycle') or _debug


if _run_calrecycle:
    cat = LcCatalog(CATALOG_ROOT)
else:
    cat = None


class CalRecycleTest(unittest.TestCase):

    def setUp(self):
        if not _run_calrecycle:
            self.skipTest('Calrecycle test not enabled')

    @classmethod
    def setUpClass(cls):
        crc = CalRecycleConfig(RESOURCES_CONFIG['calrecycle']['data_root'])
        cls.fg = crc.foreground(cat)

    def test_create_and_load(self):
        self.assertEqual(self.fg.count_by_type('fragment'), 888)

    def test_named_fragment(self):
        self.assertEqual(self.fg['fragments/43']['Name'], 'Fuels Displacement Mixer - distillate')


if __name__ == '__main__':
    unittest.main()

"""
Archive testing is divided up loosely into pre-entity (e.g. test_entity_store and test_base) and post-entity testing
(e.g. this file).  Of course test_base has entities but they are more for the purpose of validating name setting.

This class imports a minimal archive file created in antelope_utilities to contain a multioutput, nontrivial process
(USLCI petroleum refining) and an intermediate process (a grid mix).  These don't do anything special but the refinery
process can test both exchange generation / allocation and lcia.

Subclass this to access that archive.  Current uses:
"""

import unittest
import os

from lcatools.archives import LcArchive


refinery_archive = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_archive.json')
grid_id = '96bffbb9-b875-36cf-8a11-5723c9d239d9'
petro_id = '0aaf1e13-5d80-37f9-b7bb-81a6b8965c71'


class BasicEntityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.A = LcArchive.from_file(refinery_archive)
        cls.grid = cls.A[grid_id]
        cls.petro = cls.A[petro_id]

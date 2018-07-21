import unittest
import os

from lcatools.from_json import from_json
from lcatools.providers import LcArchive


refinery_archive = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_archive.json')
grid_id = '96bffbb9-b875-36cf-8a11-5723c9d239d9'
petro_id = '0aaf1e13-5d80-37f9-b7bb-81a6b8965c71'


class BasicEntityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.A = LcArchive.from_dict(from_json(refinery_archive))
        cls.grid = cls.A[grid_id]
        cls.petro = cls.A[petro_id]

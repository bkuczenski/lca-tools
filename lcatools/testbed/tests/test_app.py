import unittest
from lcatools.testbed import app_factory


class TestBedTest(unittest.TestCase):
    def setUp(self):
        my_app = app_factory()
        self.app = my_app.test_client()
        self.app.testing = True

    def test_helloworld(self):
        rv = self.app.get('/')
        self.assertEqual(rv.data, b'Hello world!')


if __name__ == '__main__':
    unittest.main()

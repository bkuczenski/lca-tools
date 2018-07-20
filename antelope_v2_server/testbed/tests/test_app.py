import unittest
from ...testbed import app_factory


# @unittest.skip('Flask testbed is under construction')
class TestBedTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        my_app = app_factory()
        cls.app = my_app.test_client()
        cls.app.testing = True

    def test_helloworld(self):
        rv = self.app.get('/')
        self.assertEqual(rv.data, b'Hello world!')


if __name__ == '__main__':
    unittest.main()

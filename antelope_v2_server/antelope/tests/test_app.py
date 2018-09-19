import unittest
from ..server import app


# @unittest.skip('Flask testbed is under construction')
class TestBedTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = app.test_client()
        cls.app.testing = True

    def test_helloworld(self):
        rv = self.app.get('/hello')
        self.assertEqual(rv.data, b'Hello world!')


if __name__ == '__main__':
    unittest.main()

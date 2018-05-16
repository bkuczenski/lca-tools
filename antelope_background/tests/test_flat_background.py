import unittest
import os
from tempfile import TemporaryDirectory

from antelope_background.flat_background import FlatBackground, TermRef

#  flow_ref, direction, term_ref, scc
term_test = (('an_arbitrary_external_ref', 0, 'a_different_ref', None),
             ('zzxxcc00', 1, 'aabbcc11', 0),
             ('zzxxcc11', 'Input', 'aabbcc22', 'a_strongly_connected_component'))


class FlatBackgroundTestCase(unittest.TestCase):
    def test_create_terms(self):
        fb = FlatBackground(term_test, [], [], None, None, None)
        self.assertEqual(fb.pdim, len(term_test))
        self.assertEqual(fb.fg[0].term_ref, term_test[0][2])
        self.assertEqual(fb.fg[1].direction, 'Output')
        self.assertEqual(fb.fg[1].scc_id, [])
        self.assertEqual(fb.fg[2].scc_id, term_test[2][3])

    def test_serialize_terms(self):
        fg = [TermRef(*x) for x in term_test]
        self.assertTupleEqual(tuple(fg[1]), (term_test[1]))

    def test_index_terms(self):
        fb = FlatBackground(term_test, [], [], None, None, None)
        for i, fg in enumerate(fb.fg):
            self.assertIs(fb.index_of(fg.term_ref, fg.flow_ref), i)

    def test_store_term_mat(self):
        fb = FlatBackground(term_test, [], [], None, None, None)
        with TemporaryDirectory() as tmpdir:
            fname = os.path.join(tmpdir, 'test.mat')
            fb.write_to_file(fname)
            fb_load = FlatBackground.from_file(fname)
            for fg in fb.fg:
                index = fb_load.index_of(fg.term_ref, fg.flow_ref)
                self.assertTupleEqual(tuple(fb_load.fg[index]), tuple(fg))


if __name__ == '__main__':
    unittest.main()

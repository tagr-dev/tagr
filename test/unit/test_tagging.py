import unittest

from tagr.tagging import Tagr

class TaggingTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.tag = Tagr()
        super().__init__(*args, **kwargs)

    def test_artfact_is_returned(self):
        test_artifact = 'foo'
        res = self.tag.save(test_artifact, 'str')
        self.assertEqual(res, 'foo')
        
import unittest

from tagr.tagging.artifacts import Tagr

class TaggingTest(unittest.TestCase):
    def __init__(self):
        self.tag = Tagr()

    def test_artfact_is_returned(self):
        test_artifact = 'foo'
        res = self.tag.save(test_artifact, 'str')
        self.assertEqual(res, 'foo')

import unittest
import pandas as pd

from tagr.tagging.artifacts import Artifact

DATA = [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]
DF = pd.DataFrame(DATA)


class ArtifactTest(unittest.TestCase):
    def test_attributes(self):
        test_artifact = "foo"
        artifact_name = "str1"
        artifact_dtype = "primitive"
        artifact = Artifact(test_artifact, artifact_name, artifact_dtype)

        self.assertEqual(artifact.val, test_artifact)
        self.assertEqual(artifact.obj_name, artifact_name)
        self.assertEqual(artifact.dtype, artifact_dtype)

    def test_attributes_no_dtype(self):
        test_artifact = DF
        artifact_name = "X_train"

        artifact = Artifact(test_artifact, artifact_name)
        pd._testing.assert_frame_equal(artifact.val, test_artifact)
        self.assertEqual(artifact.obj_name, artifact_name)
        self.assertEqual(artifact.dtype, "dataframe")

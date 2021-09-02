import unittest
import pandas as pd

from tagr.tagging.artifacts import Artifact

DATA = [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]
DF = pd.DataFrame(DATA)


class ArtifactTest(unittest.TestCase):
    def test_attributes(self):
        # Arrange
        test_artifact = "foo"
        artifact_name = "str1"
        artifact_dtype = "primitive"

        # Act
        artifact = Artifact(test_artifact, artifact_name, artifact_dtype)

        # Assert
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

    def test_evaluate_type(self):
        self.assertRaises(TypeError, lambda: Artifact.evaluate_type(1, float))

    def test_check_expected_type(self):
        test_artifact = "foo"
        
        self.assertRaises(TypeError, lambda: Artifact(test_artifact, "X_train"))
        self.assertRaises(
            TypeError, lambda: Artifact(test_artifact, "mistyped_obj", "dataframe")
        )

    def test_is_recognized_dtype(self):
        self.assertRaises(
            ValueError, lambda: Artifact.is_recognized_dtypes("primitiveS")
        )

        test_artifact = "foo"
        self.assertRaises(
            ValueError,
            lambda: Artifact(test_artifact, "unrecognized_dtype_obj", "dataframeS"),
        )

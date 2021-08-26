import unittest
import pandas as pd

from tagr.tagging import Tagr


class TaggingTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.tag = Tagr()
        super().__init__(*args, **kwargs)

    def test_artfact_is_returned(self):
        test_artifact = "foo"
        res = self.tag.save(test_artifact, "str1", "primitive")
        self.assertEqual(res, "foo")

    def test_save_other(self):
        test_artifact = {"test_key": "test_val"}
        res = self.tag.save(test_artifact, "dict1", "other")
        self.assertEqual(res, {"test_key": "test_val"})

    def test_save_with_no_dtype(self):
        test_artifact = "foo"
        self.assertRaises(
            ValueError, lambda: self.tag.save(test_artifact, "unrecognized_obj")
        )

    def test_save_with_unrecognized_dtype(self):
        test_artifact = "foo"
        self.assertRaises(
            ValueError,
            lambda: self.tag.save(test_artifact, "misnamed_dtype", "primitiveS"),
        )
        queue_length = len(self.tag.ret_queue())
        self.assertEqual(queue_length, 0)

    def test_save_with_wrong_dtype(self):
        test_artifact = "foo"
        self.assertRaises(TypeError, lambda: self.tag.save(test_artifact, "X_train"))

    def test_obj_name_to_dtype_conversion(self):
        data = [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]
        df = pd.DataFrame(data)
        self.tag.save(df, "X_train")
        df_dtype = self.tag.ret_queue()["X_train"].dtype
        self.assertEqual(df_dtype, "dataframe")

        # test aliasing
        test_int = 2
        self.tag.save(test_int, "num", "primitive")
        int_dtype = self.tag.ret_queue()["num"].dtype
        self.assertEqual(int_dtype, "primitive")

    def test_ignore_dupes(self):
        self.tag = Tagr()
        self.tag.save(2, "num", "primitive")
        self.tag.save(2, "num", "primitive")
        queue_length = len(self.tag.ret_queue())
        self.assertEqual(queue_length, 1)

    def test_summary(self):
        data = [
            {"obj_name": "num1", "val": 2.0, "dtype": "primitive"},
            {"obj_name": "str1", "val": "foo", "dtype": "primitive"},
        ]
        expected_df = pd.DataFrame(data)

        self.tag = Tagr()
        self.tag.save(2, "num1", "primitive")
        self.tag.save("foo", "str1", "primitive")
        summary = self.tag.summary()
        pd._testing.assert_frame_equal(summary, expected_df)

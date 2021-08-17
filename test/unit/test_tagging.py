import unittest
import pandas as pd

from tagr.tagging import Tagr


class TaggingTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.tag = Tagr()
        super().__init__(*args, **kwargs)

    def test_artfact_is_returned(self):
        test_artifact = "foo"
        res = self.tag.save(test_artifact, "str")
        self.assertEqual(res, "foo")

    def test_save_with_no_dtype(self):
        test_artifact = "foo"
        self.assertRaises(
            ValueError, lambda: self.tag.save(test_artifact, "unrecognized_obj")
        )

    def test_obj_name_to_dtype_conversion(self):
        data = [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]
        self.tag.save(data, "X_train")
        df_dtype = self.tag.ret_queue()["X_train"].dtype
        self.assertEqual(df_dtype, "dataframe")

        # test aliasing
        test_int = 2
        self.tag.save(test_int, "num", "int")
        int_dtype = self.tag.ret_queue()["num"].dtype
        self.assertEqual(int_dtype, "primitive")

    def test_ignore_dupes(self):
        self.tag = Tagr()
        self.tag.save(2, "num", "int")
        self.tag.save(2, "num", "int")
        queue_length = len(self.tag.ret_queue())
        self.assertEqual(queue_length, 1)

    def test_summary(self):
        data = [
            {"obj_name": "num1", "val": 2.0, "dtype": "primitive"},
            {"obj_name": "str1", "val": "foo", "dtype": "primitive"},
        ]
        expected_df = pd.DataFrame(data)

        self.tag = Tagr()
        self.tag.save(2, "num1", "int")
        self.tag.save("foo", "str1", "float")
        summary = self.tag.summary()
        pd._testing.assert_frame_equal(summary, expected_df)

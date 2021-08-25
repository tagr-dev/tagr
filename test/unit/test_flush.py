import unittest
import json
import pickle
from moto import mock_s3
import boto3
import pandas as pd

from tagr.tagging import Tagr
from tagr.storage.aws import Aws

DATA = [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]
DF = pd.DataFrame(DATA)

EXPERIMENT_PARAMS = {
    "proj": "unit-test-project",
    "experiment": "unit_test_expr",
    "tag": "unit_test_tag",
}


@mock_s3
class FlushTest(unittest.TestCase):
    maxDiff = None

    def __init__(self, *args, **kwargs):
        self.tag = Tagr()
        self.tag.storage_provider = Aws()
        super().__init__(*args, **kwargs)

    @staticmethod
    def create_connection():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=EXPERIMENT_PARAMS["proj"])
        return conn

    def test_get_primitive_objs_dict(self):
        expected_result = {"str1": "a", "int1": 1, "float1": 2.0}

        self.tag.save("a", "str1", "primitive")
        self.tag.save(1, "int1", "primitive")
        self.tag.save(2.0, "float1", "primitive")
        self.tag.save(DF, "df1", "dataframe")
        summary = self.tag.summary()
        primitive_objs_dict = self.tag._get_primitive_objs_dict(summary)

        self.assertEqual(expected_result, primitive_objs_dict)

    def test_flush_metadata_json(self):
        expected_result = {
            "types": {"df1": dict(zip(DF.columns, DF.dtypes.map(lambda x: x.name)))},
            "stats": {"df1": DF.describe().to_dict()},
            "primitive_objs": {"float1": 2.0, "int1": 1, "str1": "a"},
        }

        conn = self.create_connection()

        self.tag.save("a", "str1", "primitive")
        self.tag.save(1, "int1", "primitive")
        self.tag.save(2.0, "float1", "primitive")
        self.tag.save(DF, "df1", "dataframe")

        summary = self.tag.summary()
        # flush dfs to gen metadata
        self.tag._flush_dfs(summary, EXPERIMENT_PARAMS)
        self.tag._flush_metadata_json(summary, EXPERIMENT_PARAMS)

        res = (
            conn.Object(
                EXPERIMENT_PARAMS["proj"],
                "{}/{}/df_summary.json".format(
                    EXPERIMENT_PARAMS["experiment"], EXPERIMENT_PARAMS["tag"]
                ),
            )
            .get()["Body"]
            .read()
            .decode("utf-8")
        )
        json_content = json.loads(res)
        self.assertEqual(expected_result, json_content)

    def test_flush_dfs(self):
        expected_result = DF

        conn = self.create_connection()

        self.tag.save(DF, "df1", "dataframe")

        summary = self.tag.summary()
        self.tag._flush_dfs(summary, EXPERIMENT_PARAMS)

        res = conn.Object(
            EXPERIMENT_PARAMS["proj"],
            "{}/{}/df1.csv".format(
                EXPERIMENT_PARAMS["experiment"], EXPERIMENT_PARAMS["tag"]
            ),
        ).get()["Body"]
        df_content = pd.read_csv(res, index_col=0)
        pd._testing.assert_frame_equal(expected_result, df_content)

    def test_flush_non_dfs(self):
        expected_result_str = "a"
        expected_result_int = 1

        conn = self.create_connection()

        self.tag.save("a", "str1", "primitive")
        self.tag.save(1, "int1", "primitive")

        summary = self.tag.summary()
        self.tag._flush_non_dfs(summary, EXPERIMENT_PARAMS)

        str_file = (
            conn.Object(
                EXPERIMENT_PARAMS["proj"],
                "{}/{}/str1.pkl".format(
                    EXPERIMENT_PARAMS["experiment"], EXPERIMENT_PARAMS["tag"]
                ),
            )
            .get()["Body"]
            .read()
        )
        str_content = pickle.loads(str_file)

        int_file = (
            conn.Object(
                EXPERIMENT_PARAMS["proj"],
                "{}/{}/int1.pkl".format(
                    EXPERIMENT_PARAMS["experiment"], EXPERIMENT_PARAMS["tag"]
                ),
            )
            .get()["Body"]
            .read()
        )
        int_content = pickle.loads(int_file)

        self.assertEqual(expected_result_str, str_content)
        self.assertEqual(expected_result_int, int_content)

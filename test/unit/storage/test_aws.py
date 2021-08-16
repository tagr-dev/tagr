import unittest
import json
import pickle
from moto import mock_s3
import boto3
import pandas as pd

from tagr.storage.aws import Aws


DATA = [{"a": 1, "b": 2, "c": 3}, {"a": 10, "b": 20, "c": 30}]
DF = pd.DataFrame(DATA)

PROJ = "unit-test-project"
EXPERIMENT = "unit_test_expr"
TAG = "unit_test_tag"

DF_METADATA = {
    "types": {"a": 1},
    "stats": {"b": 2},
    "primitive_objs": {"c": 3},
}


# TODO: update tests to use fetch so one test can test if we serialize and deserialize correctly
@mock_s3
class AwsTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.storage_provider = Aws()
        super().__init__(*args, **kwargs)

    @staticmethod
    def create_connection():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=PROJ)
        return conn

    def test_dump_json(self):
        expected_result = DF_METADATA

        conn = self.create_connection()

        self.storage_provider.dump_json(
            df_metadata=DF_METADATA, proj=PROJ, experiment=EXPERIMENT, tag=TAG
        )

        res = (
            conn.Object(PROJ, "{}/{}/df_summary.json".format(EXPERIMENT, TAG))
            .get()["Body"]
            .read()
            .decode("utf-8")
        )
        json_content = json.loads(res)
        self.assertEqual(expected_result, json_content)

    def test_csv(self):
        expected_result = DF

        file_name = "test_df"
        conn = self.create_connection()

        self.storage_provider.dump_csv(
            df=DF, proj=PROJ, experiment=EXPERIMENT, tag=TAG, filename=file_name
        )

        res = conn.Object(
            PROJ, "{}/{}/{}.csv".format(EXPERIMENT, TAG, file_name)
        ).get()["Body"]
        df_content = pd.read_csv(res, index_col=0)
        pd._testing.assert_frame_equal(expected_result, df_content)

    def test_pickle(self):
        expected_result = {"test_key": "test_val"}

        test_obj = {"test_key": "test_val"}
        file_name = "test_dict"
        conn = self.create_connection()

        self.storage_provider.dump_pickle(
            model=test_obj,
            proj=PROJ,
            experiment=EXPERIMENT,
            tag=TAG,
            filename=file_name,
        )

        res = (
            conn.Object(PROJ, "{}/{}/{}.pkl".format(EXPERIMENT, TAG, file_name))
            .get()["Body"]
            .read()
        )
        obj_content = pickle.loads(res)
        self.assertEqual(expected_result, obj_content)

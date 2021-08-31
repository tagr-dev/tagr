import unittest
import json
import pickle

import boto3
import pandas as pd

from moto import mock_s3
from tagr.storage.aws import Aws, AwsHelper


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

    @staticmethod
    def clear_directory(conn):
        bucket = conn.Bucket(PROJ)
        bucket.objects.filter(Prefix="{}/{}/".format(EXPERIMENT, TAG)).delete()

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

    def test_dump_csv(self):
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

    def test_dump_pickle(self):
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

    def test_Tagr__list(self):
        expected_paths = [
            "{}/{}/unit_test_tag/text1.txt".format(PROJ, EXPERIMENT),
            "{}/{}/unit_test_tag/text2.txt".format(PROJ, EXPERIMENT),
        ]

        conn = self.create_connection()
        self.clear_directory(conn)

        conn.Object(PROJ, "{}/{}/text1.txt".format(EXPERIMENT, TAG)).put(
            Body="content_of_text1"
        )
        conn.Object(PROJ, "{}/{}/text2.txt".format(EXPERIMENT, TAG)).put(
            Body="content_of_text2"
        )

        paths_list = self.storage_provider._Tagr__list(
            proj=PROJ, experiment=EXPERIMENT, tag=TAG
        )
        self.assertEqual(expected_paths, paths_list)

    def test_Tagr__fetch(self):
        expected_df = DF
        expected_dict = {"test_key": "test_val"}

        conn = self.create_connection()
        self.clear_directory(conn)

        self.storage_provider.dump_csv(
            df=DF, proj=PROJ, experiment=EXPERIMENT, tag=TAG, filename="test_data"
        )
        df_content = self.storage_provider._Tagr__fetch(
            proj=PROJ, experiment=EXPERIMENT, tag=TAG, filename="test_data.csv"
        )
        pd._testing.assert_frame_equal(expected_df, df_content)

        self.storage_provider.dump_pickle(
            model=expected_dict,
            proj=PROJ,
            experiment=EXPERIMENT,
            tag=TAG,
            filename="test_dict",
        )
        pickle_content = self.storage_provider._Tagr__fetch(
            proj=PROJ, experiment=EXPERIMENT, tag=TAG, filename="test_dict.pkl"
        )
        self.assertEqual(expected_dict, pickle_content)


@mock_s3
class AwsHelperTest(unittest.TestCase):
    @staticmethod
    def create_connection():
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=PROJ)
        return conn

    def test_get_matching_s3_objects(self):
        expected_key = [
            "unit_test_expr/unit_test_tag/text1.txt",
            "unit_test_expr/unit_test_tag/text2.txt",
        ]

        aws_helper = AwsHelper()
        conn = self.create_connection()

        conn.Object(PROJ, "{}/{}/text1.txt".format(EXPERIMENT, TAG)).put(
            Body="content_of_text1"
        )
        conn.Object(PROJ, "{}/{}/text2.txt".format(EXPERIMENT, TAG)).put(
            Body="content_of_text2"
        )

        matching_objs_res = aws_helper.get_matching_s3_objects(
            bucket=PROJ, object_path="{}/{}".format(EXPERIMENT, TAG)
        )
        objs_list = [obj["Key"] for obj in matching_objs_res]
        self.assertEqual(expected_key, objs_list)

        matching_keys_res = aws_helper.get_matching_s3_keys(
            bucket=PROJ, object_path="{}/{}".format(EXPERIMENT, TAG)
        )
        keys_list = [key for key in matching_keys_res]
        self.assertEqual(expected_key, keys_list)

    def test_get_list_of_tables(self):
        expected_res = [
            "{}/{}/unit_test_tag/text1.txt".format(PROJ, EXPERIMENT),
            "{}/{}/unit_test_tag/text2.txt".format(PROJ, EXPERIMENT),
        ]

        aws_helper = AwsHelper()
        conn = self.create_connection()

        conn.Object(PROJ, "{}/{}/text1.txt".format(EXPERIMENT, TAG)).put(
            Body="content_of_text1"
        )
        conn.Object(PROJ, "{}/{}/text2.txt".format(EXPERIMENT, TAG)).put(
            Body="content_of_text2"
        )

        list_of_tables = aws_helper.get_list_of_tables(
            bucket=PROJ, object_path="{}/{}".format(EXPERIMENT, TAG)
        )
        self.assertEqual(expected_res, list_of_tables)

    def test_get_object(self):
        expected_df = DF
        expected_dict = {"test_key": "test_val"}

        storage_provider = Aws()
        aws_helper = AwsHelper()

        storage_provider.dump_csv(
            df=DF, proj=PROJ, experiment=EXPERIMENT, tag=TAG, filename="test_data"
        )
        df_content = aws_helper.get_object(
            bucket=PROJ, object_path="{}/{}/test_data.csv".format(EXPERIMENT, TAG)
        )
        pd._testing.assert_frame_equal(expected_df, df_content)

        storage_provider.dump_pickle(
            model=expected_dict,
            proj=PROJ,
            experiment=EXPERIMENT,
            tag=TAG,
            filename="test_dict",
        )
        pickle_content = aws_helper.get_object(
            bucket=PROJ, object_path="{}/{}/test_dict.pkl".format(EXPERIMENT, TAG)
        )
        self.assertEqual(expected_dict, pickle_content)

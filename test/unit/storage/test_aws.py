import unittest
import json
from moto import mock_s3
import boto3

from tagr.storage.aws import Aws


PROJ = "unit-test-project"
EXPERIMENT = "unit_test_expr"
TAG = "unit_test_tag"

DF_METADATA = {
    "types": {"a": 1},
    "stats": {"b": 2},
    "primitive_objs": {"c": 3},
}


@mock_s3
class AwsTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.storage_provider = Aws()
        super().__init__(*args, **kwargs)

    def test_dump_json(self):
        expected_res = DF_METADATA

        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=PROJ)

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
        self.assertEqual(expected_res, json_content)

import unittest
import json
import pickle

from unittest.mock import patch, Mock

import pandas as pd

from tagr.utils import NpEncoder
from tagr.storage.gcp import Gcp

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


class GcpTest(unittest.TestCase):
    @patch("tagr.storage.gcp.storage")
    def test_dump_json(self, mock_storage):
        # Arrange
        payload = bytes(json.dumps(DF_METADATA, cls=NpEncoder).encode("UTF-8"))
        mock_gcs_client = mock_storage.Client.return_value
        mock_bucket = Mock()
        mock_gcs_client.get_bucket.return_value = mock_bucket
        gcp = Gcp()

        # Act
        gcp.dump_json(
            df_metadata=DF_METADATA, proj=PROJ, experiment=EXPERIMENT, tag=TAG
        )

        # Assert
        mock_storage.Client.assert_called_once()
        mock_bucket.blob.return_value.upload_from_string.assert_called_once_with(
            payload
        )

    @patch("tagr.storage.gcp.storage")
    def test_dump_csv(self, mock_storage):
        file_name = "test_df"
        mock_gcs_client = mock_storage.Client.return_value
        mock_bucket = Mock()
        mock_gcs_client.get_bucket.return_value = mock_bucket
        gcp = Gcp()

        gcp.dump_csv(
            df=DF, proj=PROJ, experiment=EXPERIMENT, tag=TAG, filename=file_name
        )

        mock_storage.Client.assert_called_once()
        mock_bucket.blob.return_value.upload_from_string.assert_called_once()

    @patch("tagr.storage.gcp.storage")
    def test_dump_pickle(self, mock_storage):
        payload = pickle.dumps(DATA)
        file_name = "test_list"
        mock_gcs_client = mock_storage.Client.return_value
        mock_bucket = Mock()
        mock_gcs_client.get_bucket.return_value = mock_bucket
        gcp = Gcp()

        gcp.dump_pickle(
            model=DATA, proj=PROJ, experiment=EXPERIMENT, tag=TAG, filename=file_name
        )

        mock_storage.Client.assert_called_once()
        mock_bucket.blob.return_value.upload_from_string.assert_called_once_with(
            payload
        )

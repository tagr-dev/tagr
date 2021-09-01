import unittest
import json
import pickle

import pandas as pd

from unittest.mock import patch, Mock

from tagr.utils import NpEncoder
from tagr.storage.azure import Azure

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


class AzureTest(unittest.TestCase):
    @patch("tagr.storage.azure.BlobServiceClient")
    def test_dump_json(self, mock_service_client):
        payload_name = "{}/{}/df_summary.json".format(EXPERIMENT, TAG)
        payload_data = bytes(json.dumps(DF_METADATA, cls=NpEncoder).encode("UTF-8"))

        mock_azure_client = mock_service_client.return_value
        mock_container = Mock()
        mock_azure_client.get_container_client.return_value = mock_container
        azure = Azure()
        azure.dump_json(
            df_metadata=DF_METADATA, proj=PROJ, experiment=EXPERIMENT, tag=TAG
        )
        mock_service_client.assert_called_once()
        mock_container.upload_blob.assert_called_once_with(
            name=payload_name, data=payload_data
        )

    @patch("tagr.storage.azure.BlobServiceClient")
    def test_dump_csv(self, mock_service_client):
        file_name = "test_df"

        mock_azure_client = mock_service_client.return_value
        mock_container = Mock()
        mock_azure_client.get_container_client.return_value = mock_container
        azure = Azure()
        azure.dump_csv(
            df=DF, proj=PROJ, experiment=EXPERIMENT, tag=TAG, filename=file_name
        )
        mock_service_client.assert_called_once()
        mock_container.upload_blob.assert_called_once()

    @patch("tagr.storage.azure.BlobServiceClient")
    def test_dump_pickle(self, mock_service_client):
        file_name = "test_list"
        payload_name = "{}/{}/{}.pkl".format(EXPERIMENT, TAG, file_name)
        payload_data = pickle.dumps(DATA)

        mock_azure_client = mock_service_client.return_value
        mock_container = Mock()
        mock_azure_client.get_container_client.return_value = mock_container
        azure = Azure()
        azure.dump_pickle(
            model=DATA, proj=PROJ, experiment=EXPERIMENT, tag=TAG, filename=file_name
        )
        mock_service_client.assert_called_once()
        mock_container.upload_blob.assert_called_once_with(
            name=payload_name, data=payload_data
        )

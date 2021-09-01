"""
Azure Blob class implementation
"""

import os
import json
import pickle

from azure.storage.blob import BlobServiceClient

from io import StringIO
from tagr.utils import NpEncoder

AZURE_STORAGE_KEY = os.environ["AZURE_STORAGE_KEY"]
AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]


class Azure:
    def __init__(self):
        self.client = BlobServiceClient(
            account_url="https://{}.blob.core.windows.net".format(
                AZURE_STORAGE_ACCOUNT
            ),
            credential=AZURE_STORAGE_KEY,
        )
        self.name = "Azure"

    def dump_csv(self, df, proj, experiment, tag, filename):
        """
        turns dataframe into csv and saves it to Azure Storage Account directory

        Parameters
        ----------
        df: dataframe object
        proj: project name on metadata provider
        exp: experiment name
        tag: custom commit message
        filename: filename to be exported
        """
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        container = self.client.get_container_client(proj)
        container.upload_blob(
            name="{}/{}/{}.csv".format(experiment, tag, filename),
            data=csv_buffer.getvalue(),
        )

    def dump_json(self, df_metadata, proj, experiment, tag):
        """
        turns dictionary into json and saves it to Azure Storage Account directory

        Parameters
        ----------
        df_metadata: json object containing experiment metadata
        proj: project name on metadata provider
        experiment: experiment name
        tag: custom commit message
        """
        container = self.client.get_container_client(proj)
        container.upload_blob(
            name="{}/{}/df_summary.json".format(experiment, tag),
            data=bytes(json.dumps(df_metadata, cls=NpEncoder).encode("UTF-8")),
        )

    def dump_pickle(self, model, proj, experiment, tag, filename):
        """
        serializes object to s3 directory

        Parameters
        ----------
        pickle_object: model that has been serialized into a pickle object
        proj: project name on metadata provider
        experiment: experiment name
        tag: custom commit message
        filename: filename to be exported
        """
        pickle_object = pickle.dumps(model)
        container = self.client.get_container_client(proj)
        container.upload_blob(
            name="{}/{}/{}.pkl".format(experiment, tag, filename), data=pickle_object
        )

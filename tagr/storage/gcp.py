"""
GCP Cloud Storage class implementation
"""

import json
import pickle

from google.cloud import storage
from io import StringIO
from tagr.utils import NpEncoder


class Gcp:
    def __init__(self):
        self.client = storage.Client()
        self.name = "GCP"

    def dump_csv(self, df, proj, experiment, tag, filename):
        """
        turns dataframe into csv and saves it to GCS directory

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
        bucket = self.client.get_bucket(proj)
        blob = bucket.blob("{}/{}/{}.csv".format(experiment, tag, filename))
        blob.upload_from_string(csv_buffer.getvalue())

    def dump_json(self, df_metadata, proj, experiment, tag):
        """
        turns dictionary into json and saves it to GCS directory

        Parameters
        ----------
        df_metadata: json object containing experiment metadata
        proj: project name on metadata provider
        experiment: experiment name
        tag: custom commit message
        """
        bucket = self.client.get_bucket(proj)
        blob = bucket.blob("{}/{}/df_summary.json".format(experiment, tag))
        blob.upload_from_string(
            bytes(json.dumps(df_metadata, cls=NpEncoder).encode("UTF-8"))
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
        bucket = self.client.get_bucket(proj)
        blob = bucket.blob("{}/{}/{}.pkl".format(experiment, tag, filename))
        blob.upload_from_string(pickle_object)

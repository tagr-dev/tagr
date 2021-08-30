"""
Aws S3 class implementation
"""

import json
import boto3
import pickle

from io import StringIO
from tagr.storage.aws_helper import AwsHelper
from tagr.utils import NpEncoder


class Aws:
    def __init__(self):
        self.S3 = boto3.resource("s3")
        self.name = "AWS"

    def dump_csv(self, df, proj, experiment, tag, filename):
        """
        turns dataframe into csv and saves it to s3 directory

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
        self.S3.Object(
            bucket_name=proj, key="{}/{}/{}.csv".format(experiment, tag, filename)
        ).put(Body=csv_buffer.getvalue())

    def dump_json(self, df_metadata, proj, experiment, tag):
        """
        turns dictionary into json and saves it to s3 directory

        Parameters
        ----------
        df_metadata: json object containing experiment metadata
        proj: project name on metadata provider
        experiment: experiment name
        tag: custom commit message
        """
        self.S3.Object(
            bucket_name=proj, key="{}/{}/df_summary.json".format(experiment, tag)
        ).put(
            Body=(bytes(json.dumps(df_metadata, cls=NpEncoder).encode("UTF-8"))),
            ContentType="application/json",
        )

    def dump_pickle(self, model, proj, experiment, tag, filename):
        """
        serializes oject to s3 directory

        Parameters
        ----------
        pickle_object: model that has been serialized into a pickle object
        proj: project name on metadata provider
        experiment: experiment name
        tag: custom commit message
        filename: filename to be exported
        """
        pickle_object = pickle.dumps(model)
        self.S3.Object(
            bucket_name=proj, key="{}/{}/{}.pkl".format(experiment, tag, filename)
        ).put(Body=pickle_object)

    def _Tagr__list(self, proj, experiment, tag):
        """
        gets list of files/folders located at {proj}/{experiment}/{tag}
        Parameters
        __________
        proj: project name (s3 bucket name)
        experiment: experiment name
        tag: custom commit message (optional)
        """
        aws_helper = AwsHelper()
        object_path = experiment
        if tag:
            object_path += "/" + tag

        return aws_helper.get_list_of_tables(proj, object_path)

    def _Tagr__fetch(self, proj, experiment, tag, filename):
        """
        fetches object located at {proj}/{experiment}/{tag}/{filename}
        Parameters
        __________
        proj: project name (s3 bucket name)
        experiment: experiment name
        tag: custom commit message
        filename: name of file to be fetched (either a .csv or .pkl file)
        """
        aws_helper = AwsHelper()
        object_path = experiment + "/" + tag + "/" + filename
        return aws_helper.get_object(proj, object_path)

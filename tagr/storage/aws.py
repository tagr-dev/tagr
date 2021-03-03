"""
Aws S3 class implementation
"""

import json
from io import StringIO
import boto3

from tagr.utils import NpEncoder

csv_buffer = StringIO()


class Aws:
    def __init__(self):
        self.S3 = boto3.resource("s3")

    def dump_csv(self, df, proj, exp, tag, filename):
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
        df.to_csv(csv_buffer)
        self.S3.Object(proj, "{}/{}/{}.csv".format(exp, tag, filename)).put(
            Body=csv_buffer.getvalue()
        )

    def dump_json(self, df_metadata, proj, exp, tag):
        """
        turns dataframe into csv and saves it to s3 directory

        Parameters
        ----------
        df_metadata: json object containing experiment metadata
        proj: project name on metadata provider
        exp: experiment name
        tag: custom commit message
        """
        self.S3.Object(proj, "{}/{}/df_summary.json".format(exp, tag)).put(
            Body=(bytes(json.dumps(df_metadata, cls=NpEncoder).encode("UTF-8"))),
            ContentType="application/json",
        )

    def dump_pickle(self, pickle_object, proj, exp, tag, filename):
        """
        turns dataframe into csv and saves it to s3 directory

        Parameters
        ----------
        pickle_object: model that has been serialized into a pickle object
        proj: project name on metadata provider
        exp: experiment name
        tag: custom commit message
        filename: filename to be exported
        """
        self.S3.Object(proj, "{}/{}/{}.pkl".format(exp, tag, filename)).put(
            Body=pickle_object
        )

    def list(self, proj, experiment, tag):
        '''
        gets list of files/folders located at {proj}/{experiment}/{tag}

        Parameters
        __________
        proj: project name (s3 bucket name)
        experiment: experiment name 
        tag: custom commit message (optional)
        '''
        folders = []
        object_path = experiment
        if tag:
            object_path += ('/' + tag)

        bucket = self.S3.Bucket(proj)
        
        for obj in bucket.objects.all():
            if obj.key.startswith(object_path):
                folders.append(proj + '/' + obj.key)
        #folders = self.S3.list_buckets()
        return folders
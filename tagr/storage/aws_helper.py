"""
code heavy helper functions for Aws.py
"""

import boto3
import pandas as pd
import pickle


class AwsHelper:
    def get_matching_s3_objects(self, bucket, object_path="", max_keys_per_request=1):
        """
        List objects in an S3 bucket.

        Parameters
        ----------
        bucket: Name of the S3 bucket.
        object_path: Path that leads to all tables
        max_keys_per_request: number of objects to list down
        """

        s3 = boto3.client("s3")
        kwargs = {"Bucket": bucket}

        # If the prefix is a single string (not a tuple of strings),
        # the filtering can be done directly in the S3 API.
        if isinstance(object_path, str):
            kwargs["Prefix"] = object_path
        else:
            kwargs["Prefix"] = str(object_path)

        kwargs["MaxKeys"] = max_keys_per_request

        while True:

            # The S3 API response is a large dict of metadata.
            # 'Contents' contains information about the listed objects.
            resp = s3.list_objects_v2(**kwargs)

            try:
                contents = resp["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.startswith(object_path):
                    yield obj

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until the
            # final page pages is reached (when this field is missing).
            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

    def get_matching_s3_keys(self, bucket, object_path="", max_keys_per_request=1):
        """
        Generate the keys in an S3 bucket.

        Parameters
        ----------
        bucket: Name of the S3 bucket.
        object_path: Path that leads to all tables
        max_keys_per_request: number of objects to list down
        """
        for obj in self.get_matching_s3_objects(
            bucket=bucket,
            object_path=object_path,
            max_keys_per_request=max_keys_per_request,
        ):
            yield obj["Key"]

    def get_list_of_tables(self, bucket, object_path):
        """
        Method to list down the tables under given snapshot path prefixed as:
        bucket/object_path/{filename}

        Parameters
        ----------
        bucket: name of s3 bucket
        object_path: Path that leads to all tables
        """
        s3_objects = []

        if object_path:
            for full_object_path in self.get_matching_s3_keys(
                bucket=bucket, object_path=object_path, max_keys_per_request=1
            ):
                s3_objects.append(bucket + "/" + full_object_path)

        return s3_objects

    def get_object(self, bucket, object_path):
        """
        Helper method to fetch .csv or .pkl object from s3

        Parameters
        ----------
        bucket: name of s3 bucket (project)
        object_path: Path that leads to the object being fetched
        """
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=object_path)

        if object_path.endswith(".csv"):
            obj = pd.read_csv(response["Body"], index_col=0)
        else:
            serialized_model = response["Body"].read()
            obj = pickle.loads(serialized_model)
        return obj

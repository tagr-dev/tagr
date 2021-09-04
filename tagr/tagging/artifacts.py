import logging
import pandas as pd

from datetime import datetime
from tagr.config import OBJECTS, RECOGNIZED_DTYPES
from tagr.storage.aws import Aws
from tagr.storage.azure import Azure
from tagr.storage.gcp import Gcp
from tagr.storage.local import Local

logger = logging.getLogger("tagging_artifact")


class Artifact:
    def __init__(self, val, obj_name, dtype=None):
        self.val = val
        self.obj_name = obj_name

        if dtype:
            self.dtype = dtype
            self.is_recognized_dtypes(self.dtype)
        else:
            self.dtype = OBJECTS[obj_name]

        self.check_expected_type()

    def __repr__(self):
        return "val: {0}, obj_name: {1}, dtype: {2}".format(
            self.val, self.obj_name, self.dtype
        )

    def check_expected_type(self):
        if self.dtype == "dataframe":
            self.evaluate_type(self.val, pd.DataFrame)
        elif self.dtype == "primitive":
            self.evaluate_type(self.val, (int, float, str, bool))

    @staticmethod
    def evaluate_type(val, expected_type):
        if not isinstance(val, expected_type):
            raise TypeError(
                "TypeError: artifact type did not match expected {} type".format(
                    str(expected_type)
                )
            )

    @staticmethod
    def is_recognized_dtypes(dtype):
        if not dtype in RECOGNIZED_DTYPES:
            raise ValueError("dtype must be of {}".format(", ".join(RECOGNIZED_DTYPES)))


class Tagr(object):
    def __init__(self):
        self.queue = {}
        self.cust_queue = {}
        self.storage_provider = Local()

        self.col_types_dict = {}
        self.col_stats_dict = {}

    def save(self, artifact, obj_name: str, dtype: str = None):
        """
        Tags a variable as something to be saved

        Parameters
        ----------
        artifact: variable to store
        obj: str. type of variable to be store.
            Choice of config.OBJECTS
            If you pass a string not in config.OBJECTS
            Tagr will store the object under the cust dir
        dtype: type or None, Default None
            used for storing custom variables not in OBJECTS
            Behavior as follows:

            * If `dtype` = 'string': saved to metadata json file as String
            * If `dtype` = 'df': saved as parquet to metadata store provider
            * If `dtype` = 'int' or 'float' or 'str': saved to metadata
             json file as int or float or str respectively. Need to retrieve
                entire json file to access any values
            * If `dtype` = 'metric': saved as readily accessible pickle
                value via metadata provider client
            * If `dtype`= 'viz' or 'other: saved as pickle

        Returns
        -------
        `artifact` to preserve declarative interface
        """
        if obj_name in OBJECTS:
            save_obj = Artifact(artifact, obj_name)
            self.queue[obj_name] = save_obj
        elif not dtype:
            raise ValueError("dtype must be provided if custom obj")
        else:
            save_obj = Artifact(artifact, obj_name, dtype)
            self.queue[obj_name] = save_obj

        return artifact

    def rm(self, obj_name):
        """
        Remove the supplied object from the dictionary of saved objects

        Parameter
        ---------
        obj_name: str. Name of object to be removed

        """
        if obj_name in self.queue:
            del self.queue[obj_name]
        else:
            raise KeyError("{} is not a key in the queue".format(obj_name))

    def ret_queue(self) -> dict:
        return self.queue

    def summary(self):
        """
        Returns pd.Dataframe of tagged variables and their values
        """
        data = []
        for k, artifact in self.queue.items():
            data.append([artifact.obj_name, artifact.val, artifact.dtype])

        return pd.DataFrame(data, columns=["obj_name", "val", "dtype"])

    def flush(self, proj, experiment, tag=None, storage="local"):
        """
        Pushes all variables from `queue` to metadata store.
        Generates metadata for artifacts of type pd.Dataframe in JSON
        format

        Parameters
        ----------
        proj: project name on metadata provider
        experiment: experiment name
        tag: custom commit message
        storage: destination for experiment data to be dumped ('aws', 'gcp', 'azure', 'local')
            - for storage, asssume local by default if destination not provided
        """

        # use datetime as index if tag name not provided
        if not tag:
            logger.info("using datetime as tag")
            tag = str(datetime.utcnow())

        if storage == "aws":
            self.storage_provider = Aws()
        elif storage == "gcp":
            self.storage_provider = Gcp()
        elif storage == "azure":
            self.storage_provider = Azure()
        elif storage == "local":
            self.storage_provider = Local()
            # if folder directory doesnt exist, then create new directory
            self.storage_provider.build_path(proj, experiment, tag)

        experiment_params = {"proj": proj, "experiment": experiment, "tag": tag}

        #####################
        # generate metadata #
        #####################
        summary = self.summary()
        self._flush_dfs(summary, experiment_params)
        self._flush_metadata_json(summary, experiment_params)
        self._flush_non_dfs(summary, experiment_params)

    def _get_primitive_objs_dict(self, summary) -> dict:
        """
        Collects all tagged primitive type objects in provided summary df.

        Parameters
        ----------
        summary: summary DataFrame of tagged variables returned from Tagr.summary()
        experiment_params: dict of experiment namespace variables

        Returns
        -------
        dict of key: obj name, val: obj

        """
        logger.info("collecting primitive objects for json file")
        primitive_objs_dict = {}
        summary_primitive_objs = summary[summary["dtype"] == "primitive"]
        for i, row in summary_primitive_objs.iterrows():
            primitive_objs_dict[row["obj_name"]] = row["val"]

        return primitive_objs_dict

    def _flush_metadata_json(self, summary, experiment_params):
        """
        Collects names and values of tagged primitive objects and metadata dataframes.
        Pushes to metadata provider as json
        """
        primitive_objs_dict = self._get_primitive_objs_dict(summary)

        df_metadata = {
            "types": self.col_types_dict,
            "stats": self.col_stats_dict,
            "primitive_objs": primitive_objs_dict,
        }

        logger.info("flushing metadata json to " + self.storage_provider.name)
        self.storage_provider.dump_json(
            df_metadata,
            experiment_params["proj"],
            experiment_params["experiment"],
            experiment_params["tag"],
        )

    def _flush_dfs(self, summary, experiment_params):
        """
        Collects summary statistics for all tagged dataframes in provided summary df.
        Saves statistics to `col_types_dict` and `col_types_dict` class attributes.
        Pushes dataframes to metadata provider as csv
        """
        summary_dfs = summary[summary["dtype"] == "dataframe"]

        logger.info("collecting dataframe types and summary stats for json file")
        for i, row in summary_dfs.iterrows():
            df = row["val"]
            df_name = row["obj_name"]
            self.col_types_dict[df_name] = dict(
                zip(df.columns, df.dtypes.map(lambda x: x.name))
            )
            self.col_stats_dict[df_name] = df.describe().to_dict()

            logger.info("flushing dataframes as csv to " + self.storage_provider.name)
            self.storage_provider.dump_csv(
                df,
                experiment_params["proj"],
                experiment_params["experiment"],
                experiment_params["tag"],
                df_name,
            )

    def _flush_non_dfs(self, summary, experiment_params):
        """
        Pushes all non dataframe, int, float and str objects to metadata provider as pickle
        """
        summary_objects = summary[summary["dtype"] != "dataframe"]
        for i, row in summary_objects.iterrows():
            obj = row["val"]
            obj_name = row["obj_name"]
            logger.info("flushing objects as pickle to " + self.storage_provider.name)
            self.storage_provider.dump_pickle(
                obj,
                experiment_params["proj"],
                experiment_params["experiment"],
                experiment_params["tag"],
                obj_name,
            )

    def list(self, proj, experiment, tag=None, dump="local"):
        """
        fetches previously flushed experiments
        Parameters
        ----------
        proj: project name on metadata provider
        experiment: experiment name
        tag: custom commit message (optional)
        dump: destination for experiment data to be fetched from ('aws', 'local')
            - for dump, asssume local by default if destination not provided
        """
        if dump == "aws":
            self.storage_provider = Aws()
        elif dump == "local":
            self.storage_provider = Local()

        return self.storage_provider.__list(proj, experiment, tag)

    def fetch(self, proj, experiment, tag, filename, dump="local"):
        if dump == "aws":
            self.storage_provider = Aws()
        elif dump == "local":
            self.storage_provider = Local()

        return self.storage_provider.__fetch(proj, experiment, tag, filename)

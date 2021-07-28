import logging
import pandas as pd
import pickle

from datetime import datetime
from tagr.config import OBJECTS, EXP_OBJECTS, EXP_OBJECT_TYPES
from tagr.storage.aws import Aws
from tagr.storage.local import Local

logger = logging.getLogger("tagging_artifact")


class Artifact:
    def __init__(self, val, obj_name, dtype=None):
        self.val = val
        self.obj_name = obj_name

        if dtype:
            self.dtype = dtype
        else:
            self.dtype = OBJECTS[obj_name]

    def __repr__(self):
        return "val: {0}, obj_name: {1}, dtype: {2}".format(
            self.val, self.obj_name, self.dtype
        )


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
            self.queue[obj_name] = Artifact(artifact, obj_name)
        elif not dtype:
            raise ValueError("dtype must be provided if custom obj")
        else:
            self.queue[obj_name] = Artifact(artifact, obj_name, dtype)

        return artifact

    def ret_queue(self) -> dict:
        return self.queue

    def inspect(self):
        """
        Returns pd.Dataframe of tagged variables and their values
        """
        data = []
        for k, artifact in self.queue.items():
            data.append([artifact.obj_name, artifact.val, artifact.dtype])

        return pd.DataFrame(data, columns=["obj_name", "val", "dtype"])

    def flush(self, proj, experiment, tag=None, dump="local"):
        """
        Pushes all variables from `queue` to metadata store.
        Generates metadata for artifacts of type pd.Dataframe in JSON
        format

        Parameters
        ----------
        proj: project name on metadata provider
        experiment: experiment name
        tag: custom commit message
        dump: destination for experiment data to be dumped ('aws', 'gcp', 'azure', 'local')
            - for dump, asssume local by default if destination not provided
        """

        # use datetime as index if tag name not provided
        if not tag:
            logger.info("using datetime as tag")
            tag = str(datetime.utcnow())

        if dump == "aws":
            self.storage_provider = Aws()
        elif dump == "local":
            self.storage_provider = Local()
            # if folder directory doesnt exist, then create new directory
            self.storage_provider.build_path(proj, experiment, tag)

        experiment_params = {"proj": proj, "experiment": experiment, "tag": tag}

        #####################
        # generate metadata #
        #####################
        summary = self.inspect()
        self._flush_dfs(summar, experiment_params)

        nums_and_strings = list(
            summary[summary["type"].isin(["int", "float", "str"])].index
        )

        nums_and_strings_dict = {}

        logger.info("collecting nums and strings for json")
        for i in nums_and_strings:
            num_or_str = summary["artifact"].loc[i]
            nums_and_strings_dict[i] = num_or_str

        df_metadata = {
            "types": col_types_dict,
            "stats": col_stats_dict,
            "nums_and_strings": nums_and_strings_dict,
        }

        logger.info("flushing metadata json to " + str(dump))

        self.storage_provider.dump_json(df_metadata, proj, experiment, tag)

        logger.info("flushing models to " + str(dump))
        models = list(summary[summary["type"] == "model"].index)

        for model in models:
            model_object = summary["artifact"].loc[model]
            logger.info("flushing " + str(model) + "metadata json to S3")
            self.storage_provider.dump_pickle(
                model_object, proj, experiment, tag, model
            )

    def _flush_dfs(self, summary, experiment_params):
        """
        Collects summary statistics for all tagged dataframes in provided summary df.
        Saves statistics to `col_types_dict` and `col_types_dict` class attributes.
        Pushes dataframes to metadata provider as csv

        Parameters
        ----------
        summary: tagg varaibles summary DataFrame returned from Tagr.inspect()
        experiment_params: dict of experiment namespace variables
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

            logger.info("flushing dataframes as csv to " + str(dump))
            self.storage_provider.dump_csv(
                df,
                experiment_params["proj"],
                experiment_params["experiment"],
                experiment_params["tag"],
                df_name
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
        # determine which storage provider to use
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

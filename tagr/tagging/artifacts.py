import logging
import pandas as pd
import pickle

from datetime import datetime
from tagr.config import OBJECTS, EXP_OBJECTS, EXP_OBJECT_TYPES
from tagr.storage.aws import Aws
from tagr.storage.local import Local

logger = logging.getLogger("tagging_artifact")


class Tags(object):
    def __init__(self):
        self.queue = {}
        self.cust_queue = {}
        self.storage_provider = Local()

    def save(self, artifact, obj: str, dtype: str = None):
        """
        Tags a variable as something to be saved

        Parameters
        ----------
        artifact: variable to store
        obj: str. type of variable to be store.
            Choice of config.OBJECTS
            If you pass a string not in config.OBJECTS
            WaterFlow will store the object under the cust dir
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

        if obj in OBJECTS:
            self.queue[obj] = artifact
        elif not dtype:
            raise ValueError("dtype must be provided if custom obj")
        else:
            self.cust_queue[obj] = (artifact, dtype)

        return artifact

    def ret_queue(self) -> dict:
        return self.queue

    # todo take rows youd like to see as argument
    # todo add shape for pd dataframe
    # todo col for varaible name. %who, local(), dir()
    def inspect(self):
        """
        Returns pd.Dataframe of tagged variables and their values.
        Missing variables will have `NaN` value
        """
        cust_keys = list(self.cust_queue.keys())

        artifact = [self.queue.get(artif) for artif in EXP_OBJECTS] + [
            self.cust_queue.get(artif)[0] for artif in self.cust_queue
        ]

        types = EXP_OBJECT_TYPES + [
            self.cust_queue.get(artif)[1] for artif in self.cust_queue
        ]

        return pd.DataFrame(
            {"artifact": artifact, "type": types}, index=EXP_OBJECTS + cust_keys
        )

    def flush(self, proj, experiment, dump='local', tag=None):
        """
        Pushes all variables from `queue` to metadata store.
        Generates metadata for artifacts of type pd.Dataframe in JSON
        format

        Parameters
        ----------
        proj: project name on metadata provider
        exp: experiment name
        tag: custom commit message
        dump: destination for experiment data to be dumped ('aws', 'gcp', 'azure', 'local')
            - for dump, asssume local by default if destination not provided
        """
        # todo create metadata provider file to hook into s3 and blob
        
        # use datetime as index if tag name not provided
        if not tag:
            logger.info("using datetime as tag")
            tag = str(datetime.utcnow())

        # determine which storage provider to use
        if dump == 'aws':
            self.storage_provider = Aws()
        elif dump == 'local':
            self.storage_provider = Local()
            # if folder directory doesnt exist, then create new directory
            self.storage_provider.build_path(proj, experiment, tag)

        #####################
        # generate metadata #
        #####################
        logger.info("generating metadata json file")
        summary = self.inspect()
        # filter for not null df elements
        df_names = list(
            summary[
                (pd.notnull(summary["artifact"])) & (summary["type"] == "dataframe")
                ].index
        )

        col_types_dict = {}
        col_stats_dict = {}

        logger.info("collecting dataframe types and summary stats for json")
        for df_name in df_names:
            df = summary["artifact"].loc[df_name]
            col_types_dict[df_name] = dict(zip(df.columns, df.dtypes.map(lambda x: x.name)))
            col_stats_dict[df_name] = df.describe().to_dict()

            #############
            # Push dfs #
            #############
            # todo: save larger dfs as parquet, maybe partition as well
            logger.info("flushing dataframes as csv to " + str(dump))
            # push csv
            self.storage_provider.dump_csv(df, proj, experiment, tag, df_name)

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
            pickle_byte_obj = pickle.dumps(model_object)
            logger.info("pushing " + str(model) + "metadata json to S3")
            self.storage_provider.dump_pickle(pickle_byte_obj, proj, experiment, tag, model)

    def list(self, proj, experiment, tag="", dump='local'):
        """
        fetches previously flushed experiments

        Parameters
        ----------
        proj: project name on metadata provider
        dir: directory path to look up
        dump: destination for experiment data to be fetched from ('aws', 'gcp', 'azure', 'local')
            - for dump, asssume local by default if destination not provided
        """
        # determine which storage provider to use
        if dump == 'aws':
            self.storage_provider = Aws()
        elif dump == 'local':
            self.storage_provider = Local()
            
        return self.storage_provider.list(proj, experiment, tag)
        #return self.storage_provider.fetch(proj, path)


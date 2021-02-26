import json
import pickle
import os
import logging

logger = logging.getLogger("tagging_artifact")

class Local:
    def dump_csv(self, df, proj, experiment, tag, filename):
        """
        turns dataframe into csv and saves it to local storage

        Parameters
        ----------
        df: dataframe object
        proj: project name 
        exp: experiment name
        tag: custom commit message
        filename: filename to be saved locally
        """
        df.to_csv("{}/{}/{}/{}.csv".format(proj, experiment, tag, filename), index=False)

    def dump_json(self, df_metadata, proj, experiment, tag):
        """
        turns dataframe into csv and saves it to local storage

        Parameters
        ----------
        df_metadata: json object containing experiment metadata
        proj: project name 
        exp: experiment name
        tag: custom commit message
        """
        with open("{}/{}/{}/df_summary.json".format(proj, experiment, tag), 'w') as outfile:
            json.dump(df_metadata, outfile, default=str)

    def dump_pickle(self, pickle_object, proj, experiment, tag, filename):
        """
        turns dataframe into csv and saves it to local storage

        Parameters
        ----------
        pickle_object: model that has been serialized into a pickle object
        proj: project name 
        exp: experiment name
        tag: custom commit message
        filename: filename to be exported
        """
        pickle.dump(pickle_object, open(
            "{}/{}/{}/{}.pkl".format(proj, experiment, tag, filename), "wb"
        ))
    
    def build_path(self, proj, experiment, tag):
        try:
            os.makedirs("{}/{}/{}".format(proj, experiment, tag))
        except OSError:
            print("The directory %s already exists" % "{}/{}/{}".format(proj, experiment, tag))
    
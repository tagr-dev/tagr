import json
import pickle
import os
import logging

logger = logging.getLogger("saving experiment to local storage")

class Local:
    def dump_csv(self, df, proj, experiment, tag, filename):
        """
        turns dataframe into csv and saves it to local storage

        Parameters
        ----------
        df: dataframe object
        proj: project name 
        experiment: experiment name
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
        experiment: experiment name
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
        experiment: experiment name
        tag: custom commit message
        filename: filename to be exported
        """
        pickle.dump(pickle_object, open(
            "{}/{}/{}/{}.pkl".format(proj, experiment, tag, filename), "wb"
        ))
    
    def build_path(self, proj, experiment, tag):
        """
        sets up a folder directory within local storage to push metadata to

        Parameters
        ----------
        proj: project name 
        experiment: experiment name
        tag: custom commit message
        """
        try:
            os.makedirs("{}/{}/{}".format(proj, experiment, tag))
        except OSError:
            logger.info("The directory %s already exists. If using the tag argument, please provide a new unique identifier." % "{}/{}/{}".format(proj, experiment, tag))

    def list(self, proj, experiment, tag):
        '''
        gets list of files/folders located at {proj}/{experiment}/{tag}
        Parameters
        __________
        proj: project name
        experiment: experiment name
        tag: custom commit message (optional)
        '''
        path = proj + "/" + experiment
        if tag:
            path += ('/' + tag)

        folders = os.listdir(path)
        return folders



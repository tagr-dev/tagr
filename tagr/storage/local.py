import json
import pickle
import os

class Local:
    def dump_csv(self, df, proj, exp, tag, filename):
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
        df.to_csv("{}-{}-{}-{}.csv".format(proj, exp, tag, filename), index=False)

    def dump_json(self, df_metadata, proj, exp, tag):
        """
        turns dataframe into csv and saves it to local storage

        Parameters
        ----------
        df_metadata: json object containing experiment metadata
        proj: project name 
        exp: experiment name
        tag: custom commit message
        """
        with open("{}-{}-df_summary.json".format(exp, tag), 'w') as outfile:
            json.dump(df_metadata, outfile, default=str)

    def dump_pickle(self, pickle_object, proj, exp, tag, filename):
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
            "{}-{}-{}-{}.pkl".format(proj, exp, tag, filename), "wb"
        ))
    
    def fetch(self, proj, path):
        folders = os.listdir('/' + proj + '/' + path)
        return folders

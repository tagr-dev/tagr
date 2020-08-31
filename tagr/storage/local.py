"""
Local class implementation
"""

import json
import pickle


class Local:
    """
    Aws class
    obj = Local()
    """
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
        df.to_csv("{}-{}-{}-{}.csv".format(proj, exp, tag, filename), index=False)

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
        with open("{}-{}-df_summary.json".format(exp, tag), 'w') as outfile:
            json.dump(df_metadata, outfile, default=str)

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
        pickle.dump(pickle_object, open(
            "{}-{}-{}-{}.pkl".format(proj, exp, tag, filename), "wb"
        ))

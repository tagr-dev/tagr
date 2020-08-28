import json
import pickle
import pandas 

class Local(object):
    def dump_csv(self, df, proj, exp, tag, i):
        df.to_csv("{}-{}-{}-{}.csv".format(proj, exp, tag, i), index=False)

    def dump_json(self, df_metadata, filename):
        json.dump(df_metadata, filename)
    
    def dump_pickle(self, pickle_object, filename)
        pickle.dump(pickle_object, open( filename, "wb" ))
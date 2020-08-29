import json
import pickle
import pandas 

class Local:
    def dump_csv(self, df, proj, exp, tag, i):
        df.to_csv("{}-{}-{}-{}.csv".format(proj, exp, tag, i), index=False)

    def dump_json(self, df_metadata, proj, exp, tag):
        with open("{}-{}-df_summary.json".format(exp, tag), 'w') as outfile:
            json.dump(df_metadata, outfile, default=str)
    
    def dump_pickle(self, pickle_object, exp, tag, i):
        pickle.dump(pickle_object, open( 
            "{}-{}-{}.pkl".format(exp, tag, i), "wb" 
        ))
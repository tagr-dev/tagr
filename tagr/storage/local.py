import json
import pickle
import pandas 


class Local:
    def dump_csv(self, df, proj, exp, tag, filename):
        df.to_csv("{}-{}-{}-{}.csv".format(proj, exp, tag, filename), index=False)

    def dump_json(self, df_metadata, proj, exp, tag):
        with open("{}-{}-df_summary.json".format(exp, tag), 'w') as outfile:
            json.dump(df_metadata, outfile, default=str)
    
    def dump_pickle(self, pickle_object, proj, exp, tag, filename):
        pickle.dump(pickle_object, open( 
            "{}-{}-{}-{}.pkl".format(proj, exp, tag, filename), "wb" 
        ))

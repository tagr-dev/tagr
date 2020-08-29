import boto3
from tagr.utils import NpEncoder

s3 = boto3.resource("s3")

class Aws:
    def dump_csv(self, df, proj, exp, tag, i):
        df.to_csv("s3://{}/{}/{}/{}.csv".format(proj, exp, tag, i), index=False)
    
    def dump_json(self, df_metadata, proj, exp, tag):
        s3_object = s3.Object(proj, "{}/{}/df_summary.json".format(exp, tag))
        self.s3_object.put(
            Body=(bytes(json.dumps(df_metadata, cls=NpEncoder).encode("UTF-8"))),
            ContentType="application/json",
        )

    def dump_pickle(self, pickle_object, exp, tag, i):
        self.s3.Object(proj, "{}/{}/{}.pkl".format(exp, tag, i)).put(
            Body=pickle_byte_obj
        )
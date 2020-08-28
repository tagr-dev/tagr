import boto3
from tagr.utils import NpEncoder

class Aws(object):
    def __init__(self):
        self.s3 = boto3.resource("s3")

    def dump_csv(self, df, proj, exp, tag, i):
        df.to_csv("s3://{}/{}/{}/{}.csv".format(proj, exp, tag, i), index=False)
    
    def dump_json(self, df_metadata, proj, exp, tag):
        s3_object = self.s3.Object(proj, "{}/{}/df_summary.json".format(exp, tag))
        s3_object.put(
            Body=(bytes(json.dumps(df_metadata, cls)))
        )

    def dumo
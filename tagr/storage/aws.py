import boto3
from io import StringIO
import json
from tagr.utils import NpEncoder

s3 = boto3.resource("s3")
csv_buffer = StringIO()
class Aws:
    def dump_csv(self, df, proj, exp, tag, i):
        df.to_csv(csv_buffer)
        s3.Object(proj, "{}/{}/{}.csv".format(exp, tag, i)).put(
            Body=csv_buffer.getvalue()
        )
    
    def dump_json(self, df_metadata, proj, exp, tag):
        s3.Object(proj, "{}/{}/df_summary.json".format(exp, tag)).put(
            Body=(bytes(json.dumps(df_metadata, cls=NpEncoder).encode("UTF-8"))),
            ContentType="application/json",
        )

    def dump_pickle(self, pickle_object, proj, exp, tag, i):
        s3.Object(proj, "{}/{}/{}.pkl".format(exp, tag, i)).put(
            Body=pickle_object
        )
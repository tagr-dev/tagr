import boto3
from io import StringIO
import json
from tagr.utils import NpEncoder

csv_buffer = StringIO()


class Aws:
    def __init__(self):
        self.S3 = boto3.resource("s3")

    def dump_csv(self, df, proj, exp, tag, filename):
        df.to_csv(csv_buffer)
        self.S3.Object(proj, "{}/{}/{}.csv".format(exp, tag, filename)).put(
            Body=csv_buffer.getvalue()
        )
    
    def dump_json(self, df_metadata, proj, exp, tag):
        self.S3.Object(proj, "{}/{}/df_summary.json".format(exp, tag)).put(
            Body=(bytes(json.dumps(df_metadata, cls=NpEncoder).encode("UTF-8"))),
            ContentType="application/json",
        )

    def dump_pickle(self, pickle_object, proj, exp, tag, filename):
        self.S3.Object(proj, "{}/{}/{}.pkl".format(exp, tag, filename)).put(
            Body=pickle_object
        )

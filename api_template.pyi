#import
from waterflow import session, save

@session
def train_model():
    X_train, X_test, y_train, y_test =  save(sklearn.model_selection(X, y), "tts")

    model = save(sklearn.ensemble.RandomForestClassifier(), "model")

    y_pred = save(model.predict(y_test), "prediction")
########################

$ pip install WaterFlow
$ WaterFlow configure aws -> provide aws creds and bucket path

import WaterFlow
from WaterFlow import save

def save(artifact, obj:none, dtype:none):
    """
    side effect to s3

    obj reserved: [X_train, X_test, y_train, y_test, model, prediction, trained_model,
    hyperparams, y_pred, accuracy]

    ret => artifact
    """

# 'tts' and 'x_train' are reserved to optimize storage type
# ex: 'tts' => parquet files
X_train, X_test, y_train, y_test = save(sklearn.train_test_split(X, y), "tts") # expects 4 vairables ret

save(X_train, "x_train")

# custom save type, pass in name to create s3 obj
# these objects can only be saved and fetched, not compared
graph = save(plt.plot(X, y), obj:"graph")

clf = sklearn.ensemble.randomforestclassifier(**kwargs)

# save the model and the params as 2 objects
save(clf.fit(X_train, X_test), "model") /
    -> trained_model, hyperparams

y_pred = save(clf.predict(y_test), "y_pred")

#ex metric, optional
acc = save(accuracy(y_pred, y_test), obj:"accuracy", dtype:"float")
roc = save(roc(y_pred, y_test), obj:"roc", dtype:"float")

WaterFlow.inspect() -> # print to console whats saved. X_train: pd.Dataframe, ... also whats missing

# create dir if doesn't exist
# sub_exp is optional. Auto incrementing if none
WaterFlow.s3_flush(proj: "msci", exp: "rforest", tag:"increase number of leaves")


##################################
from WaterFlow import s3, blob, local

# print all sub_experiment names
WaterFlow.checkS3(proj: "msci", exp: "rforst")


# verbose
model = s3(proj: "msci", exp: "rforest", tag:"increase number of leaves").model

model = s3(proj: "msci", exp: "rforest").model.latest()

# context manager
WaterFlow.global_context(s3: "msci", exp: "rforest")

model = s3(tag:"increase number of leaves").model
X_train = s3(tag:"increase number of leaves").model


WaterFlow.global_context(s3: "msci", exp: "rforest", tag:"increase number of leaves")

model = s3().model()
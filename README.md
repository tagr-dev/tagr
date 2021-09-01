# Tagr 
[![Build Status](https://travis-ci.com/tagr-dev/tagr.svg?branch=master)](https://travis-ci.com/tagr-dev/tagr)
[![codecov](https://codecov.io/gh/tagr-dev/tagr/branch/master/graph/badge.svg)](https://codecov.io/gh/tagr-dev/tagr)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A cloud agnostic data science productivity tool that will:
- help streamline the data science experimentation process
- allow data scientists to manage models and experiment data
- seamlessly integrate with different cloud storage providers such as S3, Google Cloud Storage, Azure Cloud Storage

# Installation
```
pip install tagr
```

# Authentication
Tagr uses the Python SDK of each cloud provider to handle serialization and retrieval. This means any authentication methods supported by the respective SDK will be compatible with Tagr. The Azure SDK is dissimilar from AWS and GCP. It does not lookup configurations in the system. Rather credentials must be provided in the client constructor. As a result, supplying credentials via env vars is the currently only supported authentication method for Azure (I don't want to setup AD). See `.env.sample` for the necessary creds.

# Instructions
1. Import tagr 
```
from tagr.tagging.artifacts import Tagr
```
2. After building your model and performing exploratory data analysis of your dataset, tag your training/testing/prediction datasets and model
```
tag = Tagr()
x = tag.save(artifact=df, obj_name="X_train")
y = tag.save(artifact=2.5, obj_name="float1", dtype="primitive")
model = tag.save(artifact=RandomForestClassifier(max_depth=30), obj_name="model")
y_pred = tag.save(artifact=plt.plot([1, 2, 3, 4]), obj_name'viz', dtype="other")
```

3. View what artifacts you have tagged so far
```
tag.inspect()
```

4. Push all your tagged artifacts to a cloud storage solution of your choice
```
# S3
tag.flush(proj="tagr-dev", experiment="dev/sunrise", storage="aws")
```

```
# Google Cloud Storage
tag.flush(proj="tagr-dev", experiment="dev/sunrise", storage="gcp")
```

```
# Azure Storage Blob
tag.flush(proj="tagr-dev", experiment="dev/sunrise", storage="azure")
```

# How to test
1. Build the container 
```
make
```

2. Set env vars
```
source .env.test
```

3. Spin up a jupyter notebook in the container (for manual debugging)
```
jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root &
```

4. Test
```
python -m unittest discover test/
```
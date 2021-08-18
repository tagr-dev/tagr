# Tagr 
[![Build Status](https://travis-ci.com/tagr-dev/tagr.svg?branch=master)](https://travis-ci.com/tagr-dev/tagr)
[![codecov](https://codecov.io/gh/tagr-dev/tagr/branch/master/graph/badge.svg)](https://codecov.io/gh/tagr-dev/tagr)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A cloud agnostic data science productivity tool that will:
- help streamline the data science experimentation process
- allow data scientists to manage models and experiment data
- seamlessly integrate with different cloud storage providers such as S3, Google Cloud Storage, Azure Cloud Storage

# How to test
1. Run make
```
make
```
2. Spin up a jupyter notebook in the container
```
jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root &
```
3. Test
```
# export dummy aws env vars
python -m unittest discover test/
```

# Instructions
1. Import tagr 
```
from tagr.tagging.artifacts import Tagr
from tagr.config import EXP_OBJECTS, OBJECTS
```
2. After building your model and performing exploratory data analysis of your dataset, tag your training/testing/prediction datasets and model
```
tag = Tagr()
x = tag.save(mock_df1, "X_train", "int")
y = tag.save(mock_df2, "y_train")
model = tag.save(RandomForestClassifier(max_depth=30), "model")
lin_model = tag.save(LinearRegression(), 'linmodel', 'model')
y_pred = tag.save(mock_df3, 'y_pred')
```

3. View what artifacts you have tagged so far
```
tag.inspect()
```

4. Push all your tagged artifacts to a cloud storage solution of your choice
```
# s3
tag.flush('tagr-dev', 'dev/eric', 'aws', 'demo')

# local
tag.flush('tagr-dev', 'eric', 'demo')

```

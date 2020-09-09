# tagr (a work in progress)
A cloud agnostic data science productivity tool that will:
- help streamline the data science experimentation process
- allow data scientists to manage models and experiment data
- seamlessly integrate with different cloud storage providers such as S3, Google Cloud Storage, Azure Cloud Storage

# Instructions
1. Import tagr 
```
from tagr.tagging.artifacts import Tags
from tagr.config import EXP_OBJECTS, OBJECTS
```
2. After building your model and performing exploratory data analysis of your dataset, tag your training/testing/prediction datasets and model
```
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
tag.flush('waterflow-tagr', 'dev/eric', 'aws', 'demo')

# local
tag.flush('waterflow-tagr', 'eric', 'demo')

```

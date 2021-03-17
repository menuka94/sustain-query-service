# SUSTAIN Query Service Messages

This document provides a formal specification for the supported gRPC request forms, as well as any JSON request/response models.
The sustain-query-service services three distinct categories of queries which are further described below.
Each query category requires two gRPC messages: a request, and a response. Within each of the gRPC messages, JSON schemas may be further defined.
The rationale behind having a generalized gRPC message schema containing specific, strictly defined JSON string bodies, is that the 
proto file is not easily modified without making time-consuming changes to both the client and server applications to regenerate the protocol files. Thus,
it is easier to have the gRPC protocol support the *main* functionality of the backend services, without becoming too "brittle" with how messages
are defined.

## Model Query

Used for creating a Spark model on a specified set of data pulled in from MongoDB, and returns useful insights to the client.

### ModelRequest

*Version 1*

A gRPC `ModelRequest` represents a generalized request for a number of supported models.
- `version` : Used by both the client and server to agree on supported functionality.
    - *Type* : Integer
    - *Allowed values* : 1
- `type` : The type of the model being requested.
    - *Type* : String
    - *Allowed values* : "LinearRegression", "KMeansClustering"
- `id` : A number to uniquely identify a request made.
    - *Type* : Integer
    - *Allowed values* : [-2147483648, 2147483647]
- `request` : Contains a predefined, model-specific JSON string used by the server to build a Spark model.
  Based on the `type` field, the JSON `request` is unmarshalled into a specific Java model using Gson.
    - *Type* : String
    - *Allowed values* : See **Model Request Schemas**
    
gRPC message definition:
```
message ModelRequest {
    int32  version = 1;
    int32  id      = 2;
    string type    = 3;
    string request = 4;
}
```

### ModelResponse

*Version 1*

A gRPC `ModelResponse` encapsulates the results of a built model, as a result of a `ModelRequest`.
- `version` : Used by both the client and server to agree on supported functionality.
    - *Type* : Integer
    - *Allowed values* : 1
- `type` : The type of the model being requested.
    - *Type* : String
    - *Allowed values* : "LinearRegression", "KMeansClustering"
- `id` : The unique identifier of the original `ModelRequest` that kicked off the job.
    - *Type* : Integer
    - *Allowed values* : [-2147483648, 2147483647]
- `response` : Contains a predefined, model-specific JSON string used by the client display a model's results.
  Based on the `type` field, the JSON `response` is unmarshalled into a specific JavaScript on the client side.
    - *Type* : String
    - *Allowed values* : See **Model Response Schemas**
    
gRPC message definition:
```
message ModelResponse {
    int32  version  = 1;
    int32  id       = 2;
    string type     = 3;
    string response = 4;
}
```

### Model Request Schemas

JSON Schema of the model request body.

- **Linear Regression**
    - `host` :  The host of the Mongo router or database API.
    - `port` :  The port of the Mongo router or database API. (Default : 27017)
    - `database` : The name of the Mongo database wished to be used for the model.
    - `collection` : The name of the Mongo collection within the above database wished to be used for the model.
    - `features` : An array of all feature columns wished to be used to build the model.
   
Example:
```
{
   "host": "lattice-165",
   "port" 27017,
   "database": "sustaindb",
   "collection": "future_heat",
   "features": [ "year" ],
   "label": "temp",
   "gisJoins": [
       "G1201050",
       "G4804550",
       "G4500890"
   ]
}
```

    
- **Random Forest Regression**
    - `host` :  The host of the Mongo router or database API.
    - `port` :  The port of the Mongo router or database API. (Default : 27017)
    - `database` : The name of the Mongo database wished to be used for the model.
    - `collection` : The name of the Mongo collection within the above database wished to be used for the model.
    - `features` : An array of all feature columns wished to be used to build the model.
    - `isBootstrap` : Whether bootstrap samples are used when building trees.
    - `subsamplingRate` : Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
    - `numTrees`: Number of trees to train (at least 1). If 1, then no bootstrapping is used. If greater than 1, then bootstrapping is done.
    - `featureSubsetStrategy` : Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird". If "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1 (forest) set to "onethird".
    - `impurity`: Criterion used for information gain calculation. Supported values: "variance".
    - `maxDepth`: Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means 1 internal node + 2 leaf nodes). (suggested value: 4)
    - `maxBins`: Maximum number of bins used for splitting features. (suggested value: 100)
    - `trainSplit`: Training-Test Data split fraction (0,1).
    - `minInfoGain` : Minimum information gain for a split to be considered at a tree node. default 0.0.
    - `minInstancesPerNode` : Minimum number of instances each child must have after split. If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Must be at least 1. (default = 1)
    - `minWeightFractionPerNode` : Minimum fraction of the weighted sample count that each child must have after split. Should be in the interval [0.0, 0.5). (default = 0.0)
    
Example:
```
{
  "type": "R_FOREST_REGRESSION",
  "collections": [
    {
      "name": "macav2",
      "label": "min_eastward_wind",
      "features": [
        "max_eastward_wind","max_min_air_temperature"
      ]
    }
  ],
  "rForestRegressionRequest": {
    "gisJoins": [
      "G0100290"
    ],
    "isBootstrap": true,
    "subsamplingRate": 0.9,
    "numTrees": 10,
    "featureSubsetStrategy": "sqrt",
    "impurity": "variance",
    "maxDepth": 4,
    "maxBins": 50,
    "trainSplit": 0.9,
    "minInfoGain" : 0.01,
    "minInstancesPerNode" : 2,
    "minWeightFractionPerNode" : 0.1
  }
}
```

- **Gradient Boost Regression**
    - `host` :  The host of the Mongo router or database API.
    - `port` :  The port of the Mongo router or database API. (Default : 27017)
    - `database` : The name of the Mongo database wished to be used for the model.
    - `collection` : The name of the Mongo collection within the above database wished to be used for the model.
    - `features` : An array of all feature columns wished to be used to build the model.
    - `subsamplingRate` : Fraction of the training data used for learning each decision tree, in range (0, 1]. (default = 1.0)
    - `stepSize`: Param for Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator. (default = 0.1)
    - `featureSubsetStrategy` : Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt", "log2", "onethird". If "auto" is set, this parameter is set based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1 (forest) set to "onethird".
    - `impurity`: Criterion used for information gain calculation. Supported values: "variance".
    - `maxDepth`: Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means 1 internal node + 2 leaf nodes). (suggested value: 4)
    - `maxBins`: Maximum number of bins used for splitting features. (suggested value: 100)
    - `trainSplit`: Training-Test Data split fraction (0,1).
    - `minInfoGain` : Minimum information gain for a split to be considered at a tree node. default 0.0.
    - `minInstancesPerNode` : Minimum number of instances each child must have after split. If a split causes the left or right child to have fewer than minInstancesPerNode, the split will be discarded as invalid. Must be at least 1. (default = 1)
    - `minWeightFractionPerNode` : Minimum fraction of the weighted sample count that each child must have after split. Should be in the interval [0.0, 0.5). (default = 0.0)
    - `lossType` : Loss function which GBT tries to minimize. (case-insensitive) Supported: "squared" (L2) and "absolute" (L1) (default = squared)
    - `maxIter` : Maximum number of iterations >0.
    
Example:
```
{
  "type": "G_BOOST_REGRESSION",
  "collections": [
    {
      "name": "macav2",
      "label": "min_eastward_wind",
      "features": [
        "max_eastward_wind","max_min_air_temperature"
      ]
    }
  ],
  "gBoostRegressionRequest": {
    "gisJoins": [
      "G0100290"
    ],
    "subsamplingRate": 0.9,
    "stepSize": 0.1,
    "featureSubsetStrategy": "sqrt",
    "impurity": "variance",
    "maxDepth": 4,
    "maxBins": 50,
    "trainSplit": 0.9,
    "minInfoGain" : 0.01,
    "minInstancesPerNode" : 2,
    "minWeightFractionPerNode" : 0.1,
    "lossType" : "squared",
    "maxIter" : 5
  }
}
```

### Model Response Schemas

Example:
```
{
  "collection": "future_heat",
  "features": [ "year" ],
  "label": "temp",
  "results": [
    {
      "gisJoin": "G1201050",
      "coefficient": 0.028180811645281526,
      "intercept": 50.81293021627471,
      "rmse": 0.786793280966321
    },
    {
      "gisJoin": "G4804550",
      "coefficient": -0.7181363390196652,
      "intercept": 1545.8177687514876,
      "rmse": 3.3175516246730137
    },
    {
      "gisJoin": "G4500890",
      "coefficient": -0.6176283119024377,
      "intercept": 1348.397650300095,
      "rmse": 2.7104462483581777
    }
  ]
}
```
- **Random Forest Regression**

Example:
```
{"rForestRegressionResponse":
    {"gisJoin":"G0100290",
    "rmse":0.4488370652711823,
    "r2":0.9679055422883244}
}
```

- **Gradient Boost Regression**

Example:
```
{"gBoostRegressionResponse":
    {"gisJoin":"G0100290",
    "rmse":0.6858831779351341,
    "r2":0.9291608176309674}
}
```


## Compound Query


## Dataset Query

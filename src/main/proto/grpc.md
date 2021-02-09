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

## Compound Query


## Dataset Query

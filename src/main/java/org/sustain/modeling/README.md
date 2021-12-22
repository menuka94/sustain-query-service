# `org.sustain.modeling`

Package for **training** and **testing** Sustain Spark model implementations. These models are parameterized and generalized to
the input Spark [Dataset](https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/Dataset.html),
and does not do any data preprocessing, since that should have already been performed by the calling [task](../handlers/tasks/README.md).
The only thing these models provide is a means to build them using the input parameters, and launch training/testing/evaluation
in a way specific to the category of model being used.

The following packages are provided for categories of modeling with Spark:

- Regression: [org.sustain.modeling.regression](./regression/README.md)
- Clustering: *TODO*
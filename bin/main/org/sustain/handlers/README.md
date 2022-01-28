## `org.sustain.handlers`

A `Handler` class processes any given type of gRPC request, by creating a `Task` ([org.sustain.tasks](./tasks)) and executing it.
An appropriate response is returned from the `Task` to the `Handler`, which is then returned to the gRPC client.

Specific categories of handlers exist based on their context, see the following packages for more information:

- Regression Request Handlers: [org.sustain.handlers.spark.regression](./regression)
- Clustering Request Handlers *TODO*
- Query Request Handlers *TODO*
- Druid Request Handlers *TODO*
- Sliding Window Request Handlers *TODO*
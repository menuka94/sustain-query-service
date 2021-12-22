## `org.sustain.handlers.tasks`

Package for `Task` classes, which are wrappers around an entire request process.
A task encompasses everything from retrieving requested data from MongoDB and preprocessing it, to building, training, and testing models,
then building the response to be returned by the appropriate `Handler`.

The code for the actual execution of the model training/testing is found in [org.sustain.modeling](../../modeling).
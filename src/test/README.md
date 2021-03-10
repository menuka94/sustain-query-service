# Testing sustain-query-service

## Usage

This approach uses JUnit 5 and Gradle's `test` task. This lets you explicitly specify a single test to run:
- `./gradlew clean test --tests SustainServerTest.testExampleEchoQuery`
   - [Gradle Docs: Running a single JUnit class or method](https://docs.gradle.org/current/userguide/java_testing.html#simple_name_pattern)

You must have an already-running sustain-query-service on port **50051**, on the same host you're running your tests on.
This makes some of these tests *environment specific*.

## Implementation

- Test standard err/out is logged, which is reported to Gradle's Test Executor and displayed.
- The JUnit  `@BeforeAll` annotated function establishes a `ManagedChannel` and appropriate blocking stubs to act as a 
  gRPC client for your test to use. The `@AfterAll` annotated function shuts down the channel after your test is run.
- Instead of using messy, escaped, inline `String`s for test calls, tests now use file resources in the 
  `src/test/resources/requests` directory. This allows you to plop an example JSON file down, and use it without having
  to escape all the characters for Java interpretation.
- The structure of our `test` root directory looks like this:

```
└── test
    ├── java
    │   └── org
    │       └── sustain
    │           └── server
    │               └── SustainServerTest.java
    └── resources
        └── requests
            ├── bisecting_kmeans_clustering_county_stats_request.json
            ├── gaussian_mixture_clustering_county_stats_request.json
            ├── kmeans_clustering_county_stats_request.json
            ├── linear_regression_maca_v2_request.json
            └── lra_clustering_county_stats_request.json
```

- `SustainServer` and `sustain.proto` has had the `echoQuery` RPC call added to it for sanity testing and gRPC service 
  testing without launching a major 30-minute model-building task.
- To implement a _new_ modeling test, all you need to do is provide the `.json` resource file under 
  `test/resources/requests/`, and then add an `@Test` function to `SustainServerTest.java` which calls 
  `executeJsonModelRequest("<your test json>");`. This will send the JSON to the `JsonProxy` Service, and evaluates the 
  streamed responses.
  
Example unit test:
```
@Tag("slow")
@Test
public void testKMeansClusteringModel() {
    executeJsonModelRequest("requests/kmeans_clustering_county_stats_request.json");
}
```

- The `@Tag` annotation allows you to separate classes of tests, in this case, fast vs. slow

I initially implemented these tests using an `InProcessServer` which launches/shutdowns the gRPC Sustain services as a 
part of the testing JVM, but ultimately decided to make these tests _environment specific_, where they expect the gRPC 
Sustain to already be running on `localhost:50051`, and don't worry about trying to establish the server itself. This is
because we may want to test an existing instance of our server, not a new one.
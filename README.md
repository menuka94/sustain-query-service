# Project SUSTAIN
## U.S. Census Data Query Service

### How-to-Run

1. Create a file `src/main/resources/config.properties` using the given sample `src/main/resources/config.properties.sample`.
2. Fill in the values SERVER_HOST=<where you're planning to start the org.sustain.census.CensusServer>, DB_HOST=faure, and use your CSU credentials in MySQL databases in faure for DB_USERNAME and DB_PASSWORD.
3. Do `./gradlew install` from the project root.
4. Run `bin/census-server.sh` to start the CensusServer
5. Use `org.sustain.census.CensusClient` and `src/main/proto/census.proto` as references for implementing clients


### Available Data


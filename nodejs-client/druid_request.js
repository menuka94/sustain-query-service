const grpc = require('grpc');
const PROTO_DIR = '../src/main/proto'
const proto_loader = require('@grpc/proto-loader');

let packageDefinition = proto_loader.loadSync(
    [PROTO_DIR + '/sustain.proto'],
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    });

let protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
let service = protoDescriptor.sustain;
let stub = new service.Sustain('localhost:50051', grpc.credentials.createInsecure());

let druidRequest = {
    queryType: "groupBy",
    dataSource: "Gridmet_ALL_Partitioned",
    granularity: "year",
    columns: [ "GISJOIN" ],
    intervals: [ "1979-01-01/1999-01-01" ],
    filter: {
        type: "selector",
        dimension: "GISJOIN",
        value: "G4900450130600"
    },
    aggregations: [
        {
            type: "doubleMean",
            name: "pa_mean",
            fieldName: "precipitation_amount"
        },
        {
            type: "count",
            name: "atm_count",
            fieldName: "air_temperature_max"
        }
    ]
};

let call = stub.DruidDirectQuery(druidRequest);

call.on('data', function (response) {
    console.log(response.response, "\n");
});

call.on('end', function () {
    console.log('Completed');
});

call.on('err', function (err) {
    console.log(err);
})


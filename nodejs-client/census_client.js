const grpc = require('grpc');
const PROTO1_PATH = './proto/census.proto';
const PROTO2_PATH = './proto/targeted_census.proto';
const proto_loader = require('@grpc/proto-loader');

let packageDefinition = proto_loader.loadSync(
    [PROTO1_PATH, PROTO2_PATH],
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    });

let protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
let census_service = protoDescriptor.census;
let stub = new census_service.Census('localhost:50051', grpc.credentials.createInsecure());

let spatialTemporalInfo = {
    resolution: "county",
    latitude: 24.5,
    longitude: -82.0,
    decade: "_2010"
};
let totalPopulationRequest = {spatialTemporalInfo: spatialTemporalInfo};

stub.getTotalPopulation(totalPopulationRequest, function (err, response) {
    if (err) {
        console.log(err);
    } else {
        console.log(response);
    }
});


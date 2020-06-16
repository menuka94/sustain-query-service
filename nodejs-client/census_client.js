const grpc = require('grpc');
const PROTO_DIR = '../src/main/proto'
const proto_loader = require('@grpc/proto-loader');

let packageDefinition = proto_loader.loadSync(
    [PROTO_DIR + '/census.proto', PROTO_DIR + '/targeted_census.proto'],
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
    resolution: "tract",
    boundingBox: {
        x1: 40.5,
        y1: -105.1,
        x2: 40.6,
        y2: -105.0
    },
    decade: "_2010"
};
let request = {spatialTemporalInfo: spatialTemporalInfo};

console.log("Fetching total population");
stub.getTotalPopulation(request, function (err, response) {
    if (err) {
        console.log(err);
    } else {
        console.log(response);
    }
});

console.log("Fetching median household income");
stub.getMedianHouseholdIncome(request, function (err, response) {
    if (err) {
        console.log(err);
    } else {
        console.log(response);
    }
});


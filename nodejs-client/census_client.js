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


let geoJsonStringify = JSON.stringify({
        "type": "Feature",
        "properties": {},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [
                        -74.23118591308594,
                        40.56389453066509
                    ],
                    [
                        -73.75259399414062,
                        40.56389453066509
                    ],
                    [
                        -73.75259399414062,
                        40.80965166748853
                    ],
                    [
                        -74.23118591308594,
                        40.80965166748853
                    ],
                    [
                        -74.23118591308594,
                        40.56389453066509
                    ]
                ]
            ]
        }
    }
);

let spatialRequest = {
    censusResolution: "tract",
    censusFeature: "medianHouseholdIncome",
    requestGeoJson: geoJsonStringify,
    spatialOp: "geoIntersects"
};

let request = {SpatialRequest: spatialRequest};

stub.SpatialQuery(request, function (err, response) {
    if (err) {
        console.log(err);
    } else {
        console.log(response);
    }
});


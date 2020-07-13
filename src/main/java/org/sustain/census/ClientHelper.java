package org.sustain.census;

public class ClientHelper {
    CensusGrpc.CensusBlockingStub censusBlockingStub;

    public ClientHelper(CensusGrpc.CensusBlockingStub censusBlockingStub) {
        this.censusBlockingStub = censusBlockingStub;
    }

    public TargetedCensusResponse requestTargetedInfo(CensusFeature feature, CensusResolution resolution,
                                                      Decade decade,
                                                      Predicate.ComparisonOperator comparisonOperator,
                                                      double comparisonValue) {

        Predicate predicate = Predicate.newBuilder()
                .setComparisonOp(comparisonOperator)
                .setComparisonValue(comparisonValue)
                .setDecade(decade)
                .setCensusFeature(feature)
                .build();

        TargetedCensusRequest request = TargetedCensusRequest.newBuilder()
                .setResolution(resolution)
                .setPredicate(predicate)
                .build();

        return censusBlockingStub.executeTargetedCensusQuery(request);
    }
}

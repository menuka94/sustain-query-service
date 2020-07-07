package org.sustain.census;

public class ClientHelper {
    CensusGrpc.CensusBlockingStub censusBlockingStub;

    public ClientHelper(CensusGrpc.CensusBlockingStub censusBlockingStub) {
        this.censusBlockingStub = censusBlockingStub;
    }

    public TargetedQueryResponse requestTargetedInfo(CensusFeature feature, CensusResolution resolution,
                                                     Decade decade,
                                                     Predicate.ComparisonOperator comparisonOperator,
                                                     double comparisonValue) {

        Predicate predicate = Predicate.newBuilder()
                .setComparisonOp(comparisonOperator)
                .setComparisonValue(comparisonValue)
                .setDecade(decade)
                .setCensusFeature(feature)
                .build();

        TargetedQueryRequest request = TargetedQueryRequest.newBuilder()
                .setResolution(resolution)
                .setPredicate(predicate)
                .build();

        return censusBlockingStub.executeTargetedQuery(request);
    }
}

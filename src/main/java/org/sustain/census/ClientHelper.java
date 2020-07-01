package org.sustain.census;

public class ClientHelper {
    CensusGrpc.CensusBlockingStub censusBlockingStub;

    public ClientHelper(CensusGrpc.CensusBlockingStub censusBlockingStub) {
        this.censusBlockingStub = censusBlockingStub;
    }

    public TargetedQueryResponse requestTargetedInfo(Predicate.Feature feature, CensusResolution resolution,
                                                     Decade decade,
                                                     Predicate.ComparisonOperator comparisonOperator,
                                                     double comparisonValue) {

        Predicate predicate = Predicate.newBuilder()
                .setComparisonOp(comparisonOperator)
                .setComparisonValue(comparisonValue)
                .setDecade(decade)
                .setFeature(feature)
                .build();

        TargetedQueryRequest request = TargetedQueryRequest.newBuilder()
                .setResolution(resolution)
                .setPredicate(predicate)
                .build();

        return censusBlockingStub.executeTargetedQuery(request);
    }
}

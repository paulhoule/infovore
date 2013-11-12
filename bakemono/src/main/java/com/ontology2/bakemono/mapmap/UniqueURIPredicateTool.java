package com.ontology2.bakemono.mapmap;

public class UniqueURIPredicateTool extends UniqTool {

    protected Class getMapperClass() {
        return UniqueURIPredicateMapper.class;
    }

    @Override
    protected String getJobName() {
        return "uniqueURIPredicateTool";
    }
}

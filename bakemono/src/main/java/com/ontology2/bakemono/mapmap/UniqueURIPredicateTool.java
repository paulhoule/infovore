package com.ontology2.bakemono.mapmap;

import com.ontology2.bakemono.configuration.HadoopTool;

@HadoopTool("uniqURIPredicates")
public class UniqueURIPredicateTool extends UniqTool {

    protected Class getMapperClass() {
        return UniqueURIPredicateMapper.class;
    }

    @Override
    protected String getJobName() {
        return "uniqueURIPredicateTool";
    }
}

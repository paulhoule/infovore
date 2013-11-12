package com.ontology2.bakemono.mapmap;

public class UniqueURISubjectTool extends UniqTool {
    @Override
    protected Class getMapperClass() {
        return UniqueURISubjectMapper.class;
    }

    @Override
    protected String getJobName() {
        return "uniqURISubjectTool";
    }
}

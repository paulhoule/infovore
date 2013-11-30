package com.ontology2.bakemono.mapmap;

import com.ontology2.bakemono.configuration.HadoopTool;

@HadoopTool("uniqURISubjects")
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

package com.ontology2.bakemono.mapmap;

import com.ontology2.bakemono.configuration.HadoopTool;

@HadoopTool("uniqInternalURIObjects")
public class UniqueInternalURIObjectTool extends UniqTool {
    @Override
    protected Class getMapperClass() {
        return UniqueInternalURIObjectMapper.class;
    }

    @Override
    protected String getJobName() {
        return "uniqInternalURIObjects";
    }
}

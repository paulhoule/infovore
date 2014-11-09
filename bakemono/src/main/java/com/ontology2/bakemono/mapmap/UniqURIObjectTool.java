package com.ontology2.bakemono.mapmap;

import com.ontology2.bakemono.configuration.HadoopTool;

@HadoopTool("uniqURIObjects")
public class UniqURIObjectTool extends UniqTool {
    @Override
    protected Class getMapperClass() {
        return UniqueURIObjectMapper.class;
    }

    @Override
    protected String getJobName() {
        return "uniqURIObjects";
    }
}

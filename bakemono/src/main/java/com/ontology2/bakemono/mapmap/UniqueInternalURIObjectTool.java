package com.ontology2.bakemono.mapmap;

/**
 * Created with IntelliJ IDEA.
 * User: paul_000
 * Date: 11/12/13
 * Time: 1:45 PM
 * To change this template use File | Settings | File Templates.
 */
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

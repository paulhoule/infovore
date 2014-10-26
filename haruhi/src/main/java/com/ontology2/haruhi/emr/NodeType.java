package com.ontology2.haruhi.emr;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class NodeType {
    private final ImmutableMap<String,String> hadoopParameters;

    public NodeType(Map<String, String> hadoopParameters) {
        this.hadoopParameters = new ImmutableMap.Builder<String,String>().putAll(hadoopParameters).build();
    }

    public ImmutableMap<String,String> getHadoopParameters() {
        return hadoopParameters;
    }
}

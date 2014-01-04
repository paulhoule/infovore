package com.ontology2.bakemono.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

abstract public class ToolBase implements Tool {
    @Autowired
    protected ApplicationContext context;
    private Configuration conf;

    @Override
    public void setConf(Configuration entries) {
        this.conf=entries;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}

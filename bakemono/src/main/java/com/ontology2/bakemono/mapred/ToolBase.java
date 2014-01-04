package com.ontology2.bakemono.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

abstract public class ToolBase implements Tool {
    @Autowired
    protected ApplicationContext applicationContext;
    private Configuration conf;

    @Override
    public void setConf(Configuration entries) {
        this.conf=entries;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    protected void configureOutputCompression() {
        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.output.compression.type", "BLOCK");
        conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    }
}

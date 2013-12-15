package com.ontology2.bakemono.entityCentric;

import com.google.common.collect.Lists;
import com.ontology2.bakemono.configuration.HadoopTool;
import com.ontology2.bakemono.diffFacts.DiffFactsOptions;
import com.ontology2.centipede.parser.OptionParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

@HadoopTool("extractIsA")
public class ExtractIsATool implements Tool {
    @Autowired
    ApplicationContext applicationContext;
    private Configuration conf;

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration arg0) {
        this.conf=arg0;
    }

    @Override
    public int run(String[] strings) throws Exception {
        OptionParser parser=new OptionParser(DiffFactsOptions.class);
        applicationContext.getAutowireCapableBeanFactory().autowireBean(parser);

        ExtractIsAOptions options=(ExtractIsAOptions) parser.parse(Lists.newArrayList(strings));
        return 0;
    }
}

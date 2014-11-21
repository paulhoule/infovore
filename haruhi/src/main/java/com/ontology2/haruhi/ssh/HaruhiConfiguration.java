package com.ontology2.haruhi.ssh;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.File;

@Configuration
public class HaruhiConfiguration {
    @Autowired
    ResourceLoader loader;

    @javax.annotation.Resource
    String haruhiVersion;

    //
    //
    // this is bad.  we have a way for finding the jar and we should just use convention
    // over configuration,  that is,  jar X has an X-metadata jar
    //

    @Bean
    public Resource metadataJarPath() {
        return loader.getResource("file:"
            + new File(
                System.getProperty("user.home"),
                ".m2/repository/com/ontology2/bakemono/"+haruhiVersion+"/bakemono-"+haruhiVersion+"-metadata.jar"
            ).toString());
    };
}

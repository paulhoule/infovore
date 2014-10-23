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

    @Bean
    public Resource metadataJarPath() {
        return loader.getResource("file:"
            + new File(
                System.getProperty("user.home"),
                ".m2/repository/com/ontology2/bakemono/2.5-SNAPSHOT/bakemono-2.5-SNAPSHOT-metadata.jar"
            ).toString());
    };
}

package com.ontology2.bakemono.entityCentric;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.List;

import static junit.framework.TestCase.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/com/ontology2/bakemono/applicationContext.xml"})

public class ExtractIsAToolTest {

    @Resource
    ExtractIsATool extractIsATool;

    @Test
    public void parsesArgumentsCorrectly() throws IllegalAccessException {
        List<String> arguments= Lists.newArrayList(
            "-dir",
            "s3n://basekb-now/2013-12-08-00-00/sieved",
            "-input",
            "a/a-m-00000.nt.gz",
            "-prefix",
            "http://rdf.basekb.com/ns/",
            "-type",
            "skiing.ski_area",
            "-output",
            "s3n://basekb-sandbox/only-ski-tiny"
        );

        ExtractIsAOptions options=extractIsATool.extractOptions(arguments);
        assertEquals(1,options.type.size());
        assertEquals("http://rdf.basekb.com/ns/skiing.ski_area",options.type.get(0));
    }

    @Test
    public void parsesMultiplePathsCorrectly() throws IllegalAccessException {
        List<String> arguments= Lists.newArrayList(
                "-dir",
                "s3n://basekb-now/2013-12-08-00-00/sieved",
                "-input",
                "a,label",
                "-prefix",
                "http://rdf.basekb.com/ns/",
                "-type",
                "skiing.ski_area",
                "-output",
                "s3n://basekb-sandbox/only-ski-tiny"
        );

        ExtractIsAOptions options=extractIsATool.extractOptions(arguments);
        List<String> pathList=Lists.newArrayList(
                "s3n://basekb-now/2013-12-08-00-00/sieved/a",
                "s3n://basekb-now/2013-12-08-00-00/sieved/label"
        );
        assertEquals(pathList,options.input);
    }
}

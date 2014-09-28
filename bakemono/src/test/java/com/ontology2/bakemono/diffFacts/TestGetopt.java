package com.ontology2.bakemono.diffFacts;

import com.google.common.collect.Lists;
import com.ontology2.centipede.parser.OptionParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static junit.framework.TestCase.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/com/ontology2/bakemono/applicationContext.xml"})

public class TestGetopt {
    OptionParser parser;
    @Autowired
    ApplicationContext applicationContext;

    @Before
    public void setup() {
        parser=new OptionParser(DiffFactsOptions.class);
        applicationContext.getAutowireCapableBeanFactory().autowireBean(parser);
    }

    @Test
    public void oneSimpleCase() throws IllegalAccessException {
        List<String> args= Lists.newArrayList(
                "-R"
                ,"15"
                ,"-dir"
                ,"s3n://basekb-now/2013-11-24-00-00/sieved"
                ,"-left"
                ,"a,links,literals"
                ,"-dir"
                ,"s3n://basekb-now/2013-12-01-00-00/sieved"
                ,"-right"
                ,"a,links,literals"
                ,"-output"
                ,"s3n://basekb-sandbox/wayOut"
        );

        DiffFactsOptions that= (DiffFactsOptions) parser.parse(args);
        assertNotNull(that);
        assertEquals(15,that.reducerCount);
        assertEquals(
                Lists.newArrayList(
                    "s3n://basekb-now/2013-11-24-00-00/sieved/a",
                    "s3n://basekb-now/2013-11-24-00-00/sieved/links",
                    "s3n://basekb-now/2013-11-24-00-00/sieved/literals"
                )
                ,that.left
        );

        assertEquals(
                Lists.newArrayList(
                        "s3n://basekb-now/2013-12-01-00-00/sieved/a",
                        "s3n://basekb-now/2013-12-01-00-00/sieved/links",
                        "s3n://basekb-now/2013-12-01-00-00/sieved/literals"
                )
                ,that.right
        );

        System.out.println(that.output);
        assertEquals("s3n://basekb-sandbox/wayOut",that.output);
    }
}

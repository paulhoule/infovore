package com.ontology2.bakemono;

import com.ontology2.haruhi.flows.Flow;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;


public class TestMetadataContext {

    Flow basekbNowFlow;
    @Before
    public void before() throws FileNotFoundException {
        String fileTarget=null;
        String[] possibleTargets=new String[] {
            "src/metadata/resources/com/ontology2/bakemono/metadataContext.xml",
            "bakemono/src/metadata/resources/com/ontology2/bakemono/metadataContext.xml"
        };
        for(String target:possibleTargets)
            if(new File(target).exists())
                fileTarget=target;

        if(fileTarget==null)
            throw new FileNotFoundException();

        ApplicationContext ctx=new FileSystemXmlApplicationContext(
            fileTarget
        );

        basekbNowFlow=ctx.getBean("basekbNowFlow",Flow.class);
    }
    @Test
    public void testBaseKBNowFlow() {
        assertFalse(null==basekbNowFlow);
        assertEquals(4, basekbNowFlow.generateSteps("a", "b", "c").size());
    }
}

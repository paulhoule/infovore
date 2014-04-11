package com.ontology2.bakemono.dbpediaToBaseKB;

import com.ontology2.bakemono.baseKBToDBpedia.BaseKBToDBpediaTool;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations="../applicationContext.xml")
public class TestTool {
    @Resource(name="baseKBToDBpedia")
    public BaseKBToDBpediaTool tool;

    @Test
    public void itIsThere() {
        assertNotNull(tool);
    }
}

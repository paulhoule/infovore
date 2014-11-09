package com.ontology2.haruhi.flows;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"../shell/applicationContext.xml","../shell/testDefaults.xml"})

public class AssignmentStepTest {
    @Autowired AssignmentStep assignmentStep1;
    @Autowired AssignmentStep assignmentStep2;
    
    @Test
    public void step1ZappaIs11Int() {
        Map<String,Object> out=assignmentStep1.process(new HashMap<String,Object>(), new ArrayList<String>());
        assertEquals(1,out.size());
        assertEquals(11,out.get("zappa"));
    };
    
    @Test(expected=IllegalArgumentException.class)
    public void step1FailsIfZappaAlreadyExists() {
        HashMap<String, Object> input = Maps.newHashMap();
        input.put("zappa", "frank");
        Map<String,Object> out=assignmentStep1.process(input, new ArrayList<String>());
    };
    
    @Test
    public void step2SupportsExistingArguments() {
        HashMap<String, Object> input = Maps.newHashMap();
        input.put("justice", 8);
        input.put("endymion", "hope");
        
        List<String> positional=Lists.newArrayList("2","great power of");
        assertEquals("great power of",positional.get(1));
        Map<String,Object> out=assignmentStep2.process(input, positional);
        assertEquals(2,input.size());
        assertEquals(4,out.size());
        assertEquals(8,out.get("justice"));
        assertEquals("hope",out.get("endymion"));
        assertEquals("22222222",out.get("usagi"));
        assertEquals("great power of hope",out.get("mamoru"));  // yes, i mixed up my series
    }
}

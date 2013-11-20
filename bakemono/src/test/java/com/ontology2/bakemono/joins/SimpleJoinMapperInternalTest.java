package com.ontology2.bakemono.joins;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.VIntWritable;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static com.ontology2.bakemono.joins.SetJoinMapper.*;

public class SimpleJoinMapperInternalTest {
    @Test
    public void splitsCorrectly() {
        String in=INPUTS+".22";
        assertEquals("22",lastSegment(in));
    }

    @Test
    public void testMapping() throws IOException, InterruptedException {
        Configuration c=new Configuration();
        c.set(INPUTS+".1","your,brain,on,drugs");
        c.set(INPUTS+".2","very,last,factory");
        c.set(INPUTS+".3","belong");

        Map<String,VIntWritable> output=getPathMapping(c);
        assertEquals(
            getExample()
            ,output
        );
    }

    private HashMap<String, VIntWritable> getExample() {
        return new HashMap<String,VIntWritable>() {{
            put("your",new VIntWritable(1));
            put("brain",new VIntWritable(1));
            put("on",new VIntWritable(1));
            put("drugs",new VIntWritable(1));
            put("very",new VIntWritable(2));
            put("last",new VIntWritable(2));
            put("factory",new VIntWritable(2));
            put("belong",new VIntWritable(3));
        }};
    }

    @Test
    public void mappingGetsAppliedProperly() {
        assertEquals(
            2,
            determineTag(getExample(),"factory/diamond.dogs").get()
        );
    }

    @Test
    public void ZeroIfNoMatch() {
        assertEquals(
                0,
                determineTag(getExample(),"funcktion/face.to.face").get()
        );
    }

}

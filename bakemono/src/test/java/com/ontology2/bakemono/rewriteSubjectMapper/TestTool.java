package com.ontology2.bakemono.rewriteSubjectMapper;

import com.ontology2.bakemono.rewriteSubject.RewriteSubjectTool;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.junit.Test;

import static junit.framework.TestCase.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("../applicationContext.xml")
public class TestTool {
    @Autowired
    public RewriteSubjectTool smushSubject;

    @Test
    public void getsInitialized() {
        assertNotNull(smushSubject);
    }

    @Test
    public void eatsText() {
        assertEquals(TextInputFormat.class,smushSubject.getInputFormatClass());
    }

    @Test @Ignore
    public void hasLongWritableMapInputKey() {
        assertEquals(LongWritable.class,smushSubject.getMapInputKeyClass());
    }

    @Test @Ignore
    public void hasTextMapInputValue() {
        assertEquals(LongWritable.class,smushSubject.getMapInputValueClass());
    }
}

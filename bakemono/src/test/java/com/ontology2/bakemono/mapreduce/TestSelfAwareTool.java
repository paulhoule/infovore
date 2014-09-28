package com.ontology2.bakemono.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static junit.framework.TestCase.*;

public class TestSelfAwareTool {
    TestTool that;

    @Before
    public void setup() {
        that=new TestTool();
        that.setBeanName("wildIsTheWind");
        that.options=new TestOptions();
    }

    @Test
    public void getTypeArguments() {
        // Creating an anonymous class reifies the type option
        List<String> target=new ArrayList<String>() {{
            add("thimble");
        }};

        Type[] arguments= TypeDetective.sniffTypeParameters(target.getClass(), ArrayList.class);
        assertEquals(1, arguments.length);
        assertEquals(String.class,arguments[0]);
    }

    @Test(expected=SelfAwareTool.NoGenericTypeInformationAvailable.class)
    public void detectCommonGenericsMistake() {
        List<String> target=newArrayList();
        Type[] arguments= TypeDetective.sniffTypeParameters(target.getClass(), ArrayList.class);
        assertEquals(1,arguments.length);
        assertEquals(String.class,arguments[0]);
    }

    @Test
    public void optionType() {
        assertEquals(TestOptions.class, that.getOptionsClass());
    }

    @Test
    public void mapperClass() {
        assertEquals(TestMapper.class,that.getMapperClass());
    }

    @Test
    public void reducerClass() {
        assertEquals(TestReducer.class,that.getReducerClass());
    }

    @Test
    public void beanName() {
        assertEquals("wildIsTheWind",that.getName());
    }

    @Test
    public void mapOutputKeyClass() {
        assertEquals(FloatWritable.class,that.getMapOutputKeyClass());
    }

    @Test
    public void mapOutputValueClass() {
        assertEquals(BloomFilter.class,that.getMapOutputValueClass());
    }

    @Test
    public void outputKeyClass() {
        assertEquals(VIntWritable.class,that.getOutputKeyClass());
    }

    @Test
    public void outputValueClass() {
        assertEquals(DoubleWritable.class,that.getOutputValueClass());
    }

    @Test
    public void inputs() {
        Iterator<Path> inputs=that.getInputPaths().iterator();
        assertEquals(new Path("/are"), inputs.next());
        assertEquals(new Path("/friends"),inputs.next());
        assertEquals(new Path("/electric"),inputs.next());
        assertFalse(inputs.hasNext());
    } 
    @Test
    public void output() {
        assertEquals(new Path("/horseheads"),that.getOutputPath());
    }

    @Test
    public void reduceTasks() {
        assertEquals(33,that.getNumReduceTasks());
    }

    @Test
    public void inputFormat() {
        assertEquals(TextInputFormat.class,that.getInputFormatClass());
    }
}

package com.ontology2.bakemono.smushObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;

public class TestSmushMapper {
    SmushObjectMapper mapper;
    Mapper<LongWritable,Text,Text,Text>.Context context;

    @Before
    public void setup() {
        mapper=new SmushObjectMapper();
        context=mock(Mapper.Context.class);
    }

    @Test
    public void splitOrdinaryStatement() {
        Text input=new Text("<http://example.com/bo_peep>\t<http://unknown.org/predicate>\t<http://yahoo.com/yyyyy/>\t.");
        Map.Entry<Text,Text> output=mapper.splitValue(input,new VIntWritable(16));
        assertEquals("<http://yahoo.com/yyyyy/>",output.getKey().toString());
        assertEquals(input,output.getValue());
    }

    @Test
    public void splitSameAsStatement() {
        Text input=new Text("<http://aristotle.org/Alcohol>\t<http://www.w3.org/2002/07/owl#sameAs>\t<http://aristotle.org/EtOH>\t.");
        Map.Entry<Text,Text> output=mapper.splitValue(input,new VIntWritable(1));
        assertEquals("<http://aristotle.org/Alcohol>",output.getKey().toString());
        assertEquals(input,output.getValue());
    }
}

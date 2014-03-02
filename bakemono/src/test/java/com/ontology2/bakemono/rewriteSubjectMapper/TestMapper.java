package com.ontology2.bakemono.rewriteSubjectMapper;


import com.ontology2.bakemono.rewriteSubject.RewriteSubjectMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;

//
// The assumption here,  right or wrong,  is that the base class of the mapper is correct and the
// one thing that needs to be tested is the split function
//
public class TestMapper {
    RewriteSubjectMapper mapper;
    Mapper<LongWritable,Text,Text,Text>.Context context;

    @Before
    public void setup() {
        mapper=new RewriteSubjectMapper();
        context=mock(Mapper.Context.class);
    }

    @Test
    public void splitStatement() {
        Text input=new Text("<http://example.com/bo_peep>\t<http://unknown.org/predicate>\t<http://yahoo.com/yyyyy/>\t.");
        Map.Entry<Text,Text> output=mapper.splitValue(input,null);
        assertEquals("<http://example.com/bo_peep>",output.getKey().toString());
        assertEquals(input,output.getValue());
    }
}

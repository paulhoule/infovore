package com.ontology2.bakemono.mapmap;

import static org.junit.Assert.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class UniqueUriObjectMapperTest {
    UniqueURIObjectMapper mapper;

    @Before
    public void setup() {
        mapper=new UniqueURIObjectMapper();
    }

    @Test
         public void testURITriple() throws IOException, InterruptedException {
        Text t=new Text("<http://a.example.com/>\t<http://b.example.com/>\t<http://c.example.com/> .");
        Mapper.Context c=mock(Mapper.Context.class);
        mapper.map(new LongWritable(75),t,c);
        verify(c).write(new Text("<http://c.example.com/>"),new LongWritable(1));
        verifyNoMoreInteractions(c);
    }

    @Test
    public void testLiteralTriple() throws IOException, InterruptedException {
        Text t=new Text("<http://a.example.com/>\t<http://b.example.com/>\t\"even on the darkest night\"@en .");
        Mapper.Context c=mock(Mapper.Context.class);
        mapper.map(new LongWritable(75),t,c);
        verifyNoMoreInteractions(c);
    }
}

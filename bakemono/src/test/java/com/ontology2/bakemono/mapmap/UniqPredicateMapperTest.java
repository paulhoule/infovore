package com.ontology2.bakemono.mapmap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class UniqPredicateMapperTest {
    UniqueURIPredicateMapper mapper;
    @Before
    public void setup() {
        mapper=new UniqueURIPredicateMapper();
    }

    @Test
    public void testExternalURITriple() throws IOException, InterruptedException {
        Text t=new Text("<http://a.example.com/>\t<http://b.example.com/>\t<http://c.example.com/> .");
        Mapper.Context c=mock(Mapper.Context.class);
        mapper.map(new LongWritable(75),t,c);
        verify(c).write(new Text("<http://b.example.com/>"),new LongWritable(1));
        verifyNoMoreInteractions(c);
    }

}

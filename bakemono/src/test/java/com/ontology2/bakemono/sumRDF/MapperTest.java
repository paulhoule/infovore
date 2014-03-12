package com.ontology2.bakemono.sumRDF;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class MapperTest {
    SumRDFMapper mapper;
    Mapper<LongWritable,Text,Text,FloatWritable>.Context context;

    @Before
    public void setup() {
        mapper=new SumRDFMapper();
        context=mock(Mapper.Context.class);
    }

    @Test
    public void tryOne() throws IOException, InterruptedException {
        Text triple=new Text("<http://example.com/>\t<http://rdf.basekb.com/public/subjectiveEye3D>\t\"3.14159\"^^xsd:float\t.");
        mapper.map(new LongWritable(99),triple,context);
        verify(context).write(new Text("<http://example.com/>"),new FloatWritable(3.14159F));
    }

    @Test
    public void rejectsOtherPredicate() throws IOException, InterruptedException {
        Text triple=new Text("<http://example.com/>\t<http://rdf.basekb.com/public/subjectiveEye4D>\t\"3.14159\"^^xsd:float\t.");
        mapper.map(new LongWritable(99),triple,context);
        verifyNoMoreInteractions(context);
    }



}

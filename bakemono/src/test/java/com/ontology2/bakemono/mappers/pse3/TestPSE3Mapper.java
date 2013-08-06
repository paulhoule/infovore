package com.ontology2.bakemono.mappers.pse3;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.mockito.Mockito.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.bakemono.abstractions.KeyValueAcceptor;
import com.ontology2.bakemono.jena.NodePair;
import com.ontology2.bakemono.mappers.pse3.PSE3Mapper;



public class TestPSE3Mapper {

    PSE3Mapper pse3mapper;

    @Before
    public void setup() {
        pse3mapper=new PSE3Mapper();
        pse3mapper.accepted=mock(KeyValueAcceptor.class);
    }

    @Test
    public void touchlessAtInitalization() {
        verifyNoMoreInteractions(pse3mapper.accepted);
    }


    @Test
    public void acceptsAGoodTriple() throws IOException, InterruptedException {
        Context mockContext=mock(Context.class);
        pse3mapper.map(
                new LongWritable(944L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t<http://example.com/C>."),
                mockContext);

        verify(pse3mapper.accepted).write(
                new Triple(
                        Node_URI.createURI("http://example.com/A")
                        ,Node_URI.createURI("http://example.com/B")
                        ,Node_URI.createURI("http://example.com/C")
                 )
                ,new LongWritable(1)
                ,mockContext);
        verifyNoMoreInteractions(pse3mapper.accepted);
    }

    @Test
    public void rejectsABadTriple() throws IOException, InterruptedException {
        Context mockContext=mock(Context.class);
        pse3mapper.map(
                new LongWritable(24562L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t\"2001-06\"^^xsd:datetime."),
                mockContext);

        verify(mockContext).getCounter(PSE3Counters.REJECTED);

        verifyNoMoreInteractions(pse3mapper.accepted);
    }

    @After
    public void tearDown() {
    }

}

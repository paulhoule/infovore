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
import com.ontology2.bakemono.abstractions.KeyValueAcceptor;
import com.ontology2.bakemono.jena.NodePair;
import com.ontology2.bakemono.mappers.pse3.PSE3Mapper;



public class TestPSE3Mapper {

    PSE3Mapper pse3mapper;

    @Before
    public void setup() {
        pse3mapper=new PSE3Mapper();
        pse3mapper.mos=mock(MultipleOutputs.class);
        pse3mapper.accepted=mock(KeyValueAcceptor.class);
        pse3mapper.rejected=mock(KeyValueAcceptor.class);
    }

    @Test
    public void touchlessAtInitalization() {
        verifyNoMoreInteractions(pse3mapper.mos);
        verifyNoMoreInteractions(pse3mapper.accepted);
        verifyNoMoreInteractions(pse3mapper.rejected);
    }

    @Test
    public void closesMosOnShutdown() throws IOException, InterruptedException {
        pse3mapper.cleanup(mock(Context.class));
        verify(pse3mapper.mos).close();
        verifyNoMoreInteractions(pse3mapper.mos);
    }

    @Test
    public void acceptsAGoodTriple() throws IOException, InterruptedException {
        pse3mapper.map(
                new LongWritable(944L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t<http://example.com/C>."),
                mock(Context.class));
        verifyNoMoreInteractions(pse3mapper.rejected);
        verify(pse3mapper.accepted).write(
                Node_URI.createURI("http://example.com/A"),
                new NodePair(
                        Node_URI.createURI("http://example.com/B"),
                        Node_URI.createURI("http://example.com/C")
                        ));
        verifyNoMoreInteractions(pse3mapper.accepted);
    }

    @Test
    public void rejectsABadTriple() throws IOException, InterruptedException {
        pse3mapper.map(
                new LongWritable(24562L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t\"2001-06\"^^xsd:datetime."),
                mock(Context.class));

        verify(pse3mapper.rejected).write(
                new Text("<http://example.com/A>"),
                new Text("<http://example.com/B>\t\"2001-06\"^^xsd:datetime.")
                );

        verifyNoMoreInteractions(pse3mapper.accepted);
        verifyNoMoreInteractions(pse3mapper.rejected);
    }

    @After
    public void tearDown() {

    }

}

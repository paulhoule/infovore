package com.ontology2.bakemono.pse3;

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
import com.ontology2.bakemono.jena.WritableTriple;
import com.ontology2.bakemono.mapred.RealMultipleOutputs;
import com.ontology2.bakemono.pse3.PSE3Mapper;



public class TestPSE3Mapper {

    PSE3Mapper pse3mapper;

    @Before
    public void setup() {
        pse3mapper=new PSE3Mapper();
        pse3mapper.mos=mock(RealMultipleOutputs.class);
        pse3mapper.accepted=mock(KeyValueAcceptor.class);
        pse3mapper.rejected=mock(KeyValueAcceptor.class);
    }

    @Test
    public void touchlessAtInitalization() {
        verifyNoMoreInteractions(pse3mapper.accepted);
        verifyNoMoreInteractions(pse3mapper.rejected);
    }


    @Test
    public void acceptsAGoodTriple() throws IOException, InterruptedException {
        Context mockContext=mock(Context.class);
        pse3mapper.map(
                new LongWritable(944L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t<http://example.com/C>."),
                mockContext);

        verify(pse3mapper.accepted).write(
                new WritableTriple(
                        Node_URI.createURI("http://example.com/A")
                        ,Node_URI.createURI("http://example.com/B")
                        ,Node_URI.createURI("http://example.com/C")
                 )
                ,new LongWritable(1)
                ,mockContext);
        verifyNoMoreInteractions(pse3mapper.accepted);
        verifyNoMoreInteractions(pse3mapper.rejected);
    }

    @Test
    public void acceptsARealFreebaseTriple() throws IOException,InterruptedException {
        Context mockContext=mock(Context.class);
        pse3mapper.map(
                new LongWritable(944L),
                new Text("<http://rdf.basekb.com/ns/m.06fm3lj>\t<http://rdf.basekb.com/ns/book.written_work.author>\t<http://rdf.basekb.com/ns/m.03qp7yf>."),
                mockContext);

        verify(pse3mapper.accepted).write(
                new WritableTriple(
                        Node_URI.createURI("http://rdf.basekb.com/ns/m.06fm3lj")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/book.written_work.author")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/m.03qp7yf")
                 )
                ,new LongWritable(1)
                ,mockContext);
        verifyNoMoreInteractions(pse3mapper.accepted);      
    }
    
    @Test
    public void acceptsARealFreebaseTripleWithSpaces() throws IOException,InterruptedException {
        Context mockContext=mock(Context.class);
        pse3mapper.map(
                new LongWritable(944L),
                new Text("<http://rdf.basekb.com/ns/m.06fm3lj> <http://rdf.basekb.com/ns/book.written_work.author> <http://rdf.basekb.com/ns/m.03qp7yf>."),
                mockContext);

        verify(pse3mapper.accepted).write(
                new WritableTriple(
                        Node_URI.createURI("http://rdf.basekb.com/ns/m.06fm3lj")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/book.written_work.author")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/m.03qp7yf")
                 )
                ,new LongWritable(1)
                ,mockContext);
        verifyNoMoreInteractions(pse3mapper.accepted);      
    }
    
    @Test
    public void closesMosOnShutdown() throws IOException, InterruptedException {
        pse3mapper.cleanup(mock(Context.class));
        verify(pse3mapper.mos).close();
        verifyNoMoreInteractions(pse3mapper.mos);
    }

    @Test
    public void rejectsABadTriple() throws IOException, InterruptedException {
        Context mockContext=mock(Context.class);
        pse3mapper.map(
                new LongWritable(24562L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t\"2001-06\"^^xsd:datetime."),
                mockContext);

        verify(pse3mapper.rejected).write(
                new Text("<http://example.com/A>")
                ,new Text("<http://example.com/B>\t\"2001-06\"^^xsd:datetime\t.")
                ,mockContext
        );


        verifyNoMoreInteractions(pse3mapper.accepted);
        verifyNoMoreInteractions(pse3mapper.rejected);

    }

    @After
    public void tearDown() {
    }

}

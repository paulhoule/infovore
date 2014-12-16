package com.ontology2.bakemono.pse3;

import com.google.common.base.Strings;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node_URI;
import com.ontology2.bakemono.abstractions.KeyValueAcceptor;
import com.ontology2.bakemono.jena.WritableTriple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.google.common.base.Strings.*;
import static org.mockito.Mockito.*;

public class TestPSE3Mapper {

    static final int MAX_STRING_LENGTH=63999;

    PSE3Mapper pse3mapper;
    Mapper.Context mockContext;

    @Before
    public void setup() {
        pse3mapper=new PSE3Mapper();
        pse3mapper.accepted=mock(KeyValueAcceptor.class);
        mockContext=mock(Mapper.Context.class);
    }

    @Test
    public void touchlessAtInitalization() {
        verifyNoMoreInteractions(pse3mapper.accepted);
    }


    @Test
    public void acceptsAGoodTriple() throws IOException, InterruptedException {
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
                ,new WritableTriple(
                        Node_URI.createURI("http://example.com/A")
                        ,Node_URI.createURI("http://example.com/B")
                        ,Node_URI.createURI("http://example.com/C")
                )
                ,mockContext);
        verifyNoMoreInteractions(pse3mapper.accepted);
    }

    @Test
    public void acceptsARealFreebaseTriple() throws IOException,InterruptedException {
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
                ,new WritableTriple(
                        Node_URI.createURI("http://rdf.basekb.com/ns/m.06fm3lj")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/book.written_work.author")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/m.03qp7yf")
                )
                ,mockContext);
        verifyNoMoreInteractions(pse3mapper.accepted);
    }
    
    @Test
    public void arnoldSchwarzeneggerIsAFilmActor() throws IOException, InterruptedException {
        pse3mapper.map(
                new LongWritable(944L),
                new Text("<http://rdf.basekb.com/ns/m.0tc7> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://rdf.basekb.com/ns/film.actor>."),
                mockContext);
        
        verify(pse3mapper.accepted).write(
                new WritableTriple(
                        Node_URI.createURI("http://rdf.basekb.com/ns/m.0tc7")
                        ,Node_URI.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/film.actor")
                 )
                ,new WritableTriple(
                        Node_URI.createURI("http://rdf.basekb.com/ns/m.0tc7")
                        ,Node_URI.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/film.actor")
                )
                ,mockContext);
        verifyNoMoreInteractions(pse3mapper.accepted);
    }
    
    @Test
    public void acceptsARealFreebaseTripleWithSpaces() throws IOException,InterruptedException {
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
                ,new WritableTriple(
                        Node_URI.createURI("http://rdf.basekb.com/ns/m.06fm3lj")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/book.written_work.author")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/m.03qp7yf")
                )
                ,mockContext);
        verifyNoMoreInteractions(pse3mapper.accepted);
    }
    
    @Test
    public void rejects$Escapes() throws IOException,InterruptedException {
        pse3mapper.map(
                new LongWritable(944L),
                new Text("<http://rdf.basekb.com/ns/m.0tc7> <http://rdf.basekb.com/ns/common.topic.topic_equivalent_webpage> <http://www.ranker.com/review/arnold-schwarzenegger$002F493404> ."),
                mockContext);

        verify(pse3mapper.accepted).write(
                new WritableTriple(
                        Node_URI.createURI("http://rdf.basekb.com/ns/m.0tc7")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/common.topic.topic_equivalent_webpage")
                        ,Node_URI.createURI("http://www.ranker.com/review/arnold-schwarzenegger/493404")
                )
                ,new WritableTriple(
                        Node_URI.createURI("http://rdf.basekb.com/ns/m.0tc7")
                        ,Node_URI.createURI("http://rdf.basekb.com/ns/common.topic.topic_equivalent_webpage")
                        ,Node_URI.createURI("http://www.ranker.com/review/arnold-schwarzenegger/493404")
                )
                ,mockContext);
        verifyNoMoreInteractions(pse3mapper.accepted);
    }
    
    @Test
    public void closesMosOnShutdown() throws IOException, InterruptedException {
        pse3mapper.cleanup(mock(Context.class));
    }

    @Test
    public void rejectsABadTriple() throws IOException, InterruptedException {
        pse3mapper.map(
                new LongWritable(24562L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t\"2001-06\"^^xsd:datetime."),
                mockContext);

        verifyNoMoreInteractions(pse3mapper.accepted);
    }

    @Test
         public void rejectsABadDate() throws IOException, InterruptedException {
        pse3mapper.map(
                new LongWritable(24562L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t\"2001-06\"^^<http://www.w3.org/2001/XMLSchema#dateTime>."),
                mockContext);

        verifyNoMoreInteractions(pse3mapper.accepted);
    }

    @Test
    public void rejectsOversizedStrings() throws IOException, InterruptedException {
        pse3mapper.map(
                new LongWritable(24562L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t\""+ repeat("x", 70000)+"\"@en."),
                mockContext);

        verifyNoMoreInteractions(pse3mapper.accepted);
    }

    @Test
    public void acceptsAGoodDateTime() throws IOException, InterruptedException {
        pse3mapper.map(
                new LongWritable(24562L),
                new Text("<http://example.com/A>\t<http://example.com/B>\t\"2001-06-01T13:11:12Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>."),
                mockContext);

        verify(pse3mapper.accepted).write(
                new WritableTriple(
                        Node_URI.createURI("http://example.com/A")
                        , Node_URI.createURI("http://example.com/B")
                        , Node_URI.createLiteral("2001-06-01T13:11:12Z", XSDDatatype.XSDdateTime)
                )
                , new WritableTriple(
                        Node_URI.createURI("http://example.com/A")
                        , Node_URI.createURI("http://example.com/B")
                        , Node_URI.createLiteral("2001-06-01T13:11:12Z", XSDDatatype.XSDdateTime)
                )
                , mockContext);
    }

    @After
    public void tearDown() {
    }

}

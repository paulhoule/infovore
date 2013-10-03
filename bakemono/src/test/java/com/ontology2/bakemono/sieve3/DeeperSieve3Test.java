package com.ontology2.bakemono.sieve3;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node_URI;
import com.ontology2.bakemono.abstractions.KeyValueAcceptor;
import com.ontology2.bakemono.jena.WritableTriple;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.bakemono.sieve3.Sieve3Configuration.Rule;

public class DeeperSieve3Test {
    private Sieve3Mapper sieve3;
    private Context context;

    @Before
    public void setup() throws IOException, InterruptedException {
        sieve3 = new Sieve3Mapper();
        Configuration c=new Configuration();

        // we get in trouble if the TaskAttemptID is null,  but initialization seems to take
        // much longer if we don't mock the InputSplit -- is it mockito hanging up or something
        // strange happening in initialization?
        
        context = sieve3.new Context(
                c, 
                mock(TaskAttemptID.class),
                null, // mock(RecordReader.class), 
                null, // mock(RecordWriter.class),
                null, // mock(OutputCommitter.class),
                null, // mock(StatusReporter.class),
                mock(InputSplit.class));

        sieve3.setup(context);
        sieve3.outputs.clear();
        sieve3.outputs.put("a",mock(KeyValueAcceptor.class));
        sieve3.outputs.put("label", mock(KeyValueAcceptor.class));
        sieve3.outputs.put("name",mock(KeyValueAcceptor.class));
        sieve3.outputs.put("keyNs", mock(KeyValueAcceptor.class));
        sieve3.outputs.put("key",mock(KeyValueAcceptor.class));
        sieve3.outputs.put("notableForPredicate", mock(KeyValueAcceptor.class));
        sieve3.outputs.put("description",mock(KeyValueAcceptor.class));
        sieve3.outputs.put("text", mock(KeyValueAcceptor.class));
        sieve3.outputs.put("webpages", mock(KeyValueAcceptor.class));
        sieve3.outputs.put("notability",mock(KeyValueAcceptor.class));
        sieve3.outputs.put("links", mock(KeyValueAcceptor.class));
        
        sieve3.other=mock(KeyValueAcceptor.class);
    }
    
    @Test
    public void a() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(88), 
                new Text("<http://example.com/A>\t<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://example.com/B>\t."), 
                context);
        verifyNoMoreInteractions(sieve3.outputs.get("label"));
        verify(sieve3.outputs.get("a")).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                        "<http://example.com/B>"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void schwarzeneggerIsAFilmActor() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(88), 
                new Text("<http://rdf.basekb.com/ns/m.0tc7>\t<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://rdf.basekb.com/ns/film.actor>\t."), 
                context);
        verifyNoMoreInteractions(sieve3.outputs.get("label"));
        verify(sieve3.outputs.get("a")).write(
                eq(new PrimitiveTriple(
                        "<http://rdf.basekb.com/ns/m.0tc7>",
                        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                        "<http://rdf.basekb.com/ns/film.actor>"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void label() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(88), 
                new Text("<http://example.com/A>\t<http://www.w3.org/2000/01/rdf-schema#label>\t\"Freddy\"\t."), 
                context);
        verifyNoMoreInteractions(sieve3.outputs.get("a"));
        verify(sieve3.outputs.get("label")).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://www.w3.org/2000/01/rdf-schema#label>",
                        "\"Freddy\""
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void name() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(88), 
                new Text("<http://example.com/A>\t<http://rdf.basekb.com/ns/type.object.name>\t\"Asahina Mikuru\"\t."), 
                context);
        verifyNoMoreInteractions(sieve3.outputs.get("a"));
        verifyNoMoreInteractions(sieve3.outputs.get("label"));
        verify(sieve3.outputs.get("name")).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://rdf.basekb.com/ns/type.object.name>",
                        "\"Asahina Mikuru\""
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void keyNs() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(88), 
                new Text("<http://example.com/A>\t<http://rdf.basekb.com/key/key.on.drugs>\t\"000000000\"\t."), 
                context);
        verifyNoMoreInteractions(sieve3.outputs.get("a"));
        verifyNoMoreInteractions(sieve3.outputs.get("label"));
        verify(sieve3.outputs.get("keyNs")).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://rdf.basekb.com/key/key.on.drugs>",
                        "\"000000000\""
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void notableForPredicate() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(88), 
                new Text("<http://example.com/A>\t<http://rdf.basekb.com/ns/common.notable_for.predicate>\t\"/krs1/is/slammin\"\t."), 
                context);
        verifyNoMoreInteractions(sieve3.outputs.get("a"));
        verifyNoMoreInteractions(sieve3.outputs.get("label"));
        verifyNoMoreInteractions(sieve3.outputs.get("keyNs"));
        verify(sieve3.outputs.get("notableForPredicate")).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://rdf.basekb.com/ns/common.notable_for.predicate>",
                        "\"/krs1/is/slammin\""
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void description() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(75),
                new Text("<http://example.com/A>\t<http://rdf.basekb.com/ns/common.topic.description>\t\"Some description I made up for a URI in the standard example namespace.\"@en\t.")
                ,context);
        untouchedExceptFor("description");
        verify(sieve3.outputs.get("description")).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://rdf.basekb.com/ns/common.topic.description>",
                        "\"Some description I made up for a URI in the standard example namespace.\"@en"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void text() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(75),
                new Text("<http://example.com/A>\t<http://rdf.basekb.com/ns/common.document.text>\t\"Some description I made up for a URI in the standard example namespace.\"@en\t.")
                ,context);
        untouchedExceptFor("text");
        verify(sieve3.outputs.get("text")).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://rdf.basekb.com/ns/common.document.text>",
                        "\"Some description I made up for a URI in the standard example namespace.\"@en"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void notability1() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(75),
                new Text("<http://rdf.basekb.com/ns/m.010005>     <http://rdf.basekb.com/ns/common.topic.notable_types>        <http://en.wikipedia.org/wiki/index.html?curid=135765>  .")
                ,context);
        untouchedExceptFor("notability");
        verify(sieve3.outputs.get("notability")).write(
                eq(new PrimitiveTriple(
                        "<http://rdf.basekb.com/ns/m.010005>",
                        "<http://rdf.basekb.com/ns/common.topic.notable_types>",
                        "<http://en.wikipedia.org/wiki/index.html?curid=135765>"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }

    @Test
    public void notability2() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(75),
                new Text("<http://rdf.basekb.com/ns/m.010005>     <http://rdf.basekb.com/ns/common.notable_for.object>        <http://en.wikipedia.org/wiki/index.html?curid=135765>  .")
                ,context);
        untouchedExceptFor("notability");
        verify(sieve3.outputs.get("notability")).write(
                eq(new PrimitiveTriple(
                        "<http://rdf.basekb.com/ns/m.010005>",
                        "<http://rdf.basekb.com/ns/common.notable_for.object>",
                        "<http://en.wikipedia.org/wiki/index.html?curid=135765>"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void notability3() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(75),
                new Text("<http://rdf.basekb.com/ns/m.010005>     <http://rdf.basekb.com/ns/common.topic.notable_for>       <http://en.wikipedia.org/wiki/index.html?curid=135765>  .")
                ,context);
        untouchedExceptFor("notability");
        verify(sieve3.outputs.get("notability")).write(
                eq(new PrimitiveTriple(
                        "<http://rdf.basekb.com/ns/m.010005>",
                        "<http://rdf.basekb.com/ns/common.topic.notable_for>",
                        "<http://en.wikipedia.org/wiki/index.html?curid=135765>"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void notability4() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(75),
                new Text("<http://rdf.basekb.com/ns/m.010005>     <http://rdf.basekb.com/ns/common.topic.notable_for.notable_object>       <http://en.wikipedia.org/wiki/index.html?curid=135765>  .")
                ,context);
        untouchedExceptFor("notability");
        verify(sieve3.outputs.get("notability")).write(
                eq(new PrimitiveTriple(
                        "<http://rdf.basekb.com/ns/m.010005>",
                        "<http://rdf.basekb.com/ns/common.topic.notable_for.notable_object>",
                        "<http://en.wikipedia.org/wiki/index.html?curid=135765>"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    @Test
    public void webpages() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(75),
                new Text("<http://rdf.basekb.com/ns/m.010005>     <http://rdf.basekb.com/ns/common.topic.topic_equivalent_webpage>        <http://en.wikipedia.org/wiki/index.html?curid=135765>  .")
                ,context);
        untouchedExceptFor("webpages");
        verify(sieve3.outputs.get("webpages")).write(
                eq(new PrimitiveTriple(
                        "<http://rdf.basekb.com/ns/m.010005>",
                        "<http://rdf.basekb.com/ns/common.topic.topic_equivalent_webpage>",
                        "<http://en.wikipedia.org/wiki/index.html?curid=135765>"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void links() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(88), 
                new Text("<http://example.com/A>\t<http://rdf.basekb.com/ns/protons>\t<http://example.com/B>\t."), 
                context);
        untouchedExceptFor("links");
        verify(sieve3.outputs.get("links")).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://rdf.basekb.com/ns/protons>",
                        "<http://example.com/B>"
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    @Test
    public void other() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(88), 
                new Text("<http://example.com/A>\t<http://rdf.basekb.com/ns/i.scream.for.ice.cream>\t\"this is the time and this is the record of the time\"\t."), 
                context);
        untouchedExceptFor("other");
        verify(sieve3.other).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://rdf.basekb.com/ns/i.scream.for.ice.cream>",
                        "\"this is the time and this is the record of the time\""
                )),
                eq(new LongWritable(1)),
                eq(context));
    }

    @Test
    public void key() throws IOException, InterruptedException {
        sieve3.map(
                new LongWritable(88), 
                new Text("<http://example.com/A>\t<http://rdf.basekb.com/ns/type.object.key>\t\"/bird/of/prey\"\t."), 
                context);
        untouchedExceptFor("key");
        verify(sieve3.outputs.get("key")).write(
                eq(new PrimitiveTriple(
                        "<http://example.com/A>",
                        "<http://rdf.basekb.com/ns/type.object.key>",
                        "\"/bird/of/prey\""
                )),
                eq(new LongWritable(1)),
                eq(context));
    }
    
    public void untouchedExceptFor(String key) {
        for(Entry<String, KeyValueAcceptor<PrimitiveTriple, LongWritable>> that:sieve3.outputs.entrySet()) {
            if (!that.getKey().equals(key))
                verifyNoMoreInteractions(that.getValue());
        }
        
        if (!key.equals("other")) {
            verifyNoMoreInteractions(sieve3.other);
        }
    }
}

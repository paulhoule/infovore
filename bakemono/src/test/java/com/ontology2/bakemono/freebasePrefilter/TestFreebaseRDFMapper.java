package com.ontology2.bakemono.freebasePrefilter;

import static com.ontology2.bakemono.freebasePrefilter.FreebaseRDFMapper.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import com.ontology2.bakemono.freebasePrefilter.FreebasePrefilterCounter;
import com.ontology2.bakemono.freebasePrefilter.FreebaseRDFMapper;
import com.ontology2.bakemono.primitiveTriples.PrimitiveTriple;
import com.ontology2.rdf.InvalidPrefixException;

public class TestFreebaseRDFMapper {

    private FreebaseRDFMapper mapper;
    private Context context;

    @Before
    public void setup() {
        mapper = new FreebaseRDFMapper();
        context = mock(Context.class);
        mapper.setup(context);
    }

    @Test
    public void testSplitPrefix() throws InvalidPrefixException {
        List<String> parts = splitPrefixDeclaration("@prefix foo: <http://bar.com/>.");
        assertEquals("foo", parts.get(1));
        assertEquals("http://bar.com/", parts.get(2));
    }

    @Test
    public void testSplitTriple() throws Exception {
        List<String> parts = splitTriple("ns:aviation.aircraft.first_flight\tns:type.property.unique\ttrue.");
        assertEquals(3, parts.size());
        assertEquals("ns:aviation.aircraft.first_flight", parts.get(0));
        assertEquals("ns:type.property.unique", parts.get(1));
        assertEquals("true", parts.get(2));
    }

    @Test
    public void testExpandTripleParts() throws Exception {
        List<String> parts = mapper
                .expandTripleParts("ns:aviation.aircraft.first_flight\tns:type.property.unique\ttrue.");
        assertEquals(3, parts.size());
        assertEquals(
                "<http://rdf.basekb.com/ns/aviation.aircraft.first_flight>",
                parts.get(0));
        assertEquals("<http://rdf.basekb.com/ns/type.property.unique>",
                parts.get(1));
        assertEquals("true", parts.get(2));
    }
    
    @Test
    public void testRecentFreebaseDumpFormat() throws Exception {
        List<String> parts = mapper
                .expandTripleParts("<http://rdf.freebase.com/ns/automotive.engine.engine_type>      <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>       <http://www.w3.org/2000/01/rdf-schema#Property> .");
        assertEquals(3, parts.size());
        assertEquals(
                "<http://rdf.basekb.com/ns/automotive.engine.engine_type>",
                parts.get(0));
        assertEquals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                parts.get(1));
        assertEquals("<http://www.w3.org/2000/01/rdf-schema#Property>", parts.get(2));
    }

    @Test
    public void testExpandNode() throws Exception {
        assertEquals("<http://www.w3.org/2000/01/rdf-schema#label>",
                mapper.expandIRINode("rdfs:label"));
        assertEquals("<http://rdf.freebase.com/ns/type.object.type>",
                mapper.expandIRINode("ns:type.object.type"));

    }

    @Test
    public void testExpandAnyNode() throws Exception {
        assertEquals("<http://www.w3.org/2000/01/rdf-schema#label>",
                mapper.expandAnyNode("rdfs:label"));
        assertEquals("<http://rdf.freebase.com/ns/type.object.type>",
                mapper.expandAnyNode("ns:type.object.type"));
        assertEquals("\"Number\"@en", mapper.expandAnyNode("\"Number\"@en"));

    }

    @Test
    public void rejectsCompleteGarbage() throws IOException,
    InterruptedException {
        mapper.map(new LongWritable(1L),
                new Text("Furkle murkle yurkle urkle"), context);
        verify(context).getCounter(FreebasePrefilterCounter.IGNORED);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void ignoresPrefixDeclarations() throws IOException,
    InterruptedException {
        mapper.map(new LongWritable(1L), new Text(
                "@prefix foo: <http://bar.com/>."), context);
        verify(context).getCounter(FreebasePrefilterCounter.PREFIX_DECL);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void acceptsOrdinaryTriples() throws IOException,
    InterruptedException {
        String ordinaryTriple = "ns:aviation.aircraft.first_flight\tns:type.property.unique\ttrue.";
        mapper.map(new LongWritable(1L), new Text(ordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.ACCEPTED);
        verify(context)
        .write(new Text(
                "<http://rdf.basekb.com/ns/aviation.aircraft.first_flight>"),
                new Text(
                        "<http://rdf.basekb.com/ns/type.property.unique>\ttrue."));
        verifyNoMoreInteractions(context);
    }

    @Test
    public void rejectsMostATriples() throws IOException, InterruptedException {
        String ordinaryTriple = "ns:aviation.aircraft.first_flight\trdf:type\tns:anythingInsideFreebase.";
        mapper.map(new LongWritable(1L), new Text(ordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.IGNORED);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void acceptsForeignATriples() throws IOException,
    InterruptedException {
        String ordinaryTriple = "ns:aviation.aircraft.first_flight\trdf:type\towl:Thing.";
        mapper.map(new LongWritable(1L), new Text(ordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.ACCEPTED);
        verify(context)
        .write(new Text(
                "<http://rdf.basekb.com/ns/aviation.aircraft.first_flight>"),
                new Text(
                        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://www.w3.org/2002/07/owl#Thing>."));
        verifyNoMoreInteractions(context);
    }

    @Test
    public void rejectTypeTypeInstanceOutsideFreebase() throws IOException,
    InterruptedException {
        String ordinaryTriple = "ns:aviation.aircraft.first_flight\tns:type.type.instance\towl:Thing.";
        mapper.map(new LongWritable(1L), new Text(ordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.IGNORED);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void ignoreTypeTypeExpectedBy() throws IOException,
    InterruptedException {
        String ordinaryTriple = "ns:aviation.aircraft.first_flight\tns:type.type.expected_by\towl:Thing.";
        mapper.map(new LongWritable(1L), new Text(ordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.IGNORED);
        verifyNoMoreInteractions(context);
    }
    
    @Test
    public void ignoreNotableForDisplayNames() throws IOException,
    InterruptedException {
        String ordinaryTriple = "ns:rock.me.amadeus\tns:common.notable_for.display_name\t\"Musikale Tracke\"@en";
        mapper.map(new LongWritable(1L), new Text(ordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.IGNORED);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void usuallyTypeObjectTypeRewritesToA() throws IOException,
    InterruptedException {
        String ordinaryTriple = "ns:aviation.aircraft.first_flight\tns:type.object.type\tns:whatever.";
        mapper.map(new LongWritable(1L), new Text(ordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.ACCEPTED);
        verify(context)
        .write(new Text(
                "<http://rdf.basekb.com/ns/aviation.aircraft.first_flight>"),
                new Text(
                        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://rdf.basekb.com/ns/whatever>."));
        verifyNoMoreInteractions(context);
    }

    @Test
    public void weReverseRedirects() throws IOException, InterruptedException {
        String ordinaryTriple = "ns:A\tns:dataworld.gardening_hint.replaced_by\tns:B.";
        mapper.map(new LongWritable(1L), new Text(ordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.ACCEPTED);
        verify(context)
        .write(new Text("<http://rdf.basekb.com/ns/B>"),
                new Text(
                        "<http://rdf.basekb.com/ns/m.0j2r8t8>\t<http://rdf.basekb.com/ns/A>."));
        verifyNoMoreInteractions(context);
    }

    @Test
    public void weReversePermissions() throws IOException, InterruptedException {
        String ordinaryTriple = "ns:A\tns:type.permission.controls\tns:B.";
        mapper.map(new LongWritable(1L), new Text(ordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.ACCEPTED);
        verify(context)
        .write(new Text("<http://rdf.basekb.com/ns/B>"),
                new Text(
                        "<http://rdf.basekb.com/ns/m.0j2r9sk>\t<http://rdf.basekb.com/ns/A>."));
        verifyNoMoreInteractions(context);
    }
    
    @Test
    public void arnoldSchwarzeneggerAfactIsIgnored() throws IOException, InterruptedException {
        String extraordinaryTriple = "<http://rdf.freebase.com/ns/m.0tc7> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdf.freebase.com/ns/film.actor> .";
        mapper.map(new LongWritable(1940L),new Text(extraordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.IGNORED);
        verifyNoMoreInteractions(context);
    }
    
    @Test
    public void arnoldSchwarzeneggerIsAFilmActor() throws IOException, InterruptedException {
        String extraordinaryTriple = "<http://rdf.freebase.com/ns/m.0tc7> <http://rdf.freebase.com/ns/type.object.type> <http://rdf.freebase.com/ns/film.actor> .";
        mapper.map(new LongWritable(1940L),new Text(extraordinaryTriple), context);
        verify(context).getCounter(FreebasePrefilterCounter.ACCEPTED);
        verify(context)
        .write(new Text("<http://rdf.basekb.com/ns/m.0tc7>"),
                new Text(
                        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://rdf.basekb.com/ns/film.actor>."));
        verifyNoMoreInteractions(context);
    }


}

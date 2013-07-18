package com.ontology2.bakemono;

import static com.ontology2.bakemono.FreebaseRDFMapper.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

import com.ontology2.hydroxide.InvalidPrefixException;
import com.ontology2.hydroxide.fbRdfPartitioner.ExpandFreebaseRdfToNTriples;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.sink.ListSink;
import com.ontology2.millipede.sink.NullSink;

public class TestFreebaseRDFMapper {

	private FreebaseRDFMapper mapper;
	private OutputCollector<Text, Text> out;
	private Reporter meta;
	
	@Before
	public void setup() {
		mapper = new FreebaseRDFMapper();
		mapper.configure(new JobConf());
		out=mock(OutputCollector.class);
		meta=mock(Reporter.class);
	}
	
	@Test
	public void testSplitPrefix() throws InvalidPrefixException {
		List<String> parts=splitPrefixDeclaration("@prefix foo: <http://bar.com/>.");
		assertEquals("foo",parts.get(1));
		assertEquals("http://bar.com/",parts.get(2));
	}

	@Test
	public void testSplitTriple() throws Exception {
		List<String> parts=splitTriple("ns:aviation.aircraft.first_flight\tns:type.property.unique\ttrue.");
		assertEquals(3,parts.size());
		assertEquals("ns:aviation.aircraft.first_flight",parts.get(0));
		assertEquals("ns:type.property.unique",parts.get(1));
		assertEquals("true",parts.get(2));
	}
	
	@Test
	public void testExpandTripleParts() throws Exception {
		List<String> parts=mapper.expandTripleParts("ns:aviation.aircraft.first_flight\tns:type.property.unique\ttrue.");
		assertEquals(3,parts.size());
		assertEquals("<http://rdf.freebase.com/ns/aviation.aircraft.first_flight>",parts.get(0));
		assertEquals("<http://rdf.freebase.com/ns/type.property.unique>",parts.get(1));
		assertEquals("true",parts.get(2));
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
		assertEquals("\"Number\"@en",
				mapper.expandAnyNode("\"Number\"@en"));
		
	}
	
	@Test public  void rejectsCompleteGarbage() throws IOException {		
		mapper.map(new LongWritable(1L), new Text("Furkle murkle yurkle urkle"), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.ILL_FORMED, 1L);
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@Test public  void ignoresPrefixDeclarations() throws IOException {		
		mapper.map(new LongWritable(1L), new Text("@prefix foo: <http://bar.com/>."), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.PREFIX_DECL, 1L);
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@Test public void acceptsOrdinaryTriples() throws IOException {		
		String ordinaryTriple="ns:aviation.aircraft.first_flight\tns:type.property.unique\ttrue.";
		mapper.map(new LongWritable(1L), new Text(ordinaryTriple), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.ACCEPTED, 1L);
		verify(out).collect(
				new Text("<http://rdf.freebase.com/ns/aviation.aircraft.first_flight>"),
				new Text("<http://rdf.freebase.com/ns/type.property.unique>\ttrue."));
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@Test public void rejectsMostATriples() throws IOException {		
		String ordinaryTriple="ns:aviation.aircraft.first_flight\trdf:type\tns:anythingInsideFreebase.";
		mapper.map(new LongWritable(1L), new Text(ordinaryTriple), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.IGNORED, 1L);
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@Test public void acceptsForeignATriples() throws IOException {		
		String ordinaryTriple="ns:aviation.aircraft.first_flight\trdf:type\towl:Thing.";
		mapper.map(new LongWritable(1L), new Text(ordinaryTriple), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.ACCEPTED, 1L);
		verifyNoMoreInteractions(meta);
		verify(out).collect(
				new Text("<http://rdf.freebase.com/ns/aviation.aircraft.first_flight>"),
				new Text("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://www.w3.org/2002/07/owl#Thing>."));
		verifyNoMoreInteractions(out);
	}
	
	@Test public void reject() throws IOException {		
		String ordinaryTriple="ns:aviation.aircraft.first_flight\tns:type.type.instance\towl:Thing.";
		mapper.map(new LongWritable(1L), new Text(ordinaryTriple), out, meta);
		String anotherTriple="ns:aviation.aircraft.first_flight\tns:type.type.expected_by\towl:Thing.";
		verify(meta).incrCounter(FreebasePrefilterCounter.IGNORED, 1L);
		verify(meta).incrCounter(FreebasePrefilterCounter.IGNORED, 1L);
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@Test public void ignoreTypeTypeInstance() throws IOException {		
		String ordinaryTriple="ns:aviation.aircraft.first_flight\tns:type.type.instance\towl:Thing.";
		mapper.map(new LongWritable(1L), new Text(ordinaryTriple), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.IGNORED, 1L);
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@Test public void ignoreTypeTypeExpectedBy() throws IOException {		
		String ordinaryTriple="ns:aviation.aircraft.first_flight\tns:type.type.expected_by\towl:Thing.";
		mapper.map(new LongWritable(1L), new Text(ordinaryTriple), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.IGNORED, 1L);
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@Test public void weUseNativePredicateForAMostly() throws IOException {		
		String ordinaryTriple="ns:aviation.aircraft.first_flight\tns:type.object.type\tns:whatever.";
		mapper.map(new LongWritable(1L), new Text(ordinaryTriple), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.ACCEPTED, 1L);
		verify(out).collect(
				new Text("<http://rdf.freebase.com/ns/aviation.aircraft.first_flight>"),
				new Text("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t<http://rdf.freebase.com/ns/whatever>."));
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@Test public void weReverseRedirects() throws IOException {		
		String ordinaryTriple="ns:A\tns:dataworld.gardening_hint.replaced_by\tns:B.";
		mapper.map(new LongWritable(1L), new Text(ordinaryTriple), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.ACCEPTED, 1L);
		verify(out).collect(
				new Text("<http://rdf.freebase.com/ns/B>"),
				new Text("<http://rdf.freebase.com/ns/m.0j2r8t8>\t<http://rdf.freebase.com/ns/A>."));
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@Test public void weReversePermissions() throws IOException {		
		String ordinaryTriple="ns:A\tns:type.permission.controls\tns:B.";
		mapper.map(new LongWritable(1L), new Text(ordinaryTriple), out, meta);
		verify(meta).incrCounter(FreebasePrefilterCounter.ACCEPTED, 1L);
		verify(out).collect(
				new Text("<http://rdf.freebase.com/ns/B>"),
				new Text("<http://rdf.freebase.com/ns/m.0j2r9sk>\t<http://rdf.freebase.com/ns/A>."));
		verifyNoMoreInteractions(meta);
		verifyNoMoreInteractions(out);
	}
	
	@After
	public void tearDown() throws IOException {
		mapper.close();
	}


}

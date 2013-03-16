package com.ontology2.hydroxide;


import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.ontology2.hydroxide.files.ReadNTriples;
import com.ontology2.millipede.primitiveTriples.PrimitiveTriple;
import com.ontology2.millipede.sink.ListSink;
import com.ontology2.millipede.sink.NullSink;

public class ReadNTriplesTest {
	@Test
	public void test() throws Exception {
		String input="<http://dbpedia.org/property/proctor>\t <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\t <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> .";
		ListSink<PrimitiveTriple> innerSink=new ListSink<PrimitiveTriple>();
		ReadNTriples convert=new ReadNTriples(innerSink,new NullSink<String>());	
		convert.accept(input);
		List<PrimitiveTriple> output=innerSink.getContent();
		assertEquals(1,output.size());
		assertEquals("<http://dbpedia.org/property/proctor>",output.get(0).subject);
		assertEquals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",output.get(0).predicate);
		assertEquals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>",output.get(0).object);
	}

}

package com.ontology2.basekb.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.ontology2.basekb.jena.AnyQueryExecutionFactory;
import com.ontology2.basekb.jena.AnyQueryFactory;
import com.ontology2.basekb.jena.DefaultSpringConfiguration;
import com.ontology2.basekb.jena.RawConfiguration;
import com.ontology2.basekb.jena.VanillaQueryFactory;

public class JenaGroundedSparqlTest {
	private AnyQueryExecutionFactory sparql;
	private AnyQueryFactory queryFactory;
	
	@Before
	public void setUp() {
		RawConfiguration jConfig=DefaultSpringConfiguration.getInstance().getRawConfiguration();
		sparql=jConfig.getSparql();
		queryFactory=DefaultSpringConfiguration.getInstance().getGroundedQueryFactory();
	}
	
	@After
	public void tearDown() {
	}
	
	@Test
	public void testGenders() {
		Query q=queryFactory.create(
				"\r\n" + 
				"select (count(distinct ?gender) as ?cnt) {\r\n" + 
				"   graph graph:baseKB {\r\n" + 
				"      ?person basekb:people.person.gender ?gender\r\n" + 
				"    }\r\n" + 
				"}");
		
		QueryExecution qe=sparql.create(q);
		ResultSet results=qe.execSelect();
		assertTrue(results.hasNext());
		
		int count=results.next().get("cnt").asLiteral().getInt();
		
		assertEquals(2,count);
	}
	
	@Test
	public void testAirports() {
		Query q=queryFactory.create( 
				"select ?code ?name ?item {\r\n" + 
				"   graph graph:baseKB {\r\n" + 
				"      ?item a basekb:aviation.airport .\r\n" + 
				"      ?item rdfs:label ?name .\r\n" + 
				"      ?item public:gravity ?gravity .\r\n" + 
				"      ?item basekb:authority.iata ?code . \r\n" + 
				"      filter(lang(?name)='en')\r\n" + 
				"    }\r\n" + 
				"} order by desc(?gravity) limit 25");
		
		QueryExecution qe=sparql.create(q);
		ResultSet results=qe.execSelect();
		assertTrue(results.hasNext());
		
		String field="code";
		Set<String> codes=Sets.newHashSet();
		while(results.hasNext()) {
			QuerySolution row=results.next();
			String value=row.get(field).toString();
			codes.add(value);
		}
		
		assertEquals(25,codes.size());
		assertTrue(codes.contains("LAX"));
		assertTrue(codes.contains("JFK"));
		assertTrue(codes.contains("LHR"));
	}
	
	@Test
	
	public void testMusicbrainzNamespace() {
		Query q=queryFactory.create(
				"ask {\r\n" + 
				"   graph graph:baseKB {\r\n" + 
				"      basekb:authority.musicbrainz a basekb:type.namespace" +
				"    }\r\n" + 
				"}");
		
		assertTrue(sparql.create(q).execAsk());
	}
	
//	@Test
	
//	public void testMusicbrainzNotTopic() {
//		Query q=queryFactory.create(
//				"prefix basekb: <http://rdf.basekb.com/ns/>\r\n" + 
//				"prefix public: <http://rdf.basekb.com/public/>\r\n" + 
//				"prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n" + 
//				"\r\n" + 
//				"ask {\r\n" + 
//				"   graph public:baseKB {\r\n" + 
//				"      basekb:authority.musicbrainz a basekb:common.topic" +
//				"    }\r\n" + 
//				"}");
//		System.out.println(q.toString());
//		
//		assertTrue(!sparql.create(q).execAsk());
//	}
	
	@Test
	public void testWikipediaEnNamespace() {
		Query q=queryFactory.create(
				"ask {\r\n" + 
				"   graph graph:baseKB {\r\n" + 
				"      basekb:wikipedia.en a basekb:type.namespace" +
				"    }\r\n" + 
				"}");
		
		assertTrue(sparql.create(q).execAsk());
	}
	
	@Test
	public void testAuthorityNamespace() {
		Query q=queryFactory.create(
				"ask {\r\n" + 
				"   graph graph:baseKB {\r\n" + 
				"      basekb:authority a basekb:type.namespace" +
				"    }\r\n" + 
				"}");
		assertTrue(sparql.create(q).execAsk());
	}
	
	@Test
	public void testQuery_1_1_1() {
		Query q=queryFactory.create(
				"select ?name {" + 
				"   graph graph:baseKB {" + 
				"      ?director rdfs:label 'Sofia Coppola'@en ." +
				"      ?director basekb:film.director.film ?film ." +
				"      ?film rdfs:label ?name ." +
				"      filter(lang(?name)='en')"+
				"    }" + 
				"}");
		ResultSet results=sparql.create(q).execSelect();
	}
	
	@Test
	public void testQuery_UntypedTopics() {
		Query q=queryFactory.create(
				"select (count(*) as ?cnt) {" + 
				"   graph graph:baseKB {" + 
				"      ?item a basekb:common.topic ." + 
				"      MINUS {" + 
				"         ?item a ?otherType ." + 
				"         FILTER(?otherType!=basekb:common.topic) ." + 
				"      }" + 
				"    }" + 
				"}"
		);
		ResultSet results=sparql.create(q).execSelect();
		assertTrue(results.hasNext());
		
		int count=results.next().get("cnt").asLiteral().getInt();
		
		assertTrue(count<1200000);
		assertTrue(count>900000);
		
	}
	
	@Test
	public void testJuly4Raw() {
		Query q=queryFactory.create(
				"select ?date { " + 
				"   graph graph:baseKB { " + 
				"      basekb:m.09c7w0 basekb:m.035qyst ?date ." + 
				"   }" + 
				"}"
		);
		
		ResultSet results=sparql.create(q).execSelect();
		july4Check(results);
	}
	
	@Test
	public void testJuly4Grounded() {
		Query q=queryFactory.create(
				"select ?date { " + 
				"   graph graph:baseKB { " + 
				"      basekb:en.united_states basekb:location.dated_location.date_founded ?date ." + 
				"   }" + 
				"}"
		);
		
		ResultSet results=sparql.create(q).execSelect();
		july4Check(results);
	}

	private void july4Check(ResultSet results) {
		assertTrue(results.hasNext());
		
		Literal scalar = results.next().get("date").asLiteral();
		RDFDatatype rdt=scalar.getDatatype();
		assertEquals(XSDDatatype.XSDdate,rdt);
		
		XSDDateTime dt=(XSDDateTime) scalar.getValue();
		assertEquals(1776,dt.getYears());
		assertEquals(7,dt.getMonths());
		assertEquals(4,dt.getDays());
	}
	
	@Test 
	public void testSchemaQuery() {
		Query q=queryFactory.create(
				"select ?label ?property ?range {" + 
				"   graph graph:baseKB {" + 
				"      ?property basekb:type.property.schema basekb:people.person ." + 
				"      OPTIONAL{" + 
				"         ?property basekb:type.property.expected_type ?range ." + 
				"      }" + 
				"      OPTIONAL{" + 
				"         ?property rdfs:label ?label ." + 
				"      }" + 
				"   }" + 
				"}"
		);
		
		ResultSet results=sparql.create(q).execSelect();
		assertTrue(results.hasNext());
	}
	
	@Test
	public void testWikipediaLookup() {
		Query q=queryFactory.create(
				"select ?topic {" +
				"	graph graph:baseKB {" +
				"      ?topic basekb:wikipedia.en 'Urusei_Yatsura'" +
				"   }" +
				"}"
		);

		ResultSet results=sparql.create(q).execSelect();
		assertTrue(results.hasNext());
		
		String topic = results.next().get("topic").asNode().getURI();
		assertEquals("http://rdf.basekb.com/ns/m.014kzr",topic);			
	}

	@Test
	public void testPersonInPeopleDomain() {
		Query q=queryFactory.create(
				"select ?domain ?label {" + 
				"   basekb:people.person basekb:type.type.domain ?domain ." + 
				"   ?domain rdfs:label ?label ." + 
				"   filter(lang(?label)='en')" + 
				"}" 
		);
		
		ResultSet results=sparql.create(q).execSelect();
		assertTrue(results.hasNext());
		String label = results.next().get("label").asLiteral().getLexicalForm();
		assertEquals("People",label);
	}
}



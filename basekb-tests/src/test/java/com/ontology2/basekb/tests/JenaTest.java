package com.ontology2.basekb.tests;

import static org.junit.Assert.*;

import java.util.Set;

import junit.framework.TestCase;

import org.junit.*;

import com.google.common.collect.Sets;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.ontology2.basekb.jena.AnyQueryExecutionFactory;
import com.ontology2.basekb.jena.AnyQueryFactory;
import com.ontology2.basekb.jena.DefaultSpringConfiguration;
import com.ontology2.basekb.jena.RawConfiguration;
import com.ontology2.basekb.jena.SparqlProtocol;
import com.ontology2.basekb.jena.VanillaQueryFactory;

public class JenaTest extends TestCase {
	private AnyQueryExecutionFactory sparql;
	private AnyQueryFactory queryFactory;
	
	@Before
	public void setUp() {
		RawConfiguration jConfig=DefaultSpringConfiguration.getInstance().getRawConfiguration();
		sparql=jConfig.getSparql();
		queryFactory=new VanillaQueryFactory();
	}
	
	@After
	public void tearDown() {
	}
	
	@Test
	public void testNotEmpty() {
		Query q=queryFactory.create(
				"ask { graph ?g { ?s ?p ?o .} } "
		);
		
		QueryExecution qe=sparql.create(q);
		boolean result=qe.execAsk();
		assertTrue(result);
	}
	
	@Test
	public void testLoaded() {
		Query q=queryFactory.create(
				"prefix public: <http://rdf.basekb.com/public/>" +
				"" +
				"ask { graph public:baseKB { ?s ?p ?o .} } "
		);
		
		QueryExecution qe=sparql.create(q);
		boolean result=qe.execAsk();
		assertTrue(result);
	}

	@Test
	public void testLoadedSelect() {
		Query q=queryFactory.create(
				"prefix public: <http://rdf.basekb.com/public/>" +
				"" +
				"select ?g ?s ?p ?o { graph ?g { ?s ?p ?o .} } limit 1"
		);
		
		QueryExecution qe=sparql.create(q);
		ResultSet results=qe.execSelect();
		assertTrue(results.hasNext());
	}
	
	@Test
	public void testNotLoaded() {
		Query q=queryFactory.create(
				"prefix public: <http://rdf.basekb.com/public/>" +
				"" +
				"select ?s ?p ?o { graph public:bogusGraphName { ?s ?p ?o .} } limit 1"
		);
		
		QueryExecution qe=sparql.create(q);
		ResultSet results=qe.execSelect();
		assertFalse(results.hasNext());
	}
	
	@Test
	public void testAirports() {
		Query q=queryFactory.create(
				"prefix basekb: <http://rdf.basekb.com/ns/>\r\n" + 
				"prefix public: <http://rdf.basekb.com/public/>\r\n" + 
				"prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n" + 
				"\r\n" + 
				"select ?code ?name ?item {\r\n" + 
				"   graph public:baseKB {\r\n" + 
				"      ?type public:knownAs basekb:aviation.airport .\r\n" + 
				"      ?iataP public:knownAs basekb:authority.iata .      \r\n" + 
				"      ?item a ?type .\r\n" + 
				"      ?item rdfs:label ?name .\r\n" + 
				"      ?item public:gravity ?gravity .\r\n" + 
				"      ?item ?iataP ?code . \r\n" + 
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
	
	//
	// test for massive breakage
	//
	
	@Test
	public void countTopics() {
		Query q=queryFactory.create(
				"prefix basekb: <http://rdf.basekb.com/ns/>\r\n" + 
				"prefix public: <http://rdf.basekb.com/public/>\r\n" + 
				"prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n" + 
				"\r\n" + 
				"select (count(*) as ?cnt) {\r\n" + 
				"   graph public:baseKB {\r\n" + 
				"      ?type public:knownAs basekb:common.topic .\r\n" + 
				"      ?item a ?type .\r\n" + 
				"    }\r\n" + 
				"}");
		
		QueryExecution qe=sparql.create(q);
		ResultSet results=qe.execSelect();
		assertTrue(results.hasNext());
		QuerySolution row=results.next();
		int value=row.get("cnt").asLiteral().getInt();
		assertTrue(value>3900000);
	}
	
	@Test
	public void lookupRoot() {
		Query q=queryFactory.create(
				"prefix basekb: <http://rdf.basekb.com/ns/>\r\n" + 
				"prefix public: <http://rdf.basekb.com/public/>\r\n" + 
				"prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n" + 
				"\r\n" + 
				"select (count(*) as ?cnt) {\r\n" + 
				"   graph public:baseKB {\r\n" + 
				"      ?type public:knownAs basekb:common.topic .\r\n" + 
				"      ?item a ?type .\r\n" + 
				"    }\r\n" + 
				"}");
		
		QueryExecution qe=sparql.create(q);
		ResultSet results=qe.execSelect();
		assertTrue(results.hasNext());
		QuerySolution row=results.next();
		int value=row.get("cnt").asLiteral().getInt();
		assertTrue(value>3900000);
	}

}

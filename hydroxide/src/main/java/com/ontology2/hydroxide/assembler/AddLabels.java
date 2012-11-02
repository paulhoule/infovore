package com.ontology2.hydroxide.assembler;

import java.util.Enumeration;
import java.util.Map;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

import jdbm.helper.Tuple;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.ontology2.hydroxide.turtleZero.TurtleZero;
import com.ontology2.rdf.JenaUtil;

public class AddLabels implements AssemblerStep {

	final Query keyName=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"SELECT ('en' AS ?lang) (SUBSTR(str(?o),strlen('http://rdf.basekb.com/ns/')) as ?name) {" +
			"    GRAPH internal:knownAsGraph {" +
			"        ?s public:knownAs ?o . " +
			"    }" +
			"} ORDER BY strlen(str(?o)) LIMIT 1" +
			""
	);
	
	final Query aliasNames=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
			"" +
			"SELECT (lang(?n) AS ?lang) (str(?n) as ?name) {" +
			"    GRAPH internal:turtle3Graph {" +
			"        ?s basekb:common.topic.alias ?n. " +
			"    }" +

			"}"			
	);
	
	final Query orthodoxNames=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"SELECT (lang(?n) AS ?lang) (str(?n) as ?name) {" +
			"    GRAPH internal:turtle3Graph {" +
			"        ?s basekb:type.object.name ?n. " +
			"    }" +

			"}"			
	);
	
	final Query wikiNames=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"SELECT ?lang (REPLACE(?key,'_',' ') AS ?name) {" +
			"    GRAPH internal:turtle3Graph {" +
			"        ?s ?keyPredicate ?key ." +
			"    }" +
			"    GRAPH internal:langMapGraph {" +
			"        ?keyPredicate internal:forLanguage ?lang ." +
			"    }" +
			"}"
	);
	
	protected Model langMap;
	
	public AddLabels() throws Exception {
		langMap=ModelFactory.createDefaultModel();
		TurtleZero t0=new TurtleZero();
		
		String wikiNamespace=t0.lookup("/wikipedia");
		Enumeration<Tuple> list=t0.lookupNamespace(wikiNamespace);
		while(list.hasMoreElements()) {
			Tuple t=list.nextElement();
			String name=Iterables.get(Splitter.on('/').split((String) t.getKey()),3);
			if (name.endsWith("_title")) {
				String lang=name.substring(0,2);
				String ns=(String) t.getValue();
				langMap.add((Resource) langMap.asRDFNode(toBk(ns)),langMap.createProperty("http://rdf.basekb.com/internal/","forLanguage"),lang);
			}
		}
		
		System.out.println(langMap);
		
	}
	@Override
	public void applyRule(Resource subject, Dataset input, Model output)
			throws Exception {
		input.addNamedModel("http://rdf.basekb.com/internal/langMapGraph", langMap);
		Map<RDFNode,RDFNode> names=Maps.newHashMap();
		names.put(output.createLiteral("en"),output.createLiteral(toFb(subject)));
		names.putAll(JenaUtil.fetchMap(input,keyName,null));
		names.putAll(JenaUtil.fetchMap(input, aliasNames, null));
		names.putAll(JenaUtil.fetchMap(input, orthodoxNames, null));
		names.putAll(JenaUtil.fetchMapSingle(input, wikiNames, null));
		for(Map.Entry<RDFNode,RDFNode> entry:names.entrySet()) {
			String lang=entry.getKey().toString();
			String text=entry.getValue().toString();
			output.add(subject,RDFS.label,output.createLiteral(text,lang));
		}
	}

}

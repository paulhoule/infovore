package com.ontology2.hydroxide;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.ontology2.basekb.BaseIRI;
import com.ontology2.millipede.sink.Sink;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

public class QuadToTriple implements Sink<FreebaseQuad> {
	final Sink<Triple> innerSink;
	final Map<String,Node> propertyMap;
	
	public QuadToTriple(Sink<Triple> innerSink) {
		this.innerSink=innerSink;
		propertyMap=new HashMap<String,Node>() {{
			put("/type/object/type",RDF.Nodes.type);
		}};
	}

	@Override
	public void accept(FreebaseQuad item) throws Exception {
		if ("/type/object/key"==item.getProperty()) {
			String object;
			
			if (item.getDestination().equals("")) {
				object=BaseIRI.freebaseBase + item.getValue().replace("/",".");
			} else {
				object= BaseIRI.freebaseBase
					+(item.getDestination().substring(1)+"."+item.getValue()).replace("/", ".");
			}
			
			Node bNode=Node.createAnon();
			innerSink.accept(new Triple(
					fbToUri(item.getSubject()),
					fromFb("/type/object/key"),
					bNode
			));
			
			innerSink.accept(new Triple(
					bNode,
					fromFb("/type/key/namespace"),
					fromFb(item.getDestination())
			));
			
			innerSink.accept(new Triple(
					bNode,
					fromFb("/type/value/value"),
					fromFb(item.getValue())
			));
			
			
			innerSink.accept(new Triple(
					fbToUri(item.getSubject()),
					BKBInternal.knownAs,
					Node.createURI(object)
				));
				return;			
		}
		
		if("".equals(item.getDestination())) {
			innerSink.accept(new Triple(
				fbToUri(item.getSubject()),
				mapProperty(item.getProperty()),
				Node.createLiteral(item.getValue())
			));
			return;
		}
		
		if("".equals(item.getValue())) {
			innerSink.accept(new Triple(
				fbToUri(item.getSubject()),
				mapProperty(item.getProperty()),
				fbToUri(item.getDestination())
			));
			return;
		}
		
		if (item.getDestination().startsWith("/lang/")) {
			String lang=item.getDestination().substring(6);
			innerSink.accept(new Triple(
					fbToUri(item.getSubject()),
					mapProperty(item.getProperty()),
					Node.createLiteral(item.getValue(),lang,false)					
			));
			return;
		}
	}

	protected Node mapProperty(String property) {
		if(propertyMap.containsKey(property)) {
			return propertyMap.get(property);
		}
		
		return fromFb(property);
	}
	@Override
	public void close() throws Exception {
		innerSink.close();
	}

}

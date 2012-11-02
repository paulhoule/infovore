package com.ontology2.hydroxide.turtleZero;

import com.google.common.base.Function;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.ontology2.basekb.BaseIRI;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;

public class RDFGrounder  {
	private final TurtleZero turtleZero;
	
	public RDFGrounder(TurtleZero turtleZero) {
		this.turtleZero=turtleZero;
	}

	public Node ground(Node node) {
		if(!node.isURI())
			return node;
		
		Node_URI uri = (Node_URI) node;
		String namespace=uri.getNameSpace();
		if (namespace.equals(BaseIRI.bkNs) 
				|| namespace.equals(BaseIRI.freebaseBase)) {
			String localname=uri.getLocalName();
			try {
				String mid=turtleZero.lookup("/"+localname.replace('.', '/'));
				return toBk(mid);
			} catch (Exception e) {}
		}
	
		return node;
	}
	
	public Triple ground(Triple triple) {
		return new Triple(
				ground(triple.getSubject()),
				ground(triple.getPredicate()),
				ground(triple.getObject()));
	}

	public RDFNode ground(RDFNode node) {
		Model m=node.getModel();
		return m.asRDFNode(ground(node.asNode()));
	}
	
	public Statement ground(Statement s) {
		Model m=s.getModel();
		return m.asStatement(ground(s.asTriple()));
	}
	
	public Model ground(Model m) {
		Model out=ModelFactory.createDefaultModel();
		StmtIterator i=m.listStatements();
		while(i.hasNext()) {
			out.add(ground(i.next()));
		}
		return out;
	}
}

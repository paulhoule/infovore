package com.ontology2.sparqlGrounder;

import com.hp.hpl.jena.graph.Node;
import com.ontology2.basekb.BaseIRI;

import static com.ontology2.basekb.StatelessIdFunctions.*;
import static com.ontology2.basekb.jena.StatelessJenaFunctions.*;
import com.ontology2.hydroxide.turtleZero.TurtleZero;

public class TurtleZeroGrounder implements IRIGrounder {

	private TurtleZero t0;

	public TurtleZeroGrounder() throws Exception {
			this(new TurtleZero());
	}
	
	public TurtleZeroGrounder(TurtleZero t0) {
		this.t0=t0;
	}
	
	@Override
	public Node ground(String iri) throws Exception {
		if(!iri.startsWith(BaseIRI.bkNs)) {
			return Node.createURI(iri);
		}
		
		String fb=toFb(Node.createURI(iri));
		String result=t0.lookup(fb);
		if(TurtleZero.FAILED==result) {
			return Node.createURI(iri);
		}
		
		return toBk(result);
	}

	public static TurtleZeroGrounder create() {
		try {
			return new TurtleZeroGrounder();
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
}

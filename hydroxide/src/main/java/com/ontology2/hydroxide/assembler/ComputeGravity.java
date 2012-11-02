package com.ontology2.hydroxide.assembler;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.ontology2.hydroxide.BKBPublic;
import com.ontology2.rdf.JenaUtil;

public class ComputeGravity implements AssemblerStep {

	final Query countQ=Grounded.query(
			"PREFIX basekb: <http://rdf.basekb.com/ns/>" +
			"PREFIX public: <http://rdf.basekb.com/public/>" +
			"PREFIX internal: <http://rdf.basekb.com/internal/>" +
			"" +
			"SELECT (COUNT(*) AS ?cnt) { " +
			"    GRAPH internal:turtle3Graph {" +
			"        { ?s ?p ?o . }  " +
			"    } " +
			"}");
	@Override
	
	public void applyRule(Resource subject,Dataset input, Model output) throws Exception {
		RDFNode countNode=JenaUtil.fetchScalar(input, countQ);
		double count=countNode.asLiteral().getDouble();
		double gravity=Math.log(count)-5;
		output.add(output.createLiteralStatement(subject,output.createProperty(BKBPublic.gravity.toString()),gravity));
	}

}
